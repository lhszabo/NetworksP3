import socket, sys
import threading
import json
import enum
import pickle

# from https://docs.skyswitch.com/en/articles/579-what-does-udp-timeout-mean#:~:text=UDP%20Timeout%20refers%20to%20the,at%2030%20or%2060%20seconds.
TIMEOUT = 30
BUFSIZE = 1400
filename = None
file_descriptor = None
fetch_file = False
send_ack = False
keep_transmitting = False
fin = False

class Network_node():
  def __init__(self, host, port, peers, content, name):
    self.host = host
    self.port = port
    self.peers = peers # dictionary of network_peer's where key is port as string
    self.content = content # list of content infos
    self.owner = (None, None)
    self.name = name # for debugging purposes only 
    
class Network_peer():
  def __init__(self, host, content):
    self.host = host
    self.content = content
    self.ack = 1
    
class Handshake_states(enum.Enum):
  closed = 1
  listen = 2
  syn_rec = 3
  syn_sent = 4
  est = 5
  fin_wait_1 = 6
  closing = 7
  fin_wait_2 = 8
  time_wait = 9
  close_wait = 10
  last_ack = 11

# ONLY IMPLEMENTED FOR STOP AND WAIT SO FAR, transitions only, handling outputs manually
class State_machine():
  def __init__(self):
    self.state = Handshake_states.closed
    # ways for owner to store info
    self.requestor_info = (None, None) 
    self.file_req = None 
  
  def next(self, input): # (cur state, input) -> next state
    # advancing to next state and return necessary output to other node
    if (self.state == Handshake_states.closed and input == "LISTEN"):
      self.state = Handshake_states.listen
    elif (self.state == Handshake_states.closed and input == "CONNECT"):
      self.state = Handshake_states.syn_sent
    
    elif (self.state == Handshake_states.listen and input == "SYN"):
      self.state = Handshake_states.syn_rec
    
    elif (self.state == Handshake_states.syn_rec and input == "ACK 0"):
      # subsequent ACK's will count packets
      self.state = Handshake_states.est
    
    elif (self.state == Handshake_states.syn_sent and input == "SYN+ACK"):
      self.state = Handshake_states.est
    
  def reset(self):
    self.state = Handshake_states.closed
    self.requestor_info = (None, None) 
    self.file_req = None 
    
requestor_machine = State_machine()
owner_machine = State_machine()
owner_machine.next("LISTEN")

def parse_conf(conf_file):
  name = conf_file.split(".")[0] # for debugging purposes only
  f = open(conf_file, 'r')
  json_dict = json.load(f)
  host = json_dict['hostname']
  port = json_dict['port']
  content = json_dict['content_info']
  peer_dict = dict()
  for peer in json_dict['peer_info']:
    port_key = str(peer['port'])
    peer_host = peer['hostname']
    peer_content = peer['content_info']
    new_peer = Network_peer(peer_host, peer_content)
    peer_dict[port_key] = new_peer
  node = Network_node(host, port, peer_dict, content, name)
  return node

def is_file(cmd):
  return "." in cmd
 
def get_file_owner(node, filename):
  # find node with file in config
  file_peer_port = None
  owner_host = None
  for peer_port in node.peers:
    peer = node.peers[peer_port]
    peer_content = peer.content
    if (filename in peer_content):
      file_peer_port = peer_port
      owner_host = peer.host
  node.owner = (owner_host, int(file_peer_port))

def parse_syn(msg):
  msg_fields = msg.split(" ")
  filename = msg_fields[1]
  sender_host = msg_fields[2]
  sender_port = int(msg_fields[3])
  return filename, sender_host, sender_port

def tx_thread(name, socket, node):
  while True:
    global fetch_file
    global send_ack
    global fin
    global send_ack
    global keep_transmitting
    global filename
    global file_descriptor
    
    if (fetch_file): # send SYN
      get_file_owner(node, filename) # setting node.owner variable
      requestor_machine.next("CONNECT") 
      msg = "SYN"
      msg += " " + filename + " " + node.host + " " + str(node.port)
      # print(msg)
      msg = pickle.dumps(msg)
      socket.sendto(msg, node.owner)
      fetch_file = False
      assert(requestor_machine.state != Handshake_states.closed)
    
    elif (owner_machine.state == Handshake_states.syn_rec):
      msg = "SYN+ACK"
      # print(msg)
      msg = pickle.dumps(msg)
      socket.sendto(msg, owner_machine.requestor_info) 
      assert(requestor_machine.state != Handshake_states.closed)
    
    elif (owner_machine.state == Handshake_states.est and requestor_machine.state == Handshake_states.est
          and not send_ack):
      print("HELLO")
      file_descriptor = open(owner_machine.file_req, "rb")
      chunk = file_descriptor.read(BUFSIZE)
      data_dict = {"header": "PUT"}
      # print('other chunks')
      if (chunk):
        # msg = "PUT " + str(chunk)
        data_dict["data"] = chunk
        # print('other chunks', msg)
        msg = pickle.dumps(data_dict)
        socket.sendto(msg, owner_machine.requestor_info)
      else:
        fin = True
        keep_transmitting = False
        msg = "FIN"
        msg = pickle.dumps(msg)
        socket.sendto(msg, owner_machine.requestor_info)
      assert(requestor_machine.state != Handshake_states.closed)
        
    elif (requestor_machine.state == Handshake_states.est):
      msg = "ACK 0"
      # print(msg)
      msg = pickle.dumps(msg)
      socket.sendto(msg, node.owner) 
      assert(requestor_machine.state != Handshake_states.closed)
    
    # elif (owner_machine.state == Handshake_states.est):
    #   # start transmitting data 
    #   f = open(owner_machine.file_req, "rb")
    #   chunk = f.read(BUFSIZE)
    #   data_dict = {"header": "PUT"}
    #   if (chunk):
    #     data_dict["data"] = chunk
    #     # msg = b"PUT " + chunk
    #     print('chunk 1')
    #     msg = pickle.dumps(data_dict)
    #     socket.sendto(msg, owner_machine.requestor_info
        
    elif (send_ack):
      owner_port = node.owner[1]
      ack_num = node.peers[str(owner_port)].ack
      msg = "ACK " + str(ack_num)
      # print(msg)
      msg = pickle.dumps(msg)
      socket.sendto(msg, node.owner)
      node.peers[str(owner_port)].ack += 1
      send_ack = False
      assert(requestor_machine.state != Handshake_states.closed)

def rx_thread(name, socket, node):
  while True:
    rec_msg, addr = socket.recvfrom(BUFSIZE)
    # print('rec msg', rec_msg)
    pickle_msg = pickle.loads(rec_msg)
    # print(pickle_msg)
    # rec_msg = rec_msg.decode()
    # pickle_msg = pickle.loads(rec_msg)
    
    global filename
    global send_ack
    if ("SYN " in pickle_msg):
      filename, sender_host, sender_port = parse_syn(pickle_msg)
      # saving info for owner
      owner_machine.requestor_info = (sender_host, sender_port)
      owner_machine.file_req = filename
      owner_machine.next("SYN")
      assert(requestor_machine.state != Handshake_states.closed)
    
    elif (pickle_msg == "SYN+ACK"):
      # print(requestor_machine.state, 'rm state')
      requestor_machine.next(pickle_msg)
      assert(requestor_machine.state != Handshake_states.closed)
    
    elif (pickle_msg == "ACK 0"):
      # print('rec ack 0')
      # print(owner_machine.state, 'om state')
      # print(requestor_machine.state, 'rm state')
      owner_machine.next(pickle_msg)
      assert(requestor_machine.state != Handshake_states.closed)
    
    elif (pickle_msg["header"] == "PUT"):
      print('received chunk')
      # requestor knows filename!
      chunk = pickle_msg["data"]
      with open(filename, "wb") as binary_file: # see appending issue
        # file_bytes = rec_msg[4:] # excluding "PUT " from string
        # print('rec file bytes', file_bytes)
        binary_file.write(chunk)
        # global send_ack
        print('setting send_ack')
        send_ack = True
      assert(requestor_machine.state != Handshake_states.closed)
        
    elif (pickle_msg != "ACK 0" and "ACK " in pickle_msg):
      # global send_ack
      send_ack = False
      assert(requestor_machine.state != Handshake_states.closed)
    
    elif (pickle_msg == "FIN"):
      # could put some check on byte length
      assert(requestor_machine.state != Handshake_states.closed)
      requestor_machine.reset()
      owner_machine.reset()
    
if __name__ == '__main__':
  conf_file = None
  if len(sys.argv) != 2:
    sys.exit(-1)
  else:
    conf_file = sys.argv[1] # no -c flag
  
  node = parse_conf(conf_file)
  
  sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  sender.bind((node.host, node.port))
  
  receiver = sender
  
  tx = threading.Thread(target=tx_thread, args=(1, sender, node), daemon=True)
  tx.start()
  
  rx = threading.Thread(target=rx_thread, args=(2, receiver, node), daemon=True)
  rx.start()
  
  while True:
    cmd = input()
    cmd = cmd.rstrip()
    if (is_file(cmd)):
      filename = cmd
      fetch_file = True