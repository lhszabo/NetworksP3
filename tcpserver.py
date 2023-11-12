import socket, sys
import threading
import json
import enum

# from https://docs.skyswitch.com/en/articles/579-what-does-udp-timeout-mean#:~:text=UDP%20Timeout%20refers%20to%20the,at%2030%20or%2060%20seconds.
TIMEOUT = 30
filename = None
fetch_file = False
BUFSIZE = 1400
send_ack = False
keep_transmitting = False
fin = False

class Network_node():
  # host = -1
  # port = -1
  # peers = -1
  # content = -1
  # owner = -1
  def __init__(self, host, port, peers, content):
    self.host = host
    self.port = port
    self.peers = peers # dictionary of network_peer's where key is port
    self.content = content # list of content infos
    self.owner = (None, None)
    
  def set_node_owner(self, owner):
    self.owner = owner
  
  def get_node_owner(self):
    return self.owner
    
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
  chk_rec = 12
  ack_rec = 13

# ONLY IMPLEMENTED FOR STOP AND WAIT SO FAR -> may not need dotted lines transitions
class State_machine():
  def __init__(self):
    self.state = Handshake_states.closed
  
  def next(self, input):
    # advancing to next state and return necessary output to other node
    if (self.state == Handshake_states.closed and input == "LISTEN"):
      self.state = Handshake_states.listen
      return None
    if (self.state == Handshake_states.closed and input == "CONNECT"):
      self.state = Handshake_states.syn_sent
      return "SYN"
    
    elif (self.state == Handshake_states.listen and input == "SYN"):
      self.state = Handshake_states.syn_rec
      return "SYN+ACK"
    elif (self.state == Handshake_states.listen and input == "CLOSE"):
      self.state = Handshake_states.closed
      return None
    elif (self.state == Handshake_states.listen and input == "SEND"):
      self.state = Handshake_states.syn_sent
      return "SYN"
    
    elif (self.state == Handshake_states.syn_rec and input == "RST"):
      self.state = Handshake_states.listen
      return None
    elif (self.state == Handshake_states.syn_rec and input == "ACK 0"):
      self.state = Handshake_states.est
      return None
    
    elif (self.state == Handshake_states.syn_sent and input == "CLOSE"):
      self.state = Handshake_states.closed
      return None
    elif (self.state == Handshake_states.syn_sent and input == "SYN"):
      self.state = Handshake_states.syn_rec
      return "SYN+ACK"
    elif (self.state == Handshake_states.syn_sent and input == "SYN+ACK"):
      self.state = Handshake_states.est
      return "ACK"
  
  def reset(self):
    self.state = Handshake_states.closed
    
requestor_machine = State_machine()
owner_machine = State_machine()
owner_machine.next("LISTEN")

def parse_conf(conf_file):
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
  node = Network_node(host, port, peer_dict, content)
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
  return owner_host, int(file_peer_port)

def parse_syn(msg):
  msg_fields = msg.split(" ")
  filename = msg_fields[1]
  sender_host = msg_fields[2]
  sender_port = int(msg_fields[3])
  return filename, sender_host, sender_port

def tx_thread(name, socket, node):
  while True:
    f = None
    
    global fetch_file
    global send_ack
    global fin
    global send_ack
    global keep_transmitting
    
    owner_host, file_peer_port = None, None
    if (filename != None):
      # print('in here')
      owner_host, file_peer_port= get_file_owner(node, filename)
      # print(owner_host, file_peer_port)
    
    if (fetch_file): # send SYN
      # owner_host, file_peer_port = get_file_owner(node, filename)
      msg = requestor_machine.next("CONNECT") # requestor machine in syn_sent
      msg += " " + filename + " " + node.host + " " + str(node.port)
      socket.sendto(msg.encode(), (owner_host, file_peer_port))
      fetch_file = False # turn off
    
    elif (owner_machine.state == Handshake_states.syn_rec):
      msg = "SYN+ACK"
      # owner_host, file_peer_port = get_file_owner(node, filename)
      print(owner_host, file_peer_port)
      socket.sendto(msg.encode(), (owner_host, file_peer_port)) 
    
    elif (requestor_machine.state == Handshake_states.est):
      msg = "ACK 0"
      # owner_host, file_peer_port = get_file_owner(node, filename)
      socket.sendto(msg.encode(), (owner_host, file_peer_port)) 
    
    elif (owner_machine.state == Handshake_states.est):
      # start transmitting data 
      f = open(filename, "rb")
      chunk = f.read(BUFSIZE)
      if (chunk):
        msg = "PUT " + chunk
        socket.sendto(msg.encode(), (owner_host, file_peer_port))
    
    elif (send_ack):
      ack_num = node.peers[str(file_peer_port)].ack
      msg = "ACK " + str(ack_num)
      socket.sendto(msg.encode(), (owner_host, file_peer_port))
      node.peers[str(file_peer_port)].ack += 1
      send_ack = False
    
    elif (keep_transmitting):
      chunk = f.read(BUFSIZE)
      if (chunk):
        msg = "PUT " + chunk
        socket.sendto(msg.encode(), (owner_host, file_peer_port))
      else:
        fin = True
        keep_transmitting = False
        msg = "FIN"
        socket.sendto(msg.encode(), (owner_host, file_peer_port))

def rx_thread(name, socket, node):
  while True:
    rec_msg, addr = socket.recvfrom(BUFSIZE)
    rec_msg = rec_msg.decode()
    
    if ("SYN " in rec_msg):
      owner_machine.next("SYN")
    
    elif (rec_msg == "SYN+ACK"):
      requestor_machine.next(rec_msg)
    
    elif (rec_msg == "ACK 0"):
      owner_machine.next(rec_msg)
    
    elif ("PUT" in rec_msg):
      with open(filename, "ab") as binary_file: # see appending issue
        file_bytes = rec_msg.split(" ")[1]
        binary_file.write(file_bytes)
        global send_ack
        send_ack = True
        
    elif ("ACK " in rec_msg and rec_msg[-1] != 0):
      global keep_transmitting
      keep_transmitting = True
    
    elif (rec_msg == "FIN"):
      # could put some check on byte length
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