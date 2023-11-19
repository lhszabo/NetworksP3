import socket, sys
import threading
import json
import enum
import os
import time
from pathlib import Path

# from https://docs.skyswitch.com/en/articles/579-what-does-udp-timeout-mean#:~:text=UDP%20Timeout%20refers%20to%20the,at%2030%20or%2060%20seconds.
TIMEOUT = 30
BUFSIZE_FROM_FILE = 1400
BUFSIZE_TO_REC = 1500 # including PUT header
W_SIZE = 30
filename = None
fetch_file = False
sent_chunk = None
rec_chunk = None
ack_lock = threading.Lock()

class Network_node():
  def __init__(self, host, port, peers, content, name):
    # eventually each node-node interaction stored in a dictionary
    self.host = host
    self.port = port
    self.peers = peers # dictionary of network_peer's where key is port as string
    self.content = content # list of content infos
    self.owner = (None, None)
    self.requestor = (None, None)
    self.name = name # for debugging purposes only 
    self.state_machine = State_machine()
    self.filename = None
    self.pkt_length = 0
    self.length_received = 0
    self.rec_acks = dict()
    self.num_msgs_sent = 0
    # self.num_msgs_rec = 0
    # self.num_acks_sent = 0
    self.num_acks_rec = 0
    
    
    # node.last_file_ptr = 0
    
class Network_peer():
  def __init__(self, host, content):
    self.host = host
    self.content = content
    self.ack = 1
    
class Handshake_states(enum.Enum):
  closed = 1
  syn_rec = 2
  syn_ack_rec = 3
  send = 4
  req = 5
  done = 6
  wait_req = 7
  wait_send = 8

# ONLY IMPLEMENTED FOR STOP AND WAIT SO FAR, transitions only, handling outputs manually
class State_machine():
  def __init__(self):
    self.state = Handshake_states.closed
  
  def next(self, input): # (cur state, input) -> next state  
    if (self.state == Handshake_states.closed and input == "SYN"):
      self.state = Handshake_states.syn_rec
    
    elif (self.state == Handshake_states.syn_rec and input == "SYN+ACK"):
      self.state = Handshake_states.syn_ack_rec
    
    elif (self.state == Handshake_states.syn_ack_rec and input == "rec ACK 0"):
      self.state = Handshake_states.send
    
    elif (self.state == Handshake_states.syn_ack_rec and input == "send ACK 0"):
      self.state = Handshake_states.req
    
    elif (self.state == Handshake_states.send and input == "SENT PKT"):
      self.state = Handshake_states.wait_send
    elif (self.state == Handshake_states.wait_send and input == "REC PKT ACK"):
      self.state = Handshake_states.send
      
    elif (self.state == Handshake_states.req and input == "REC PKT"):
      self.state = Handshake_states.wait_req
    elif (self.state == Handshake_states.wait_req and input == "SENT PKT ACK"):
      self.state = Handshake_states.req
    
  def reset(self):
    self.state = Handshake_states.done

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
  node.filename = filename

def parse_syn(msg):
  msg_fields = msg.split(" ")
  filename = msg_fields[1]
  sender_host = msg_fields[2]
  sender_port = int(msg_fields[3])
  return filename, sender_host, sender_port

def parse_rec_msg(rec_msg):
  seq_num = None
  chunk = None
  for i in range(len(rec_msg)):
    if (rec_msg[i] == 32): # 32 is the value for space
      seq_num = rec_msg[:i].decode() # gives a str
      chunk = rec_msg[i+1:]
      return int(seq_num), chunk
    

def tx_thread(name, socket, node):
  while True:
    global fetch_file
    global filename
    global sent_chunk
      
    if (fetch_file): # send SYN
      get_file_owner(node, filename) # setting node.owner and node.filename variable
      node.requestor = (node.host, node.port)
      node.state_machine.next("SYN")
      msg = "SYN "
      msg += filename + " " + node.host + " " + str(node.port)
      socket.sendto(msg.encode(), node.owner)
      # print(msg, node.name)
      fetch_file = False
      
    elif (node.state_machine.state == Handshake_states.syn_rec):
      msg = "SYN+ACK "
      if (os.path.exists(node.filename)): # should only run for owner
        file_size = Path(node.filename).stat().st_size # getting size in bytes
        msg += str(file_size)
        node.pkt_length = file_size
        node.state_machine.next("SYN+ACK")
        # print(msg, node.name)
        socket.sendto(msg.encode(), node.requestor) 
    
    elif (node.state_machine.state == Handshake_states.syn_ack_rec):
      if not (os.path.exists(node.filename)):
        msg = "ACK 0"
        node.state_machine.next("send ACK 0")
        # print(msg, node.name)
        # print(node.name, 'state is', node.state_machine.state)
        socket.sendto(msg.encode(), node.owner) 
    
    elif (node.state_machine.state == Handshake_states.send):
      with open(node.filename, "rb") as file_descriptor:
        while (node.num_msgs_sent < W_SIZE):
          
          req_port = node.requestor[1]
          cur_ack = node.peers[str(req_port)].ack
          file_descriptor.seek(BUFSIZE_FROM_FILE*(cur_ack-1)) # whence=1, ref from cur position
          # print('cur ack', cur_ack)
          # print('tell', file_descriptor.tell())
          chunk = file_descriptor.read(BUFSIZE_FROM_FILE)
          
          if (chunk):
            put_bytes = "PUT ".encode()
            sent_chunk = chunk
            node.length_received += len(chunk)
            # print('len rec', node.length_received)
            # print('send', node.name, cur_ack)
            ack_bytes = (str(cur_ack) + " ").encode()
            node.peers[str(req_port)].ack += 1
            msg = put_bytes + ack_bytes + chunk
            socket.sendto(msg, node.requestor)
            node.num_msgs_sent += 1
            # node.state_machine.next("SENT PKT")
          else:
            msg = "FIN"
            node.state_machine.reset()
            socket.sendto(msg.encode(), node.requestor)
            break # not sure if this is necessary
        node.state_machine.next("SENT PKT")  
        # print('state should be wait send', node.state_machine.state, node.name)    
    elif (node.state_machine.state == Handshake_states.req):
      # print('req sending ack', node.name)
      # assert(node.name == "node1")
      owner_port = node.owner[1]
      # ack_num = None
      # with ack_lock:
      # print('accessing this')
      if (owner_port not in node.rec_acks):
        time.sleep(.01)
      ack_num = node.rec_acks[owner_port]
      # print(node.rec_acks)
      msg = "ACK " + str(ack_num)
      # print('req sending ack', msg, node.name)
      socket.sendto(msg.encode(), node.owner)
      # node.state_machine.next("SENT PKT ACK")
      # print('state should be req', node.state_machine.state, node.name, msg)
      # node.peers[str(owner_port)].ack += 1
      # send_ack = False

def rx_thread(name, socket, node):
  while True:
    rec_msg, addr = socket.recvfrom(BUFSIZE_TO_REC)

    global filename
    
    put_bytes = "PUT ".encode()
    # print(rec_msg.decode())
    
    if (put_bytes == rec_msg[0:4] and node.state_machine.state == Handshake_states.req):
      seq_num, chunk = parse_rec_msg(rec_msg[4:])
      # print('rec', node.name, 'sn', seq_num)
      node.length_received += len(chunk)
      # if not (node.length_received == node.pkt_length):
        # assert(node.length_received == BUFSIZE_FROM_FILE*seq_num)
      owner_port = node.owner[1]
      # prev_num = None
      # if (owner_port in node.rec_acks):
        # prev_num = node.rec_acks[owner_port]
      with ack_lock:
        # print('setting this')
        node.rec_acks[owner_port] = seq_num
      # print(node.rec_acks)
      # if (prev_num != None):
        # assert(seq_num == prev_num + 1)
        
      with open(filename, "ab") as binary_file: # see appending issue
        binary_file.write(chunk)
        if (node.length_received == node.pkt_length):
          print('received entire length')
          binary_file.close()
      # node.num_msgs_rec += 1
      # if (node.num_msgs_rec == W_SIZE):
      # node.state_machine.next("REC PKT")
      # print('state should be wait req', node.state_machine.state, node.name)
        # could do wb and seek
        
    elif ("SYN " in rec_msg.decode()):
      pickle_msg = rec_msg.decode()
      filename, sender_host, sender_port = parse_syn(pickle_msg)
      # saving info for owner
      node.requestor = (sender_host, sender_port)
      node.owner = (node.host, node.port)
      node.filename = filename
      # print('syn pkt received by', node.name)
      node.state_machine.next("SYN")
    
    elif ("SYN+ACK" in rec_msg.decode()):
      rec_msg = rec_msg.decode() 
      node.pkt_length = int(rec_msg.split(" ")[-1])
      # print("syn ack msg received by", node.name)
      # print("pkt length to be received is", node.pkt_length)
      node.state_machine.next("SYN+ACK")
    
    elif (rec_msg.decode() == "ACK 0"):
      # print('ack 0 received by', node.name)
      node.state_machine.next("rec ACK 0")
      # print(node.name, 'state is', node.state_machine.state)
        
    elif (rec_msg.decode() != "ACK 0" and "ACK " in rec_msg.decode()):
      # send_ack = False
      node.num_acks_rec += 1
      if (node.num_acks_rec == W_SIZE):
        node.state_machine.next("REC PKT ACK")
        node.num_msgs_sent = 0 
        # print('reset for next window')
        # print('state should be send', node.state_machine.state, node.name)
    
    elif (rec_msg.decode() == "FIN"):
      # if (node.length_received == node.pkt_length):
      # could put some check on byte length
      # print("FIN rec")
      node.state_machine.reset()
      # print(node.state_machine.state, node.name)
      
      # at start -> send how many bytes should be transferred
      # once number of bytes received FIN
    
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
  
  # check that packets are received in order and same number are sent and received
  # contents of each packet is correct
