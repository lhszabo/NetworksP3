import subprocess, time, filecmp, os
import sys, traceback
from kill_command import kill_command

TIMEOUT = 20
FILENAME = 'trailer_400p.ogg'

print()
print("=" * 100)
print("===> Test case 4.1. Concurrently transferring 5 large files without packet loss")

execution_dir_1 = './node1/'
execution_dir_2 = './node2/'
execution_dir_3 = './node3/'
execution_dir_4 = './node4/'
execution_dir_5 = './node5/'
execution_dir_6 = './node6/'

solution_path = '../solution/tcpserver.py'

print("===> Setup Connection")


node_total_num = 6
node_process_list = []
shell_command_line_1 = 'python3 %s node1.json\n' % solution_path
shell_command_line_2 = 'python3 %s node2.json\n' % solution_path
shell_command_line_3 = 'python3 %s node3.json\n' % solution_path
shell_command_line_4 = 'python3 %s node4.json\n' % solution_path
shell_command_line_5 = 'python3 %s node5.json\n' % solution_path
shell_command_line_6 = 'python3 %s node6.json\n' % solution_path

proc = subprocess.Popen(shell_command_line_1, stdin=subprocess.PIPE, shell=True, universal_newlines=True, cwd=execution_dir_1)
node_process_list.append(proc)
proc = subprocess.Popen(shell_command_line_2, stdin=subprocess.PIPE, shell=True, universal_newlines=True, cwd=execution_dir_2)
node_process_list.append(proc)
proc = subprocess.Popen(shell_command_line_3, stdin=subprocess.PIPE, shell=True, universal_newlines=True, cwd=execution_dir_3)
node_process_list.append(proc)
proc = subprocess.Popen(shell_command_line_4, stdin=subprocess.PIPE, shell=True, universal_newlines=True, cwd=execution_dir_4)
node_process_list.append(proc)
proc = subprocess.Popen(shell_command_line_5, stdin=subprocess.PIPE, shell=True, universal_newlines=True, cwd=execution_dir_5)
node_process_list.append(proc)
proc = subprocess.Popen(shell_command_line_6, stdin=subprocess.PIPE, shell=True, universal_newlines=True, cwd=execution_dir_6)
node_process_list.append(proc)
time.sleep(2)

print("===> Request five medium file and TIMEOUT is %d seconds" % TIMEOUT)

for node_num in range(1,node_total_num):
	node_process_list[node_num].stdin.write(FILENAME+'\n')
for node_num in range(1,node_total_num):
	node_process_list[node_num].stdin.flush()
time.sleep(TIMEOUT)

print("===> Kill nodes")
for node_num in range(node_total_num):
	try:
		node_process_list[node_num].kill()
	except EOFError:
		pass

kill_command()

return_bool = False
check_exists_bool = os.path.exists(execution_dir_2+FILENAME) \
					& os.path.exists(execution_dir_3+FILENAME) \
					& os.path.exists(execution_dir_4+FILENAME) \
					& os.path.exists(execution_dir_5+FILENAME) \
					& os.path.exists(execution_dir_6+FILENAME)

if check_exists_bool:
	return_bool = filecmp.cmp(execution_dir_1+FILENAME, execution_dir_2+FILENAME, shallow=False) \
				& filecmp.cmp(execution_dir_1+FILENAME, execution_dir_3+FILENAME, shallow=False) \
				& filecmp.cmp(execution_dir_1+FILENAME, execution_dir_4+FILENAME, shallow=False) \
				& filecmp.cmp(execution_dir_1+FILENAME, execution_dir_5+FILENAME, shallow=False) \
				& filecmp.cmp(execution_dir_1+FILENAME, execution_dir_6+FILENAME, shallow=False)

if return_bool:
	print("===> Test Case [[[Success]]]")
else:
	print("===> Test Case [[[Fail]]]")

if os.path.exists(execution_dir_2+FILENAME):
	print("remove file -", execution_dir_2+FILENAME)
	os.remove(execution_dir_2+FILENAME)

if os.path.exists(execution_dir_3+FILENAME):
	print("remove file -", execution_dir_3+FILENAME)
	os.remove(execution_dir_3+FILENAME)

if os.path.exists(execution_dir_4+FILENAME):
	print("remove file -", execution_dir_4+FILENAME)
	os.remove(execution_dir_4+FILENAME)

if os.path.exists(execution_dir_5+FILENAME):
	print("remove file -", execution_dir_5+FILENAME)
	os.remove(execution_dir_5+FILENAME)

if os.path.exists(execution_dir_6+FILENAME):
	print("remove file -", execution_dir_6+FILENAME)
	os.remove(execution_dir_6+FILENAME)

print("=" * 100)
