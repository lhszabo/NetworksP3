import subprocess, time, filecmp, os
import sys, traceback
from kill_command import kill_command

TIMEOUT = 5

FILENAME = 'Carnegie_Mellon_University.jpg'

print()
print("=" * 100)
print("===> Test case 2.1. Small File transfer test without packet loss")

execution_dir_1 = './node1/'
execution_dir_2 = './node2/'

solution_path = '../solution/tcpserver.py'

print("===> Setup Connection")

node_process_list = []
shell_command_line_1 = 'python3 %s node1.json\n' % solution_path
shell_command_line_2 = 'python3 %s node2.json\n' % solution_path
proc = subprocess.Popen(shell_command_line_1, stdin=subprocess.PIPE, shell=True, universal_newlines=True, cwd=execution_dir_1)
node_process_list.append(proc)
proc = subprocess.Popen(shell_command_line_2, stdin=subprocess.PIPE, shell=True, universal_newlines=True, cwd=execution_dir_2)
node_process_list.append(proc)
time.sleep(2)

print("===> Request a small file and TIMEOUT is %d seconds" % TIMEOUT)

node_process_list[0].stdin.write(FILENAME + '\n')
node_process_list[0].stdin.flush()
time.sleep(TIMEOUT)

print("===> Kill nodes")
try:
	# print("kill process1")
	node_process_list[0].kill()
except:
	traceback.print_exc()

try:
	# print("kill process2")
	node_process_list[1].kill()
except:
	traceback.print_exc()


kill_command()

# Check the case where file does not exist

return_bool = False
check_exists_bool = os.path.exists(execution_dir_1+FILENAME)
if check_exists_bool:
	return_bool = filecmp.cmp(execution_dir_1+FILENAME, execution_dir_2+FILENAME, shallow=False)

if return_bool:
	print("===> Test Case [[[Success]]]")
else:
	print("===> Test Case [[[Fail]]]")

if check_exists_bool:
	print("remove file -", execution_dir_1+FILENAME)
	os.remove(execution_dir_1+FILENAME)

print("=" * 100)
