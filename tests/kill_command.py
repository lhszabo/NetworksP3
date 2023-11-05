from subprocess import check_output
import os, signal
import traceback

def kill_command():
    try:
        cmd = "ps -x | grep 'python3 /18441_project3/solution/tcpserver.py'"
        x = check_output(cmd, shell=True)
        pid_list = [] 
        for line in str(x)[2:-1].split('\\n'):
            # print(line)
            if "grep" in line:
                # print("[X] " + line)
                pass
            elif len(line) != 0:
                print("[kill] " + line)
                stripped = line.strip()
                pid_string = stripped.split(' ')[0]
                pid_list.append(int(pid_string))
        # print("pid list - ", pid_list)
        for pid in pid_list:
            # print(pid)
            os.kill(int(pid), signal.SIGTERM)
        # print("[kill success]")
    except:
        print("[kill run_switchd fail]")
        traceback.print_exc()
