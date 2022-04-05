# import kubeApiHandler as kubeHandler
import sys, re
from restApiServer import Run

if len(sys.argv) != 2:
    print("Insufficient arguments")
    sys.exit()

listening = sys.argv[1]

if not re.search(r"^([0-9]{1,3}.){3}[0-9]{1,3}$", listening, flags=0):
    print("Invalid host address")
    sys.exit()

# 서버 실행
if __name__ == '__main__':
    # run sever
    Run(listening)

