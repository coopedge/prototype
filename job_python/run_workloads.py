import subprocess

class Workload:
    def __init__(self):
        super().__init__()

    def run(self, count, round):
        count = 0
        while count < round:
            p = subprocess.Popen(['~/development/ycsb-0.17.0/bin/ycsb.sh run basic -P ~/development/ycsb-0.17.0/workloads/workloada -p operationcount=%s' % count], shell = True)
            p.wait()
            count = count + 1
