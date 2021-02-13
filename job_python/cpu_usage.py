import psutil
import logging
import time
import sys

class CPU:
    def __init__(self, pid):
        super().__init__()
        self.pid = pid

    def run(self):
        # logging.basicConfig(filename="cpu_usage.log", level=logging.DEBUG)
        logger = logging.getLogger()
        hdlr = logging.FileHandler('cpu_usage.log')
        # formatter = logging.Formatter('%(asctime)s  -  %(message)s')
        # hdlr.setFormatter(formatter)
        logger.addHandler(hdlr) 
        logger.setLevel(logging.DEBUG)
        while 1:
            p = psutil.Process(int(self.pid))
            cpu_usage = p.cpu_percent(interval=1)
            t = time.time()
            info = str(t) + '-' + str(cpu_usage)
            logger.info(info)

if __name__ == "__main__":
    pid = sys.argv[1]
    print(pid)
    cpu=CPU(pid)
    cpu.run()