import os
import socket
import threading
import fd
from pyspark import SparkContext
from collections import defaultdict
import json
sc = SparkContext()

VM6_10 = ['fa18-cs425-g73-06.cs.illinois.edu',  # 06
            'fa18-cs425-g73-07.cs.illinois.edu',  # 07
            'fa18-cs425-g73-08.cs.illinois.edu',  # 08
            'fa18-cs425-g73-09.cs.illinois.edu',  # 09
            'fa18-cs425-g73-10.cs.illinois.edu',  # 10
        ]

INTRODUCER_HOST = 'fa18-cs425-g73-06.cs.illinois.edu'

class Spark:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.addr = (self.host, self.port)
        self.typeDict = defaultdict(int)
        self.server = fd.FailureDetector(host_name=socket.gethostname(), port=52333)


    def sender(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            run_spark_msg = {
                'type': 'run'

            }
            for vm in VM6_10:
                s.sendto(json.dumps(run_spark_msg).encode('utf-8'), (vm, self.port))
                print('have sent to', vm)


    def receiver(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(self.addr)

            while True:
                data, server = s.recvfrom(4096)
                # print('have data')

                if data:
                    msg = json.loads(data.decode('utf-8'))
                    if msg['type'] == 'run':
                        self.spark()

                    if msg['type'] == 'result':
                        print('result received!')
                        recv_dict = msg['dict']
                        for key in recv_dict:
                            self.typeDict[key] += recv_dict[key]
                        print('received result processed!')

    def spark(self):
        lines = sc.textFile('file:///home/zli104/MP4/test2_.txt')
        ttt = lines.map(lambda line: line.split()).collect()

        for i in ttt:
            if len(i)<7:
                continue
            word = i[6].split('.')[1]
            if word == '':
                continue
            if word != '' and word[-1] == '\"':
                word = word[:-1]
            self.typeDict[word] +=1

        if self.host != INTRODUCER_HOST:

            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                result_msg = {
                    'type': 'result',
                    'dict': self.typeDict
                }
                s.sendto(json.dumps(result_msg).encode('utf-8'), (INTRODUCER_HOST, self.port))
            print('result sent!')

        #print(typeDict)

    def print_result(self):
        sum_ = 0

        for (k, v) in sorted(self.typeDict.items(), key=lambda d: d[1], reverse=True):
            if v < 20: continue
            print(k, v)
            sum_ += v
        print('sum of all:', sum_)

    def file_result(self, filename):
        if os.path.exists(filename):
            os.remove(filename)
        with open(filename, 'w+') as output:
            for (k, v) in sorted(self.typeDict.items(), key=lambda d: d[1], reverse = True):
                if v < 20: continue
                output.write(k+' ')
                output.write(str(v))
                output.write('\n')
        output.close()

    def monitor(self):
        while True:
            arg = input('please input cmd:')
            args = arg.split(' ')

            if arg == 'start':
                t_sender = threading.Thread(target=self.sender)
                t_sender.start()
                t_sender.join()

            elif arg == 'print':
                self.print_result()
            elif args[0] == 'file_result':
                if len(args) != 2:
                    print('[ERROR]: need file_result filename ')
                    continue
                self.file_result(args[1])


    def run(self):
        self.server.run()
        t_receiver = threading.Thread(target=self.receiver)
        t_monitor = threading.Thread(target=self.monitor)

        t_receiver.start()
        t_monitor.start()

        t_receiver.join()
        t_monitor.join()

def main():
    s = Spark(host=socket.gethostname(), port=53555)
    s.run()


if __name__ == '__main__':
    main()