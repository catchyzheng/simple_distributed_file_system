import datetime
import socket
import threading
import json
import hashlib
import subprocess
import os
import shutil
from glob import *
from pprint import pprint
import random
import time
from collections import defaultdict
import fd

'''
ALL_SLAVES = [
            'fa18-cs425-g73-02.cs.illinois.edu',  # 02
            'fa18-cs425-g73-03.cs.illinois.edu',  # 03
            'fa18-cs425-g73-04.cs.illinois.edu',  # 04
            'fa18-cs425-g73-05.cs.illinois.edu',  # 05
            'fa18-cs425-g73-06.cs.illinois.edu',  # 06
            'fa18-cs425-g73-07.cs.illinois.edu',  # 07
            'fa18-cs425-g73-08.cs.illinois.edu',  # 08
            'fa18-cs425-g73-09.cs.illinois.edu',  # 09
            'fa18-cs425-g73-10.cs.illinois.edu',  # 10
        ]
SLAVE_NUM = len(ALL_SLAVES)
'''



class Crane:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.addr = (self.host, self.port)
        self.result = defaultdict(int)
        self.host_dict = defaultdict(int)
        self.dic = defaultdict(int) # record the appearance of msg id
        self.all_heard = False
        self.slaves = []
        self.cur_time = time.time()
        self.lock = threading.Lock()
        self.server = fd.FailureDetector(host_name=socket.gethostname(), port=DEFAULT_PORT_FD)

        #self.server = sdfs.Server(host=socket.gethostname(), port=DEFAULT_PORT_SDFS)

    def dispatch(self, path, mark):
        """
        the introducer dispatch the input stream file to different nodes.

        :return: None
        """



        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            cnt = 0
            try:
                with open(path, 'r',encoding='utf-8',errors='ignore') as input_:
                    output_list = []
                    members = int(len(self.server.ml.d))
                    for i in range(members):
                        if os.path.exists('calgary'+str(i+1)+'.txt'):
                            os.remove('calgary'+str(i+1)+'.txt')
                        output = open('calgary'+str(i+1)+'.txt','w+')

                        output_list.append(output)


                    length = len(self.server.ml.d)

                    for line in input_:
                        #if 'gif' in line:
                        #    continue
                        if mark == 'calgary':
                            '''
                            lst = line.split(' ')
                            if len(lst) < 9:
                                continue
                            '''
                            output_list[cnt % length].write(line)
                            cnt += 1
                        

                    for ele in output_list:
                        ele.close()


                    '''
                        ping_msg = {
                        'source': 'calgary',
                        'type': 'dispatch',
                        'chunk': arr
                    }
                    '''

                    #host_list = list(self.server.ml.d.keys())


                    for i in range(len(output_list)):
                        to_host1 = 'fa18-cs425-g73-' + ('%02d' %(i+1) ) + '.cs.illinois.edu'
                        to_host2 = NEXT_TWO[to_host1][0]
                        to_host3 = NEXT_TWO[to_host1][1]
                        os.system('scp {} {}@{}:{}'.format(
                            'calgary' + str(i + 1) + '.txt',
                            'zli104',
                            to_host1,
                            '~/MP4/data'
                        ))
                        os.system('scp {} {}@{}:{}'.format(
                            'calgary' + str(i + 1) + '.txt',
                            'zli104',
                            to_host2,
                            '~/MP4/data'
                        ))
                        os.system('scp {} {}@{}:{}'.format(
                            'calgary' + str(i + 1) + '.txt',
                            'zli104',
                            to_host3,
                            '~/MP4/data'
                        ))

                        ping_msg = {
                            'source': 'calgary',
                            'type': 'dispatch',
                            'file_id': i+1
                        }
                        s.sendto(json.dumps(ping_msg).encode('utf-8'), (to_host1, self.port))
                        #s.sendto(json.dumps(ping_msg).encode('utf-8'), (to_host2, self.port))
                        #s.sendto(json.dumps(ping_msg).encode('utf-8'), (to_host3, self.port))


                    '''
                    #length = len(self.server..ml.d)
                    host_list = list(self.server..ml.d.keys())
                    to_host1 = host_list[i % length]
                    to_host2 = host_list[(i+1) % length]
                    to_host3 = host_list[(i+2) % length]
                    s.sendto(json.dumps(ping_msg).encode('utf-8'), (to_host1, self.port))
                    s.sendto(json.dumps(ping_msg).encode('utf-8'), (to_host2, self.port))
                    s.sendto(json.dumps(ping_msg).encode('utf-8'), (to_host3, self.port))
                    '''




                    print('finish dispatch time: ', time.time() - self.cur_time)

            finally:
                print('finish dispatch. ')

    def read(self, host_name):
        # fa18-cs425-g73-02.cs.illinois.edu
        print('waiting for lock')
        self.lock.acquire()

        self.result.clear()

        host_num = str(int(host_name.split('.')[0].split('-')[-1]))
        print('start processing calgary', host_num)
        infile = open('./data/calgary'+ host_num + '.txt', 'r',encoding='utf-8',errors='ignore')

        line_cnt = 0
        for line in infile:
            lst = line.split(' ')
            if len(lst) < 9: continue
            key = lst[6].split('.')[1]
            if key == '': continue
            if key != '' and key[-1] == '\"':
                key = key[:-1]
            self.result[key] += 1
            # sleep
            line_cnt += 1
            if line_cnt % 5000 == 0:
                time.sleep(0.5)

        self.lock.release()
        self.file_result('calgary' + host_num + '_stats.txt')
        print('processing finish calgary', host_num)
        infile.close()
        self.send_result(host_name)

    def send_result(self, host_name):
        if self.host != INTRODUCER_HOST:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as ss:
                stats_msg = {
                    'source': 'calgary',
                    'type': 'dict',
                    'host': host_name,
                    'send_dict': self.result
                }
                ss.sendto(json.dumps(stats_msg).encode('utf-8'), (INTRODUCER_HOST, self.port))
            print('result sent!')



    def checker(self):
        membership_list = self.server.ml.d
        #host_list = list(membership_list.keys())

        print('checker membership list')
        for host in membership_list:
            if host == INTRODUCER_HOST:
                self.host_dict[host] = 1
            else:
                self.host_dict[host] = -1
            print(host, self.host_dict[host])

        while True:
            sum_ = 0
            for ele in self.host_dict:
                sum_ += self.host_dict[ele]
            if sum_ == len(self.host_dict):
                print ('total running time:', time.time() - self.cur_time)
                break

            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as ss:

                for host_key in self.host_dict:
                    if self.host_dict[host_key] == -1 and membership_list[host_key]['status'] == 'FAILED':
                        print('please resent the dataset of ', host_key)
                        redo_msg = {
                            'source': 'calgary',
                            'type': 'redo',
                            'redo_host': host_key
                        }
                        if membership_list[NEXT_TWO[host_key][0]]['status'] != 'FAILED':
                            ss.sendto(json.dumps(redo_msg).encode('utf-8'), (NEXT_TWO[host_key][0], self.port))
                        else:
                            ss.sendto(json.dumps(redo_msg).encode('utf-8'), (NEXT_TWO[host_key][1], self.port))
                time.sleep(10)

    def print_current_host_dict(self):
        print('current host dict:')
        for host in self.host_dict:
            print(host, self.host_dict[host])

    def receiver(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(self.addr)

            print('begin into while')

            self_id = int(self.host.split('.')[0].split('-')[-1])
            print('self id:', self_id)



            recv_all = 0
            last_dict = ''
            while True:
                data, server = s.recvfrom(4096)
                # print('have data')

                if data:
                    msg = json.loads(data.decode('utf-8'))
                    msg_source = msg['source']
                    msg_type = msg['type']
                    #pprint(msg)

                    # if calgary
                    if msg_source == 'calgary':
                        if msg_type == 'dispatch':
                            #file_id = msg['file_id'] # file id
                            self.read(self.host)

                        elif msg_type == 'redo':
                            the_host = msg['redo_host']
                            print('receive redo request', the_host)
                            self.read(the_host)

                        elif msg_type == 'dict':
                            if last_dict == msg['host']:
                                continue

                            if self.host_dict[msg['host']] == 1:
                                continue

                            recv_dict = msg['send_dict']
                            for key in recv_dict:
                                self.result[key] += recv_dict[key]

                            print(msg['host'], 'received!')
                            self.host_dict[msg['host']] = 1
                            self.print_current_host_dict()
                            last_dict = msg['host']


                            # recv_all += 1
                            '''
                            alive_num = 0
                            for ele in self.server.failure_detector.ml.d:
                                if ele['status'] == 'RUNNING':
                                    alive_num += 1

                            if recv_all == alive_num:
                                print ('total running time:', time.time() - self.cur_time)
                            '''

                    

    '''
    def print_status(self):
        previous_host = PREVIOUS_TWO[self.host][0]
        previous_prev_host = PREVIOUS_TWO[self.host][1]
        prev_host_status = self.server.failure_detector.ml.d[previous_host]['status']
        prev_prev_host_status = self.server.failure_detector.ml.d[previous_prev_host]['status']
        print('prev', prev_host_status, 'prev prev', prev_prev_host_status)
    '''

    def print_result(self):
        sum_ = 0

        for (k, v) in sorted(self.result.items(), key = lambda d:d[1], reverse = True):
            if v < 20: continue
            print(k, v)
            sum_ += v
        print('sum of all:', sum_)

    def file_result(self, filename):
        if os.path.exists(filename):
            os.remove(filename)
        with open('./' + filename, 'w+') as output:
            for (k, v) in sorted(self.result.items(), key=lambda d: d[1], reverse = True):
                if v < 20: continue
                output.write(k+' ')
                output.write(str(v))
                output.write('\n')
        output.close()

    def clear_result(self):
        self.result.clear()
        self.host_dict.clear()

    '''
    def processing(self, path, mark):
        if mark == 'calgary':
            file = open(path, 'r')

            for line in file:
                lst = line.split(' ')
                if len(lst) < 9:
                    continue
                key = lst[6].split('.')[1]
                if key == '':
                    continue
                self.result[key] += 1
            if self.host != INTRODUCER_HOST:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as ss:
                    all_msg = {
                        'source': mark,
                        'type': 'dict',
                        'host': self.host,
                        'send_dict': self.result
                    }
                    ss.sendto(json.dumps(all_msg).encode('utf-8'), (INTRODUCER_HOST, self.port))

    '''

    def monitor(self):


        while True:
            arg = input('-->')
            args = arg.split(' ')

            if args[0] == 'start' and self.host == INTRODUCER_HOST:
                if len(args) != 3:
                    print('[ERROR]: start filename filename_mark')
                    continue

                if args[2] == 'calgary':
                    t_dispatcher = threading.Thread(target=self.dispatch, args=(args[1], args[2]))
                    t_checker = threading.Thread(target=self.checker)
                    t_dispatcher.start()
                    t_checker.start()
                    t_dispatcher.join()
                    t_checker.join()
                

                


            elif args[0] == 'print':
                if len(args) != 1:
                    print('[ERROR]: need print ')
                    continue
                self.print_result()
            elif arg == 'join':
                self.server.join()
            elif arg == 'leave':
                self.server.leave()
            elif arg == 'ml':
                self.server.print_ml()
            elif args[0] == 'file_result':
                if len(args) != 2:
                    print('[ERROR]: need file_result filename ')
                    continue
                self.file_result(args[1])

            elif arg == 'clear_result':
                if len(args) != 1:
                    print('[ERROR]: need send_result')
                    continue
                self.clear_result()
            elif arg == 'current_host_dict':
                if len(args) != 1:
                    print('[ERROR]: need current_host_dict')
                    continue
                self.print_current_host_dict()
            else:
                print('[ERROR] Invalid input arg %s' % arg)

    def run(self):
        self.server.run()
        if os.path.exists('data'):
            shutil.rmtree('data')
        os.mkdir('data')
        t_receiver = threading.Thread(target=self.receiver)
        t_monitor = threading.Thread(target=self.monitor)

        t_receiver.start()
        t_monitor.start()


        t_receiver.join()
        t_monitor.join()




def main():
    s = Crane(host=socket.gethostname(), port=DEFAULT_PORT_CRANE)
    s.run()



if __name__== '__main__':
    main()

