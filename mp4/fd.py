import socket
import random
import threading
from glob import *
import json
import time
import datetime
from collections import defaultdict


class MessageField:
    TYPE = 'message_type'
    HOST = 'host'
    PORT = 'port'
    INFO = 'info'
    ID = 'id'

class MembershipList:
    def __init__(self, host_name, init_id):
        self.d = {
            host_name: {
                'status': 'LEAVED',
                'id': init_id,
                'ts': datetime.datetime.now().strftime(TIME_FORMAT_STRING)
            }
        }



class FailureDetector:
    def __init__(self, host_name, port):
        self.id = random.randint(0, 65535)
        self.host = host_name
        self.nbs = CONNECTIONS[host_name]
        self.ml = MembershipList(host_name=host_name, init_id=self.id)
        self.port = port
        self.ml_lock = threading.Lock()
        self.addr = (self.host, self.port)

        self.timer = {}
        self.cd = defaultdict(int)
        self.checker_lock = threading.Lock()
        self.sent = set()

    def generate_short_message(self,type,host):
        message = {
            'type': type,
            'host': host,
        }
        return message

    def generate_message(self,messageType,messagehost,messageport,messageInfo):
        message = {
            MessageField.TYPE: messageType,
            MessageField.HOST: messagehost,
            MessageField.PORT: messageport,
            MessageField.INFO: messageInfo
        }
        return message

    def print_ml(self):
        """
        Print Membership List to the terminal.

        :return: None
        """

        print('=== MembershipList on %s ===' % self.host)
        for k, v in self.ml.d.items():
            print('%s: %d [%s] [%s]' % (k, v['id'], v['status'], v['ts']))
        print('============================')


    def activate(self,status):
        '''
        update the membership list
        :param status:
        :return:
        '''
        self.ml.d[self.host]['status'] = status
        self.ml.d[self.host]['ts'] = datetime.datetime.now().strftime(TIME_FORMAT_STRING)
        self.ml.d[self.host]['id'] = random.randint(0, 65535)

    def is_introducer(self):
        return self.host == INTRODUCER_HOST


    def receiver(self):

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(self.addr)
            while True:
                mld = self.ml.d
                selfStatus = mld[self.host]['status']

                if selfStatus != 'LEAVED':
                    # UDP receiver
                    data, server = s.recvfrom(4096)
                    if data:
                        msg = json.loads(data.decode('utf-8'))
                        flag = msg.get(MessageField.TYPE, '#')=='#'
                        if flag:
                            continue
                        msg_type = msg.get(MessageField.TYPE, '#')
                        from_host = msg[MessageField.HOST]
                        now = datetime.datetime.now().strftime(TIME_FORMAT_STRING)
                        self.ml_lock.acquire()

                        if msg_type == 'JOIN':
                            mld[from_host] = msg[MessageField.INFO]
                            mld[from_host]['status'] = 'JOINING'
                            mld[from_host]['ts'] = now
                            self.sent.discard(from_host)
                            flag2 = self.is_introducer()
                            if flag2:
                                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s1:
                                    join_msg = self.generate_message('JOIN',from_host,DEFAULT_PORT_FD,mld[from_host])

                                    for host in ALL_HOSTS:
                                        if host != from_host :
                                            if host != self.host:
                                                json_dump = json.dumps(join_msg).encode('utf-8')
                                                s1.sendto(json_dump, (host, DEFAULT_PORT_FD))
                                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s2:
                                    jsonMessage = self.generate_short_message('join',[from_host])
                                    jsonMessageDump = json.dumps(jsonMessage).encode('utf-8')
                                    s2.sendto(jsonMessageDump, (self.host, DEFAULT_PORT_SDFS))
                            self.ml_lock.release()
                            continue
                        if msg_type == 'PING':

                            info = msg[MessageField.INFO]
                            for host in info:
                                flag = host in mld
                                if flag:
                                    old_time = datetime.datetime.strptime(mld[host]['ts'], TIME_FORMAT_STRING)
                                    new_time = datetime.datetime.strptime(info[host]['ts'], TIME_FORMAT_STRING)
                                    diff = new_time > old_time
                                    if diff :
                                        mld[host] = info[host]
                                else:
                                    mld[host] = info[host]
                            hostInfo = mld[self.host]
                            ack_msg = self.generate_message('ACK',self.host,DEFAULT_PORT_FD,hostInfo)
                            jsonMessage = json.dumps(ack_msg).encode('utf-8')
                            s.sendto(jsonMessage, (from_host, DEFAULT_PORT_FD))
                            self.ml_lock.release()
                            continue
                        if msg_type == 'ACK':

                            mld[from_host] = msg['info']
                            flag11 = from_host in self.timer
                            if flag11:
                                self.checker_lock.acquire()
                                flag12 = from_host in self.timer
                                if flag12:
                                    del self.timer[from_host]  # clean time table
                                self.checker_lock.release()
                            self.ml_lock.release()
                            continue
                        if msg_type == 'LEAVE':
                            mld[from_host]['status'] = 'LEAVED'
                            mld[from_host]['ts'] = now
                            self.ml_lock.release()
                            continue



    def sender(self):


        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            while True:
                try:
                    mld = self.ml.d
                    time.sleep(0.5)

                    self_status = mld[self.host]['status']

                    if self_status != 'LEAVED':
                        now = datetime.datetime.now()
                        self.ml_lock.acquire()

                        mld[self.host]['status'] = 'RUNNING'
                        mld[self.host]['ts'] = now.strftime(TIME_FORMAT_STRING)

                        for host in self.nbs:

                            if host not in mld:
                                    continue
                            if mld[host]['status'] == 'LEAVED' :
                                continue
                            ping_msg = self.generate_message('PING',self.host,self.port,mld)
                            json_message = json.dumps(ping_msg).encode('utf-8')
                            s.sendto(json_message, (host, self.port))
                            self.checker_lock.acquire()
                            if host in mld:
                                if host not in self.timer:
                                    cur_time = datetime.datetime.now()
                                    self.timer[host] = cur_time
                            self.checker_lock.release()
                        self.ml_lock.release()
                except Exception as e:
                    print(e)

    def checker(self):

        while True:
            mld = self.ml.d
            timer = self.timer
            timer_key_list = list(timer.keys())
            for host in timer_key_list:
                now = datetime.datetime.now()
                time_delta = now - timer.get(host, datetime.datetime.now())
                if time_delta.days >= 0 :
                    if time_delta.seconds > 2.:
                        if host in mld :
                            if mld[host]['status'] not in {'FAILED', 'LEAVED'}:
                                mld[host]['status'] = 'FAILED'
                                cur_time = now.strftime(TIME_FORMAT_STRING)
                                mld[host]['ts'] = cur_time
                        try:
                            del timer[host]
                        except Exception as e:
                            pass
            key_list = list(mld.keys())
            for host in key_list:
                flag = host in mld
                if flag:
                    host_ts = mld[host]['ts']
                    ts = datetime.datetime.strptime(host_ts, TIME_FORMAT_STRING)
                    now = datetime.datetime.now()
                    delta = now - ts
                    if mld[host]['status'] == 'FAILED' :
                        if delta.days >= 0 :
                            if delta.seconds > 3. :
                                if host not in self.sent:
                                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                                        failed_msg = self.generate_short_message('failed_relay',host)
                                        failed_msg_dump = json.dumps(failed_msg).encode('utf-8')
                                        s.sendto(failed_msg_dump, (self.host, DEFAULT_PORT_SDFS))
                                    self.sent.add(host)



    def join(self):

        self.activate('RUNNING')
        if self.is_introducer():
            info = '[INFO] I\'m introducer!'
            print(info)
        else:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                self_info = self.ml.d[self.host]
                join_msg = self.generate_message('JOIN',self.host,self.port,self_info)

                s.sendto(json.dumps(join_msg).encode('utf-8'), (INTRODUCER_HOST, DEFAULT_PORT_FD))





    def leave(self):
        """
        Notifies all members in the membership list that it will leave
        """

        self_status = self.ml.d[self.host]['status']

        if self_status == 'RUNNING':
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                leave_msg = {
                    MessageField.TYPE: 'LEAVE',
                    MessageField.HOST: self.host,
                    MessageField.PORT: self.port,
                }
                json_message = json.dumps(leave_msg).encode('utf-8')
                for host in self.nbs:

                    s.sendto(json_message, (host, DEFAULT_PORT_FD))
                self.ml.d[self.host]['status'] = 'LEAVED'


    def monitor(self):

        while True:
            arg = input('Type join leave or ml:')
            if arg == 'join':
                self.join()
                continue
            if arg == 'leave':
                self.leave()
                continue
            if arg == 'ml':
                self.print_ml()
                continue
            else:
                print('[ERROR] Invalid input arg %s' % arg)

    def run(self):

        t_receiver = threading.Thread(target=self.receiver)
        t_receiver.start()
        t_sender = threading.Thread(target=self.sender)
        t_sender.start()
        t_checker = threading.Thread(target=self.checker)
        t_checker.start()
        t_monitor = threading.Thread(target=self.monitor)
        t_monitor.start()
        t_sender.join()
        t_receiver.join()
        t_monitor.join()
        t_checker.join()


def main():
    s = FailureDetector(host_name=socket.gethostname(), port=DEFAULT_PORT_FD)
    s.run()


if __name__ == '__main__':
    main()
