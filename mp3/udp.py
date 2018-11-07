from threading import Thread
from socket import *
import time
from helper import *



class UDPServer():

    def __init__(self, slave):
        self._udp_count = 0
        self._slave = slave
        self._udp_socket = socket(AF_INET, SOCK_DGRAM)
        self._udp_socket.bind(('0.0.0.0', 9000))
        self._worker_queue = []


    def add_worker_to_queue(self):
        message, address = self._udp_socket.recvfrom(4096)
        self._worker_queue.append(message)
        self._udp_count += 1

    def server_thread(self):

        while True:
            self.add_worker_to_queue()

    def worker_thread(self):


        while True:
            time.sleep(0.9)
            while len(self._worker_queue) > 0:
                message = self._worker_queue.pop()

                remote_member_list = eval(base64.b64decode(message).decode("utf-8"))

                command = remote_member_list[0][0]

                if command == 'join' or command == 'leave':
                    if command == 'join':
                        self._slave.handle_join_request(remote_member_list[0][1])
                        continue
                    elif command == 'leave':
                        self._slave.handle_leave_request(remote_member_list[0][1])
                        continue
                if self._slave._alive: 
                    self._slave.merge_member_list(remote_member_list)
            

    def run_server(self):
        '''
        init the server
        '''
        udp_thread = Thread(target = self.server_thread)
        worker_thread = Thread(target = self.worker_thread)
        udp_thread.start()
        worker_thread.start()

