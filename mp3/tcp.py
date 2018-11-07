from random import random
from socket import *
from xmlrpc.server import SimpleXMLRPCServer
import time
from xmlrpc.server import SimpleXMLRPCRequestHandler
from helper import *
import signal

class MyTimeout():


    def __init__(self, sec):
        self.sec = sec

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.sec)

    def __exit__(self, *args):
        signal.alarm(0)  # disable alarm

    def raise_timeout(self, *args):
        raise MyTimeout.MyTimeout()

    class MyTimeout(Exception):
        pass
# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Create Server Object
server = SimpleXMLRPCServer(
            ("0.0.0.0", 8000),
            requestHandler=RequestHandler,
            logRequests=False,
            allow_none=True
)
server.register_introspection_functions()


class TCPServer():
    def __init__(self, slave, sdfs_master, logger):
        self._slave = slave
        self._sdfs_master = sdfs_master
        self._logger = logger




    def put_file_info(self, sdfsfilename, requester_ip):
        start_time = time.time()
        flag = self._sdfs_master.file_updated_recently(sdfsfilename)
        if flag:
            self._logger.info('Write conflict detected: {}s'.format(
                time.time() - start_time,
            ))
            try:
                with MyTimeout(30):
                    flag1 = requester_ip == getfqdn()
                    if not flag1:
                        requester_handle = get_tcp_client_handle(requester_ip)
                        if not requester_handle.confirmation_handler():
                            return False
                    else:
                        command = input('This file was recently updated, are you sure you want to proceed? (yes/no) ')
                        if command != 'yes':
                            return False

            except MyTimeout.MyTimeout:
                return False

        put_info = self._sdfs_master.handle_put_request(sdfsfilename)
        return put_info

    def get_file_info(self, sdfsfilename):

        replica_list = self._sdfs_master.get_file_replica_list(sdfsfilename)
        return replica_list

    def get_file_versioned_list(self,sdfsfilename):
        file_versioned_list = self._sdfs_master.get_file_version_cache_list(sdfsfilename)
        return file_versioned_list

    def delete_file_info(self, sdfsfilename):
        delete_file_info = self._sdfs_master.delete_file_info(sdfsfilename)
        return delete_file_info

    def get_file_version(self,sdfsfilename):
        file_version = self._sdfs_master.get_file_version(sdfsfilename)
        return file_version


    def confirmation_handler(self):

        try:
            with MyTimeout(30):
                command = input('This file was recently updated, are you sure you want to proceed? (yes/no) ')

                return command == 'yes'

        except MyTimeout.MyTimeout:
            return False
    
    def remote_put_to_replica(self, target_ip, local_filename, sdfs_filename, ver):

        flag = False
        cmd = 'Got Remote Put Request {} {} {}'.format(
            target_ip,
            sdfs_filename,
            ver
        )

        self._logger.info(cmd)
        flag = True
        self._slave.put_to_replica(target_ip, local_filename, sdfs_filename, ver)
        return flag

    def put_file_data(self, sdfs_filename, file, ver, requester_ip):
        flag = False
        self._slave.put_file_data(sdfs_filename, file, ver, requester_ip)
        flag = True
        return flag

    def get_file_data(self, sdfs_filename, ver):
        file_data = self._slave.get_file_data(sdfs_filename, ver)
        return file_data

    def delete_file_data(self, sdfs_filename):
        flag = False
        self._slave.delete_file_data(sdfs_filename)
        flag = True
        return flag

    def vote(self, ip):
        rec_vote = self._slave.receive_vote(ip)
        return rec_vote

    def assign_new_master(self, ip):
        return self._slave.assign_new_master(ip)

    def update_file_version(self, filename, ver):

        flag = False
        self._slave.update_file_version(filename, ver)
        flag = True
        return flag

    def update_cache(self,filename,target_ip,ver):
        self._slave.update_cache(filename,target_ip,ver)





