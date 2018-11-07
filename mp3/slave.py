import time
from operator import itemgetter
import random
import os
from threading import Thread
import traceback
from threading import Lock
from sdfs_slave import SDFS_Slave

from socket import *
import math
from helper import *


SDFS_ABSOLUTE_PATH = '/home/zli104/sdfs/'
HEARTBEAT_PERIOD = 0.9

INTRODUCER = 'fa18-cs425-g73-01.cs.illinois.edu'
SDFS_PREFIX = 'sdfs/'


CACHE0_PATH = '/home/zli104/sdfs/cache/0/'
CACHE1_PATH = '/home/zli104/sdfs/cache/1/'
CACHE2_PATH = '/home/zli104/sdfs/cache/2/'
CACHE3_PATH = '/home/zli104/sdfs/cache/3/'
CACHE4_PATH = '/home/zli104/sdfs/cache/4/'


class Slave():
    
    def __init__(self, logger, sdfs_master):
        self._member_list = []

        self._recent_removed = []

        self._alive = False

        self._my_socket = socket(AF_INET, SOCK_DGRAM)
        self._neighbors = []
        self._sdfs = SDFS_Slave()
        self._voting = False
        self._sdfs_master = sdfs_master
        self._logger = logger
        self._vote_num = 0
        self._lock = Lock()
        self._voting = False
        self._voters = {}
        self._master = INTRODUCER
        self._hb_lock = Lock()
        self._work_in_progress = {}



    def is_alive(self):
        if not self._alive:
            return False
        return len(self._member_list) >= 4

    def init_join(self):
        self._alive = True
        flag = len(self._member_list) < 4
        flag = flag or self._member_list[0][1] < 1
        while flag:
            self.send_udp_message(self._master, [('join', getfqdn())])
            time.sleep(0.9)
            flag = len(self._member_list) < 4 or self._member_list[0][1] < 1

    def send_udp_message(self, target, obj):
        self._my_socket.sendto(base64.b64encode(str(obj).encode('utf-8')), (target, 9000))

    def send_heartbeat(self):
        while True:
            time.sleep(0.9)
            if self.is_alive():
                self.update_heartbeat()
                has_sent = set()

                self._hb_lock.acquire()
                for index in self._neighbors:
                    if index < len(self._member_list):
                        ip = self._member_list[index][0]
                        if random.randint(0, 99) >= 0 and (ip not in has_sent):
                            self.send_udp_message(ip, self._member_list)
                            has_sent.add(ip)
                self._hb_lock.release()

    def update_heartbeat(self):
        idx = [m[0] for m in self._member_list].index(getfqdn())
        new_hb = self._member_list[idx][1] +1
        new_time = time.time()
        tuple = (self._member_list[idx][0],new_hb,new_time)
        self._member_list[idx] = tuple

    def maintenance_func(self):
        '''
     e   This will maintain and checks the states of each server node
        '''
        while True:
            time.sleep(0.9)
            if self.is_alive():
                self.detect_failure()
                self.delete_removed_list()
                self._sdfs_master.update_member_list(self._member_list)
                temp_list = []
                for x in self._member_list:
                    temp_list.append(x[0])
                if self._master not in temp_list:
                    self.revote_master()

    def delete_removed_list(self):

        cur_time = time.time()
        tmp_list = []
        for member in self._recent_removed:
            if cur_time <= member[2] + 10:
                tmp_list.append(member)
        self._recent_removed = tmp_list
    
    def detect_failure(self):
        cur_time = time.time()
        need_update = False

        for member in self._member_list:
            flag = member[0] != getfqdn()
            if flag and member[2] < cur_time - 2.2 and member[1]>0:
                need_update = True
                self._recent_removed.append(member)
                pop_element = []
                for m in self._member_list:
                    pop_element.append(m[0])
                pop_element_index = pop_element.index(member[0])
                self._member_list.pop(pop_element_index)


        self.update_neighbors()
        self._sdfs_master.update_member_list(self._member_list)
        if need_update:
            worker = Thread(target = self.fail_recover)
            worker.run()

    def fail_recover(self):
        time.sleep(7.2)

        self._sdfs_master.update_member_list(self._member_list)
        start_time = time.time()

        update_meta = self._sdfs_master.update_metadata(self._member_list)
        flag= len(update_meta) != 0

        if flag:
            for filename in update_meta:
                good_node, ver, new_nodes = update_meta[filename]
                good_node_handle = get_tcp_client_handle(good_node)
                for ip in new_nodes:
                    self._logger.info('Reparing {}@{}'.format(filename,ip))
                    flag2 = good_node == getfqdn()
                    if not flag2:
                        good_node_handle.remote_put_to_replica(ip, SDFS_ABSOLUTE_PATH + filename, filename, ver)
                    else:
                        self.put_to_replica(ip, SDFS_ABSOLUTE_PATH + filename, filename, ver)
            self._logger.info("Repair done [{}s]".format(time.time() - start_time))







    def merge_member_list(self, remote_member_list):


        if self._alive:
            cur_time = time.time()
            j = 0
            my_len = len(self._member_list)
            remote_len = len(remote_member_list)
            for i in range(remote_len):
                flag1 = j < my_len
                while flag1 and self._member_list[j][0] < remote_member_list[i][0]:
                        j+=1
                flag1 = j < my_len
                if flag1 and remote_member_list[i][0] == self._member_list[j][0]:
                    remote_hb = remote_member_list[i][1]
                    self_hb = self._member_list[j][1]
                    if remote_hb > self_hb:
                        self._member_list[j] = (self._member_list[j][0], remote_member_list[i][1], cur_time)
                    j += 1
                else:

                    tmpList = []
                    for m in self._recent_removed:
                        tmpList.append(m[0])
                    domain_name = remote_member_list[i][0]
                    flag1 = domain_name not in tmpList
                    if flag1 or self._recent_removed[tmpList.index(domain_name)][1] < \
                            remote_member_list[i][1]:
                        ith_ip = remote_member_list[i]
                        self._member_list.append(ith_ip)
                        flag = domain_name not in tmpList
                        if not flag:
                            continue
                        self._logger.debug("Time[{}]: {} is joining.".format(time.time(), domain_name))
            self._member_list = sorted(self._member_list, key=itemgetter(0))
            self.update_neighbors()


    def update_neighbors(self):
        self._hb_lock.acquire()
        idx = [m[0] for m in self._member_list].index(getfqdn())


        self._neighbors = [
            (idx-2) % len(self._member_list),
            (idx-1) % len(self._member_list),
            (idx+1)  % len(self._member_list),
            (idx+2)  % len(self._member_list),
        ]


        self._hb_lock.release()

    def handle_join_request(self, joiner_ip):
        '''
        used by introducer only
        handle all join request, enable entire system when enough machine has joined
        '''

        temp_list = [m[0] for m in self._member_list]

        if joiner_ip not in temp_list:
            self._logger.info("Time[{}]: {} is joining.".format(time.time(), joiner_ip))
            cur_time = time.time()
            self._member_list.append((joiner_ip,0,cur_time))
            self._member_list = sorted(self._member_list, key=itemgetter(0))

            # assumption that there is more than 5 machine at any given time
            flag = len(self._member_list) == 4
            if flag:
                self.update_heartbeat()
                self.update_neighbors()
                for member in self._member_list:
                    if member[0] != getfqdn():
                        self.send_udp_message(member[0], self._member_list)

    def handle_leave_request(self, leaver_ip):
        '''
        used by introducer/leader
        '''

        
        leaver_index = [m[0] for m in self._member_list].index(leaver_ip)
        self._logger.info("Time[{}]: {} volunterally left".format(time.time(), leaver_ip))
        self._recent_removed.append(self._member_list[leaver_index])
        self._member_list.pop(leaver_index)
        self.update_neighbors()


    def leave(self):
        reset_member_list = []
        self._alive = False
        for member in self._member_list:
            cur_time = time.time()
            tuple = (member[0],0,cur_time)
            reset_member_list.append(tuple)
            flag = member[0] != getfqdn()
            if flag:
                self.send_udp_message(member[0], [('leave', getfqdn())])
        self._member_list = reset_member_list

    def put_to_sdfs(self, local_filename, sdfs_filename):

        start_time = time.time()
        put_info = get_tcp_client_handle(self._master).put_file_info(sdfs_filename, getfqdn())
        if not put_info:
            self._logger.info('Put operation aborted.')
            return False

        self._lock.acquire()
        self._work_in_progress[sdfs_filename] = []
        self._lock.release()

        for ip in put_info['ips']:
            p = Thread(target=self.put_to_replica, args=(ip, local_filename, sdfs_filename, put_info['ver']))
            p.start()

        for ip in put_info['ips']:
            p = Thread(target = self.put_version_cache, args = (ip,local_filename,sdfs_filename,put_info['ver']))
            p.start()

        # return to user immediately when quorum is satisfied
        while True:
            time.sleep(0.9)

            if len(self._work_in_progress[sdfs_filename]) < math.ceil((len(put_info['ips']) + 1) / 2):
                continue

            self._logger.info('Put %s@ : Version %d Done. [%fs]' % (
                sdfs_filename,
                put_info['ver'],
                time.time() - start_time
            ))
            self.delete_work_in_progress(sdfs_filename)
            return True

    def delete_work_in_progress(self,sdfsfilename):
        self._lock.acquire()
        del self._work_in_progress[sdfsfilename]
        self._lock.release()


    def put_version_cache(self, target_ip, local_filename, sdfs_filename, ver):
        '''

        :param target_ip:
        :param local_filename:
        :param sdfs_filename:
        :param ver: we assume that ver<=5
        :return:
        '''
        replica_handle = get_tcp_client_handle(self._master)


        ver_prefix = ''

        if ver % 5 == 0:
            ver_prefix = CACHE0_PATH
        elif ver % 5 == 1:
            ver_prefix = CACHE1_PATH
        elif ver % 5 == 2:
            ver_prefix = CACHE2_PATH
        elif ver % 5 == 3:
            ver_prefix = CACHE3_PATH
        else:
            ver_prefix = CACHE4_PATH

        try:

            os.system('scp {} {}@{}:{}'.format(
                local_filename,
                'zli104',
                target_ip,
                ver_prefix + sdfs_filename
            ))
            replica_handle.update_cache(sdfs_filename,target_ip,ver)
        except:
            traceback.print_exc()
            self._logger.info("cache update of local file {} failed for {}!".format(local_filename,target_ip))


    def put_to_replica(self, target_ip, local_filename, sdfs_filename, ver):
        replica_handle = get_tcp_client_handle(target_ip)
        try:

            os.system('scp {} {}@{}:{}'.format(
                local_filename,
                'zli104',
                target_ip,
                SDFS_ABSOLUTE_PATH + sdfs_filename
            ))
            replica_handle.update_file_version(sdfs_filename, ver)
        
        except:
            traceback.print_exc()
            self._logger.info("local file {} doesn't exist".format(local_filename))


        flag = local_filename.startswith(SDFS_PREFIX)
        if not flag:
            self._lock.acquire()
            if sdfs_filename in self._work_in_progress:
                self._work_in_progress[sdfs_filename].append(1)
            self._lock.release()


    def get_from_replica(self, target_ip, sdfs_filename, ver):
        file_data = get_tcp_client_handle(target_ip).get_file_data(sdfs_filename, ver)
        self._lock.acquire()
        if sdfs_filename in self._work_in_progress:
            self._work_in_progress[sdfs_filename].append(file_data)
        self._lock.release()
        return file_data
    
    def get_file_data(self, sdfs_filename, ver):
        flag = self._sdfs.get_file_version(sdfs_filename) < ver

        if flag:
            p = Thread(target=self.get, args=(sdfs_filename, SDFS_ABSOLUTE_PATH + sdfs_filename))
            p.start()
        return {'ver': self._sdfs.get_file_version(sdfs_filename),
                    'ip': getfqdn()}


    def get(self, sdfs_filename, local_filename):

        self._logger.info('contacting master for file {}'.format(sdfs_filename))
        start_time = time.time()
        replica_list = get_tcp_client_handle(self._master).get_file_info(sdfs_filename)

        flag = len(replica_list) > 0

        if flag:
            ips = replica_list[0]

            self._lock.acquire()
            self._work_in_progress[sdfs_filename] = []
            self._lock.release()

            for ip in ips:
                p = Thread(target=self.get_from_replica, args=(ip, sdfs_filename, replica_list[1]))
                p.start()

            while True:
                time.sleep(0.9)
                quorom = math.ceil((len(ips) + 1) / 2)
                if len(self._work_in_progress[sdfs_filename]) >= quorom:
                    break

            self._lock.acquire()
            for file_meta in self._work_in_progress[sdfs_filename]:
                local_ver = file_meta['ver']
                flag1 = local_ver == replica_list[1]
                flag2 = len(self._work_in_progress[sdfs_filename]) == 1
                if flag1 or flag2:
                    os.system('scp {}@{}:{} {}'.format(
                        'zli104',
                        file_meta['ip'],
                        SDFS_ABSOLUTE_PATH + sdfs_filename,
                        local_filename
                    ))
                    break;
            del self._work_in_progress[sdfs_filename]
            self._lock.release()

            is_sdfs_file = local_filename.startswith(SDFS_ABSOLUTE_PATH)

            if not is_sdfs_file:
                self._logger.info('write to local file {}'.format(local_filename))
                self._logger.info("Get done [{}s]".format(time.time() - start_time))
            else:
                self._sdfs.update_file_version(sdfs_filename, replica_list[1])
        else:
            self._logger.info('NO FILE NAMED {} FOUND.'.format(sdfs_filename))
            return False


    def ls(self, sdfs_filename):
        replica_list = get_tcp_client_handle(self._master).get_file_info(sdfs_filename)

        if len(replica_list) > 0:
            self._logger.info('File: {} Ver:{}'.format(sdfs_filename,replica_list[1]))
            for i, ip in enumerate(replica_list[0]):
                self._logger.info('Replica {}: {}'.format(i,ip,))
        else:
            self._logger.info('No Such File.')

    def get_versions(self, sdfs_filename, num_versions, local_filename):
        master_handle = get_tcp_client_handle(self._master)
        replica_list = master_handle.get_file_versioned_list(sdfs_filename)
        version = master_handle.get_file_version(sdfs_filename)
        get_ver = min(int(version),int(num_versions))

        os.system("rm "+local_filename)
        f = open(local_filename,"a+")

        for i in range(get_ver):

            version_index = (version-i) % 5
            ver_prefix = ''

            if version_index == 0:
                ver_prefix = CACHE0_PATH
            elif version_index == 1:
                ver_prefix = CACHE1_PATH
            elif version_index == 2:
                ver_prefix = CACHE2_PATH
            elif version_index == 3:
                ver_prefix = CACHE3_PATH
            else:
                ver_prefix = CACHE4_PATH

            first_ip = replica_list[version_index][0]

            os.system('scp {}@{}:{} \'testing\' '.format(
                        'zli104',
                        first_ip,
                        ver_prefix + sdfs_filename,
                    ))
            f_temp = open("testing","r",encoding = "ISO-8859-1")
            temp_data = f_temp.read()
            f_temp.close()
            os.system("rm testing")
            f.write("The number " + str(get_ver-i) +  " Newest Version :\n")
            f.write(temp_data)
            f.write("\n")

        f.close()

    def store(self):
        file = self._sdfs.ls_file()
        for k in file:
            self._logger.info('File: {} Ver: {}'.format(k,file[k]))

    def delete(self, sdfs_filename):
        old_nodes = get_tcp_client_handle(self._master).delete_file_info(sdfs_filename)
        for node in old_nodes:
            if node != getfqdn():
                node_handle = get_tcp_client_handle(node)
                node_handle.delete_file_data(sdfs_filename)
            else:
                self._sdfs.delete_file_data(sdfs_filename)
                continue
        self._logger.info("deletion is done for {}".format(sdfs_filename))

    def delete_file_data(self, sdfs_filename):
        self._logger.info("file is deleted: {}".format(sdfs_filename))
        self._sdfs.delete_file_data(sdfs_filename)



    def initialize_vote(self):
        self._vote_num = 0
        self._voters = {}
        self._voting = True

    def revote_master(self):
        '''
        vote for the machine with lowest number
        Note that the new master will only have one ticket lol
        '''

        if not self._voting:
            self.initialize_vote()
        flag = getfqdn() == self._member_list[0][0]
        if not flag:
            get_tcp_client_handle(self._member_list[0][0]).vote(getfqdn())
        else:
            self._vote_num += 1
            return


    def receive_vote(self, voter):
        if not self._voting:
            self.initialize_vote()
        if voter not in self._voters:
            self._voters[voter] = 1
            self._vote_num += 1

        flag = self._vote_num > len(self._member_list) / 2

        if self._master != getfqdn():
            if flag:

                self._logger.info("I am voted to be the new master")
                self._master = getfqdn()
                p = Thread(target=self.rebuild_file_meta)
                p.start()

    def rebuild_file_meta(self):

        time.sleep(1.8)
        tmp_file_meta = {}
        for member in self._member_list:
            if member[0] != getfqdn():
                member_files = get_tcp_client_handle(member[0]).assign_new_master(getfqdn())
            else:
                member_files = self._sdfs.ls_file()
            for filename in member_files:
                ver = member_files[filename]
                flag = filename not in tmp_file_meta
                if flag:
                    tmp_file_meta[filename] = []
                tmp_file_meta[filename].append([member[0], ver])

        for filename in tmp_file_meta:
            file_list = tmp_file_meta[filename]
            sorted(file_list, key=lambda x : x[1])
            value = []
            tmp_list = [x[0] for x in file_list]
            tmp_list_min = tmp_list[0:min(len(file_list), 4)]
            value.append(tmp_list_min)
            temp = file_list[0][1]
            value.append(temp)
            cur_time = time.time()
            value.append(cur_time)
            self._sdfs_master.put_file_metadata(filename, value)
        self._voters = {}
        self._voting = False
        self._logger.info("SDFS file metadata has been rebuilt")
        
        p = Thread(target=self.fail_recover)
        p.start()



    def update_file_version(self, filename, ver):
        self._sdfs.update_file_version(filename, ver)


    def update_cache(self,filename,target_ip,ver):
        self._sdfs_master.update_cache(filename,target_ip,ver)

    def assign_new_master(self, new_master):

        self._logger.info("accepting new matser: {}".format(new_master))
        self._voting = False
        self._master = new_master
        return self._sdfs.ls_file()


    def run(self):
        my_thread = Thread(target=self.send_heartbeat)
        maintenance_thread = Thread(target=self.maintenance_func)
        my_thread.start()
        maintenance_thread.start()
