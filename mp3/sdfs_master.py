import time
import random

SDFS_PREFIX = 'sdfs/'
SDFS_CACHE_PREFIX = '/home/zli104/sdfs/cache/'
SDFS_ABSOLUTE_PATH = '/home/zli104/sdfs/'


CACHE0_PATH = '/home/zli104/sdfs/cache/0/'
CACHE1_PATH = '/home/zli104/sdfs/cache/1/'
CACHE2_PATH = '/home/zli104/sdfs/cache/2/'
CACHE3_PATH = '/home/zli104/sdfs/cache/3/'
CACHE4_PATH = '/home/zli104/sdfs/cache/4/'


class SDFS_Master():
    def __init__(self,metadata,member_list,latest_five):

        self.member_list = member_list
        self.file_metadata = metadata
        self.latest_five_ver_file = latest_five
    
    def update_member_list(self, member_list):
        if member_list:
            self.member_list = member_list
        else:
            self.member_list = member_list

    def put_file_metadata(self, filename, value):
        if value:
            self.file_metadata[filename] = value
        else:
            self.file_metadata[filename] = value

    def update_metadata(self, member_list):
        to_replicate = {}
        tmp_list = []
        for item in member_list:
            tmp_list.append(item[0])
        alive = set(tmp_list)
        for filename in self.file_metadata:
            good = []
            for node in self.file_metadata[filename][0]:
                if node in alive:
                    good.append(node)
            if len(good) < 4:
                ver = self.file_metadata[filename][1]
                self.file_metadata[filename][0] = list(good)
                self.init_replica_nodes(filename)
                new_nodes = list(set(self.file_metadata[filename][0]) - set(good))
                to_replicate[filename] = [good[0], ver, new_nodes]
        return to_replicate

    def handle_put_request(self, filename):
        self.update_timestamp(filename)
        self.init_replica_nodes(filename)
        self.file_metadata[filename][1] += 1
        ips = self.file_metadata[filename][0]
        ver = self.file_metadata[filename][1]
        result = {}
        result['ips'] = ips
        result['ver'] = ver

        print(result)
        return result

    def update_cache(self,filename,target_ip,ver):

        if filename not in self.latest_five_ver_file:
            self.latest_five_ver_file[filename] = []
            for i in range(5):
                self.latest_five_ver_file[filename].append([])

        self.update_cache_helper(filename,ver,target_ip)


    def update_cache_helper(self,filename,ver,target_ip):
        modulo = ver % 5
        if  len(self.latest_five_ver_file[filename][modulo]  ) !=0:
            self.latest_five_ver_file[filename][modulo] = []
        self.latest_five_ver_file[filename][modulo].append(target_ip)


    def select_random_ip(self):
        num = random.randint(0, len(self.member_list) - 1)
        return self.member_list[num][0]

    def init_replica_nodes(self, filename):
        tmp_list = self.file_metadata[filename][0]
        replicas = set(tmp_list)
        replica_length = len(replicas)
        while replica_length < 4:
            ip = self.select_random_ip()
            if ip in replicas:
                continue
            else:
                replicas.add(ip)
                self.file_metadata[filename][0].append(ip)
            replica_length = len(replicas)

    def get_file_timestamp(self, filename):
        return self.file_metadata[filename][2] if filename in self.file_metadata else -1

    def get_file_version(self, filename):
        return self.file_metadata[filename][1] if filename in self.file_metadata else -1
    
    
    def get_file_replica_list(self, filename):
        return self.file_metadata[filename] if filename in self.file_metadata else []

    def get_file_version_cache_list(self, filename):
        if filename in self.latest_five_ver_file:
            return self.latest_five_ver_file[filename]
        else:
            return []

    def file_updated_recently(self, filename):
        flag = filename in self.file_metadata
        if flag:
            cur_time = time.time()
            last_update = self.get_file_timestamp(filename)
            return cur_time - last_update < 60
        return False

    def initialize_metadata(self,filename):
        tmp_list = [[],0,0]
        self.file_metadata[filename] = []
        for i in tmp_list:
            self.file_metadata[filename].append(i)


    def update_timestamp(self, filename):
        flag = filename in self.file_metadata
        if flag:
            cur_time = time.time()
            self.file_metadata[filename][2] = cur_time
        else:
            self.initialize_metadata(filename)
            cur_time = time.time()
            self.file_metadata[filename][2] = cur_time

    def delete_file_info(self, sdfs_filename):
        flag = sdfs_filename in self.file_metadata
        if flag:
            old_nodes = self.file_metadata[sdfs_filename][0]
            del self.file_metadata[sdfs_filename]
            return old_nodes
        else:
            return []





