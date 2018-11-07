import os

from sdfs_master import SDFS_PREFIX, SDFS_ABSOLUTE_PATH


class SDFS_Slave():

    def __init__(self):

        self.local_files = {}

    def update_file_version(self, filename, version):
        self.local_files[filename] = version

    def get_file_version(self, filename):
        file_version = self.local_files[filename]
        return file_version

    def read_file(self,file_path):
        f = open(file_path, "rb")
        file_data = f.read()
        f.close()
        return file_data

    def get_file(self, filename, version):
        flag = self.local_files[filename] != version
        if flag:
            return self.local_files[filename], None
        file_data = self.read_file(SDFS_ABSOLUTE_PATH + filename)
        return self.local_files[filename], file_data

    def get_versioned_file(self,filename,version):
        ver = version % 5
        f = open(SDFS_ABSOLUTE_PATH + filename, "rb")
        file_data = f.read()
        f.close()
        return self.local_files[filename], file_data

    def ls_file(self):
        files = self.local_files
        return files

    def delete_file_data(self, sdfs_filename):
        os.remove(SDFS_ABSOLUTE_PATH + sdfs_filename)
        del self.local_files[sdfs_filename]