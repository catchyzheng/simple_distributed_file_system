import time
from socket import *
from threading import Thread
import traceback


class commandLine():
    def __init__(self, slave, logger):
        self._logger = logger
        self._slave = slave

    def run_cli(self):
        while True:
            command = input('Enter your command: ')
            try:
                if command == '':
                    continue
                if command == 'lss':
                    self._logger.info('Time[{}]: {}'.format(
                        time.time(), getfqdn())
                    )
                    continue

                if command == 'lsm':
                    cmd = "Time[{}]: current number of members = {}".format(
                        time.time(),
                        len(self._slave._member_list)
                    )
                    self._logger.info(cmd)
                    for member in self._slave._member_list:
                        sen = "\t{}".format(member)
                        self._logger.info(sen)
                    continue
                if command == 'join':
                    self._slave.init_join()
                    continue
                if command == 'leave':
                    self._slave.leave()
                    continue
                if command.startswith('put'):
                    args = command.split(' ')
                    print(args)
                    self._slave.put_to_sdfs(args[1], args[2])
                    continue
                if command.startswith('get') and ( not command.startswith('get-versions')) :
                    args = command.split(' ')
                    print(args)
                    self._slave.get(args[1], args[2])
                    continue
                if command.startswith('ls'):
                    args = command.split(' ')
                    if len(args) == 2:
                        self._slave.ls(args[1])
                        continue
                    elif len(args) < 2:
                        self._slave.store()
                        continue
                if command.startswith('get-versions'):
                    #get-versions sdfsfilename num-versions localfilename
                    args = command.split(' ')
                    print(args)
                    if len(args) == 4:
                        ver = int(args[2])
                        if ver > 5:
                            print("num-versions should not be larger than 5！")
                            continue
                        self._slave.get_versions(args[1], args[2], args[3])
                    continue
                if command.startswith('store'):
                    self._slave.store()
                    continue
                if command.startswith('delete'):
                    args = command.split(' ')
                    self._slave.delete(args[1])
                    continue
            except:
               print("COMMAND NOT SUPPORTED")
               traceback.print_exc()


    def run(self):
        self._logger.info("CLI Started")
        cli_thread = Thread(target = self.run_cli)
        cli_thread.start()

