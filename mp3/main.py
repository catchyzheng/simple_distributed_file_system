import os
import logging

from slave import Slave
from tcp import *
from sdfs_master import SDFS_Master
from udp import UDPServer
from commandline import commandLine


def run_tcp_server(tcp_obj):
    server.register_instance(tcp_obj)
    server.serve_forever()

def initialize_slave(logging,sdfs_m):
    slave = Slave(logging,sdfs_m)
    return slave

def initialize_udp(slave):
    udpserver = UDPServer(slave)
    return udpserver

def initialize_cli(slave,logging):
    cmd_line = commandLine(slave, logging)
    return cmd_line

def initialize_tcp(slave,sdfs_m,logging):
    tcpserver = TCPServer(slave, sdfs_m, logging)
    return tcpserver

def initialize_sdfs_m():
    sdfs_master = SDFS_Master({}, [], {})
    return sdfs_master

def remake_cache():
    for i in range(5):
        os.system("mkdir /home/zli104/sdfs/cache/"+ str(i))

def initialize_packets():
    sdfs_master = initialize_sdfs_m()

    slave = initialize_slave(logging, sdfs_master)

    udpserver = initialize_udp(slave)
    cli = initialize_cli(slave, logging)
    tcpserver = initialize_tcp(slave, sdfs_master, logging)

    return sdfs_master,slave,udpserver,cli,tcpserver


if __name__ == '__main__':
    logging.basicConfig(filename='mp3.log',level=logging.INFO, filemode='w')
    os.system("rm -rf /home/zli104/sdfs/*")
    os.system("mkdir /home/zli104/sdfs/cache")
    remake_cache()
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)

    sdfs_master,slave,udpserver,cli,tcpserver = initialize_packets()


    udpserver.run_server()
    slave.run()
    cli.run()
    slave.init_join()
    run_tcp_server(tcpserver)






