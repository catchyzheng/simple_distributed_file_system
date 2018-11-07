from socket import *
from xmlrpc.server import *
import time
from operator import itemgetter
import random
import base64
from threading import Thread
import logging


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


INTRODUCER = 'fa18-cs425-g73-01.cs.illinois.edu'
tRejoin = 12.0
member_list = []
neighbors = []
recent_removed = []
sleep_time = 1.0
timeout_time = 1.0
false_positive_count = 0
UDP_LOST_RATE = 0
isOnline = False
tFail = 2.0



def run_tcp_server():
    # Create server
    with SimpleXMLRPCServer(("0.0.0.0", 8000),
                            requestHandler=RequestHandler) as server:
        server.register_introspection_functions()
        server.serve_forever()


def ask_for_join(joiner_ip):
    global member_list
    # member_list stores a list of (process_ip,0,time)!!!

    for member in member_list:
        if joiner_ip == member[0]:
            return True
    logging.info("Ask for join Time[{}]: {} is joining.".format(time.time(), joiner_ip))
    member_list.append((joiner_ip,0,time.time()))
    member_list = sorted(member_list, key=itemgetter(0))
    clientSocket = makeNewSocket(timeout_time)
    update_neighbors()
    for member in member_list:
        clientSocket.sendto(base64.b64encode(str(member_list).encode('utf-8')), (gethostbyname(member[0]), 9000))
    return True


def detect_failure():
    global member_list
    global recent_removed
    global false_positive_count
    cur_time = time.time()
    tmp_member_list = []
    for member in member_list:
        if member[2] >= cur_time - tFail:
            tmp_member_list.append(member)
            continue
        recent_removed.append(member)
        false_positive_count += 1
        logging.info(
            "Time[{}]: {} has gone offline, current member_list: {}".format(time.time(), member[0], member_list))
    member_list = tmp_member_list
    update_neighbors()


def getIndex(key):
    global member_list
    return [m[0] for m in member_list].index(key)

def update_neighbors():
    global neighbors
    global member_list
    index = getIndex(getfqdn())

    ml_len = len(member_list)
    temp_set = set()
    temp_set.add( (index-2) % ml_len)
    temp_set.add((index - 1) % ml_len)
    temp_set.add((index + 1) % ml_len)
    temp_set.add((index + 2) % ml_len)

    neighbors = list(temp_set)


def merge_member_list(remote_member_list):
    global member_list
    j = 0
    cur_time = time.time()
    for i in range(len(remote_member_list)):
        current_remote_member = remote_member_list[i]
        current_remote_member_ip = current_remote_member[0]
        while j < len(member_list) and member_list[j][0] < current_remote_member_ip:
            j += 1
        if j < len(member_list) and current_remote_member_ip == member_list[j][0]:
            if current_remote_member[1] > member_list[j][1]:
                member_list[j] = (member_list[j][0], current_remote_member[1], cur_time)
            j += 1
        else:
            tmpList = [m[0] for m in recent_removed]
            domain_name = current_remote_member[0]
            if domain_name not in tmpList or recent_removed[tmpList.index(domain_name)][1] < current_remote_member[1]:
                member_list.append(current_remote_member)
                if domain_name not in tmpList:
                    logging.info(" ( Merge Member List) Time[{}]: {} is joining.".format(time.time(), domain_name))

    member_list = sorted(member_list, key=itemgetter(0))
    update_neighbors()



def run_udp_server():
    global isOnline
    serverSocket = socket(AF_INET, SOCK_DGRAM)
    serverSocket.bind(('0.0.0.0', 9000))

    while True:
        message, address = serverSocket.recvfrom(65565)
        remote_member_list = eval(base64.b64decode(message).decode("utf-8"))
        command = remote_member_list[0][0]
        if command == 'join':
            ask_for_join(remote_member_list[0][1])
            continue
        elif command == 'leave':
            handle_leave_request(remote_member_list[0][1])
            continue
        elif command == 'ping':
            reply_ping(remote_member_list[0][1])
            continue
        elif command == 'ack':
            update_ping_ack_count(remote_member_list[0][1])
            merge_member_list(remote_member_list[1])
            continue
        if isOnline: merge_member_list(remote_member_list)


def reply_ping(ack_receiver):
    global member_list
    clientSocket = socket(AF_INET, SOCK_DGRAM)
    clientSocket.settimeout(1)
    large_list=[]
    large_list.append(('ack',getfqdn()))
    large_list.append(member_list)
    msg = base64.b64encode(str(large_list).encode('utf-8'))
    addr = (gethostbyname(ack_receiver), 9000)
    clientSocket.sendto(msg,addr)


def handle_leave_request(ip):
    global member_list

    index = getIndex(ip)

    recent_removed.append(member_list[index])
    member_list.pop(index)
    logging.info("Time[{}]: {} voluntarily left the group, current member_list: {}".format(time.time(), ip, member_list))
    update_neighbors()


def update_member(index):
    global member_list
    tmp = (member_list[index][0],member_list[index][1]+1,time.time())
    member_list[index] = tmp


def update_ping_ack_count(ack_sender):
    global member_list

    index = -1
    for i in range(len(member_list)):
        if member_list[i][0] == ack_sender:
            index = i
            break

    update_member(index)
    detect_failure()
    clean_removed_list()


def clean_removed_list():
    global recent_removed
    cur_time = time.time()
    for member in recent_removed:
        if member[2] >= cur_time - tRejoin:
            continue
        recent_removed.remove(member)



def update_member_time(index):
    global member_list
    tmp = (member_list[index][0],member_list[index][1],time.time())
    member_list[index] = tmp


def send_ack():
    clientSocket = makeNewSocket(timeout_time)
    global isOnline
    global member_list          # shall we add this line?
    global neighbors            # shall we add this line as well?
    global sleep_time
    while True:
        time.sleep(sleep_time)
        if  not isOnline:
            continue
        idx = [m[0] for m in member_list].index(getfqdn())
        update_member_time(idx)
        detect_failure()
        clean_removed_list()
        has_sent = {}
        for index in neighbors:
            curIP = member_list[index][0]
            if random.randint(0, 99) >= UDP_LOST_RATE and curIP not in has_sent:
                sendMessage(curIP,clientSocket,'ping')
                has_sent[curIP] = 1



def sendMessage(hostIP,socket,message):
    socket.sendto(base64.b64encode(str([(message, getfqdn())]).encode('utf-8')), (gethostbyname(hostIP), 9000))


def makeNewSocket(time):
    clientSocket = socket(AF_INET,SOCK_DGRAM)
    clientSocket.settimeout(time)
    return clientSocket

def leave():
    global isOnline
    global member_list
    global neighbors
    isOnline = False
    clientSocket = makeNewSocket(timeout_time)
    member_list_temp = [(m[0],0,time.time()) for m in member_list]
    for member in member_list:
        if member[0] != getfqdn():
            sendMessage(member[0],clientSocket,'leave')
    member_list = member_list_temp



def join():
    global isOnline
    global member_list
    global sleep_time
    clientSocket = makeNewSocket(timeout_time)
    isOnline = True
    sendMessage(INTRODUCER,clientSocket,'join')
    time.sleep(sleep_time)


def commandLine():
    global member_list
    while True:
        command = input('Enter your command(lsm, lsid, join or leave): ')
        if command == 'lsid':
            logging.info('Time[{}]: {}'.format(time.time(), getfqdn()))
            continue
        if command == 'lsm':
            logging.info("Time[{}]: {}".format(time.time(), member_list))
            continue
        if command == 'join':
            join()
            continue
        if command == 'leave':
            leave()
            continue
        print("COMMAND IS NOT AVAILABLE!")


def startNewThread(targetFunc):
    a = Thread(target=targetFunc)
    a.start()
    return a

if __name__ == '__main__':
    logging.basicConfig(filename='mp2.log', level=logging.INFO, filemode='w')
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)
    udp_thread = startNewThread(run_udp_server)
    tcp_thread = startNewThread(run_tcp_server)
    ping_ack_thread = startNewThread(send_ack)
    cmdLineThread = startNewThread(commandLine)
    join()
    udp_thread.join()
    tcp_thread.join()
    ping_ack_thread.join()
    cmdLineThread.join()
