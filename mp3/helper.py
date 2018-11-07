import base64
import xmlrpc.client


def get_tcp_client_handle(ip):
    return xmlrpc.client.ServerProxy('http://' + ip + ':8000')