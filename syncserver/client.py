import common as hfn
import json
import socket
class Client(hfn.c_HelperFunctions):
    def __init__(self, ip, port, Tasks):
        self.ip = ip
        self.port = port
        self.Tasks = Tasks

    # def m_create_data(self, command, payload=0):
    #     self.data = {}
    #     self.data["command"] = command
    #     self.data["payload"] = payload
    #     return json.dumps(self.data)

    def m_send(self, payload):
        # SOCK_STREAM == a TCP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.setblocking(0)  # optional non-blocking
        self.sock.connect((self.ip, int(self.port)))
        #logging.debug("sending data => %s", (payload))
        SizeOfData = len(payload)
        self.payload = str(SizeOfData) + "|" + payload
        self.sock.sendall(bytes(self.payload, 'utf8'))

        #sock.setblocking(0)
        # self.ready = select.select([self.sock],[],[],2)
        # if self.ready[0]:

        self.reply = self.m_receive_all(self.sock)
        if len(self.reply)>0:
            return self.reply
        # else:
        #     print("request timed out")

        if self.sock != None:
            self.sock.close()
        #return reply