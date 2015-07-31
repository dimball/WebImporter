import common as hfn
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
import json
import socket
import websocket
import threading
# class Client(hfn.c_HelperFunctions):
#     def __init__(self,ip, port, Tasks):
#         self.connected = False
#         self.ip = ip
#         self.port = port
#         self.Tasks = Tasks
#         try:
#             logging.debug("Attempting to register on sync server at %s:%s",self.Tasks.WorkData["syncserver_ip"],self.Tasks.WorkData["syncserver_port"])
#             self.m_send(self.m_create_data("/syncserver/v1/server/register",self.Tasks.WorkData["local_serverport"]))
#             self.connected = True
#         except:
#             logging.debug("Sync server at %s:%s is not reachable. Disabling syncserver support for this session",self.Tasks.WorkData["syncserver_ip"],self.Tasks.WorkData["syncserver_port"])
#             self.connected = False
#         if self.connected == True:
#             logging.debug("Connected to sync server at: %s:%s",self.Tasks.WorkData["syncserver_ip"],self.Tasks.WorkData["syncserver_port"])
#     def m_IsConnected(self):
#         try:
#             self.response = self.m_send(self.m_create_data("/syncserver/v1/server/isconnected"))
#             self.response = json.loads(self.response)
#             if self.response["status"] == "OK":
#                 self.connected = True
#                 return True
#             else:
#                 self.connected = False
#                 return False
#         except:
#             self.connected = False
#             return False
#     def m_create_data(self, command, payload=0):
#         self.data = {}
#         self.data["command"] = command
#         self.data["payload"] = payload
#         return json.dumps(self.data)
#     def m_send(self, payload, bDebug=False):
#         # SOCK_STREAM == a TCP socket
#         self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         #self.sock.setblocking(0)  # optional non-blocking
#         self.sock.connect((self.ip, int(self.port)))
#
#         if bDebug:
#             logging.debug("sending data => %s", len(payload))
#
#         self.SizeOfData = len(payload)
#         self.payload = str(self.SizeOfData) + "|" + payload
#         self.sock.sendall(bytes(self.payload, 'utf8'))
#         self.reply = self.m_receive_all(self.sock)
#         if len(self.reply)>0:
#             return json.loads(self.reply)
#
#         if self.sock != None:
#             self.sock.close()
class simple_connection(hfn.c_HelperFunctions):
    def __init__(self, Tasks, type):
        self.connected = False
        self.Tasks = Tasks
        self.ip = self.Tasks.WorkData["syncserver_ip"]
        self.port = self.Tasks.WorkData["syncserver_port"]
        self.connection = None
        self.type = type
    def m_connect(self):
        try:
            logging.debug("[" + self.type + "] Establishing a websocket with server at %s:%s", self.ip, self.port)
            #self.m_send(self.m_create_data("/syncserver/v1/server/register",self.Tasks.WorkData["local_serverport"]))
            self.connection = websocket.create_connection("ws://" + self.ip + ":" + self.port + "/" + self.type)
            self.connected = True
        except:
            logging.debug("Server at %s:%s is not reachable via websockets",self.ip, self.port)
            self.connected = False
    def m_send(self, payload, bDebug=False):
        self.m_connect()
        if bDebug:
            logging.debug("sending data => %s", len(payload))
        self.connection.send(payload)
        self.connection.close

    def m_request(self, payload, bDebug=True):
        self.m_connect()
        if bDebug:
            logging.debug("sending data => %s", payload)
        self.connection.send(payload)
        self.response = self.connection.recv()
        self.connection.close()
        return self.response
class threaded_Websocket_Client(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self,ip, port, Tasks, sType, handler):
        threading.Thread.__init__(self)
        self.connected = False
        self.ip = ip
        self.port = port
        self.Tasks = Tasks
        self.connection = None
        self.name = sType
        self.on_message = handler.on_message

        try:
            logging.debug("[" + sType + "] Attempting to register on sync server at %s:%s", self.ip, self.port)
            #self.m_send(self.m_create_data("/syncserver/v1/server/register",self.Tasks.WorkData["local_serverport"]))
            self.connection = websocket.create_connection("ws://" + self.ip + ":" + self.port + "/" + sType)
            self.connected = True
        except:
            logging.debug("Sync server at %s:%s is not reachable. Disabling syncserver support for this session",self.ip, self.port)
            self.connected = False
        if self.connected:
            logging.debug("Connected to sync server at: %s:%s",self.ip, self.port)
        self.start()
    def run(self):
        while True:
            if self.connection != None:
                self.on_message(self.connection, self.connection.recv())
                #logging.debug("received data from [%s} %s", self.name, self.connection.recv())

    def m_send(self, payload,bDebug=True):
        self.connection.send(payload)





