import common as hfn
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
import websocket
import threading
# class simple_connection(hfn.c_HelperFunctions):
#     def __init__(self, Tasks, type):
#         self.connected = False
#         self.Tasks = Tasks
#         self.ip = self.Tasks.WorkData["syncserver_ip"]
#         self.port = self.Tasks.WorkData["syncserver_port"]
#         self.connection = None
#         self.type = type
#     def m_connect(self):
#         try:
#             logging.debug("[" + self.type + "] Establishing a websocket with server at %s:%s", self.ip, self.port)
#             #self.m_send(self.m_create_data("/syncserver/v1/server/register",self.Tasks.WorkData["local_serverport"]))
#             self.connection = websocket.create_connection("ws://" + self.ip + ":" + self.port + "/" + self.type)
#             self.connected = True
#         except:
#             logging.debug("Server at %s:%s is not reachable via websockets",self.ip, self.port)
#             self.connected = False
#     def m_send(self, payload, bDebug=False):
#         self.m_connect()
#         if bDebug:
#             logging.debug("sending data => %s", len(payload))
#         self.connection.send(payload)
#         self.connection.close
#
#     def m_request(self, payload, bDebug=True):
#         self.m_connect()
#         if bDebug:
#             logging.debug("sending data => %s", payload)
#         self.connection.send(payload)
#         self.response = self.connection.recv()
#         self.connection.close()
#         return self.response
class threaded_Websocket_Client(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self,ip, port, Tasks, sType, handler):
        threading.Thread.__init__(self)
        self.connected = False
        self.ip = ip
        self.port = port
        self.Tasks = Tasks
        self.sType = sType
        self.connection = None
        self.name = sType
        self.on_message = handler.on_message
        self.on_close = handler.on_close
        self.m_connect()
        self.start()
    def m_connect(self, bConsole=True):
        try:
            if bConsole:
                logging.debug("[" + self.sType + "] Attempting to register on sync server at %s:%s", self.ip, self.port)
            self.connection = websocket.create_connection("ws://" + self.ip + ":" + self.port + "/" + self.sType)
            self.connected = True
        except:
            if bConsole:
                logging.debug("Sync server at %s:%s is not reachable. Disabling syncserver support for this session",self.ip, self.port)
            self.connected = False

        if self.connected:
            logging.debug("Connected to sync server at: %s:%s",self.ip, self.port)


    def run(self):
        while True:
            while self.connected:
                if self.connection != None:
                    try:
                        self.on_message(self.connection, self.connection.recv())
                    except:
                        #try to reconnect
                        self.connected = False
                        self.on_close()
                        #should timeout?? or just run indefinitely

    def m_send(self, payload,bDebug=True):
        self.connection.send(payload)





