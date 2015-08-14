import common as hfn
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
import websocket
import threading

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
            while not self.connected:
                ###trying to re-connect indefinitely
                try:
                    self.m_connect(False)
                except:
                    continue
    def m_send(self, payload,bDebug=True):
        self.connection.send(payload)





