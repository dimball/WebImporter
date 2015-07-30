import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.websocket
import tornado.gen
from tornado.options import define, options

import json
import common as hfn
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

import logging

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )


import dataclasses

define("port", default=8908, help="run on the given port", type=int)
 
Progress_Clients = []
Command_Clients = []
import json

class ProgressHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        print('new connection')
        Progress_Clients.append(self)
        self.write_message("connected")
 
    def on_message(self, message):
        print('Progress data: %s', json.loads(message))
        self.write_message('got it!')
 
    def on_close(self):
        print('connection closed')
        Progress_Clients.remove(self)

class CommandHandler(tornado.websocket.WebSocketHandler, hfn.c_HelperFunctions):
    def m_add_task_to_list(self,ID,Payload):
        #adds a Task from a client into the global list.
        #Task only gets the data on progress, ID and metadata
        Tasks.Jobs[ID] = Payload
    def m_send_tasklist(self):
        #sends the whole task list to client
        self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
    def m_setpriority(self,prioritylist):
        #reorders the task list from incoming priority list array and then sets a flag to show that the list has changed and update is needed
        #prioritylist is an array of ID's
        Tasks.Order = prioritylist
    def m_getpriority(self):
        logging.debug("Sending task priority order")
        #return the order of the tasks in the global list
        self.Output = []
        for ID in Tasks.Order:
            self.TaskData = {}
            self.TaskData["ID"] = ID
            self.Output.append(self.TaskData)

        self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
    def m_setprogress(self,ID,progress, client_address):
        if ID in Tasks.Jobs:
            if progress > Tasks.Jobs[ID].progress:
                #logging.debug("Setting progress for:%s:%s", ID, progress)
                Tasks.Jobs[ID].progress = progress
                ### needs to only notify clients of the progress.
                self.SendData = {}
                self.SendData["ID"] = ID
                self.SendData["progress"] = progress
                self.m_NotifyClients("/webimporter/v1/global/queue/task/set_progress", self.SendData, client_address)
    def m_NotifyClients(self,command, payload):
        for client in Command_Clients:
            logging.debug("Sending tasks to:%s", client)
            client.write_message(self.client.m_create_data(command, payload))


    def open(self):
        logging.debug('new command connection from %s', self)
        Command_Clients.append(self)
        self.write_message("connected")

    def on_message(self, message):
        self.data = json.loads(message)
        logging.debug(self.data["command"])
        logging.debug(len(self.data["payload"]["TaskList"]))
        self.Command = self.data["command"]
        self.Payload = self.data["payload"]
        self.Output = {}

        if self.Command == "/syncserver/v1/global/queue/task/put":
            ############################ PUT ON TASKLIST ############################
            #when a new task is put into the global task list, then it needs to notify this to all registered clients (except for the one that sent the
            #request in the first place.

            logging.debug("number of global tasks:%s", len(Tasks.Jobs))
            self.m_deSerializeTaskList(self.Payload, Tasks)
            self.IncomingTasks = []
            if len(self.Payload["TaskList"])>0:
                for data in self.Payload["TaskList"]:
                    self.IncomingTasks.append(Tasks.Jobs[data["ID"]])
                    logging.debug("Adding a task to the global list from:")

                    # if not self.m_Is_ID_In_List(Tasks.Order,data["ID"]):

            #only send the job you have received to other clients other than the one who sent this in the first place
            self.m_NotifyClients("/webimporter/v1/global/queue/put",self.m_SerialiseTaskList(self.IncomingTasks, Tasks),Command_Clients)
        elif self.Command == "/syncserver/v1/global/queue/task/put_no_reply":
            logging.debug("number of global tasks:%s", len(Tasks.Jobs))
            logging.debug("Adding %s tasks to the global list from:%s", len(self.Payload["TaskList"]), self.client_address[0])
            self.m_deSerializeTaskList(self.Payload, Tasks)

        elif self.Command == "/syncserver/v1/global/queue/task/get_IDs":
            self.output = []
            for ID in Tasks.Jobs:
                self.output.append(ID)
            logging.debug(self.output)
            self.m_reply(json.dumps(self.output),self.request)

        elif self.Command == "/syncserver/v1/global/queue/task/request":
            ############################ GET TASK  ############################
            logging.debug("Sending requested Task IDs to client:%s", self.client_address[0])
            self.SendJobs = []
            for ID in self.Payload:
                self.SendJobs.append(Tasks.Jobs[ID])

            self.m_reply(self.m_SerialiseTaskList(self.SendJobs, Tasks, False), self.request)
        elif self.Command == "/syncserver/v1/global/queue/task/get":
            ############################ GET TASK  ############################
            logging.debug("Sending tasks to client:%s", self.client_address[0])
            self.m_reply(self.m_SerialiseSyncTasks(Tasks, False),self.request)
        elif self.Command == "/syncserver/v1/global/queue/task/set_progress":
            ############################ SET PROGRESS ############################

            #notifies clients of the progress too
            self.m_setprogress(self.Payload["ID"],self.Payload["progress"], self.client_address[0])


        elif self.Command == "/syncserver/v1/global/queue/set_priority":
            ############################ SET PRIORITY ############################
            logging.debug("Set priority list")
            self.data = self.Payload
            Tasks.Order = []
            for Data in self.data:
                Tasks.Order.append(Data)

            self.m_NotifyClients("/webimporter/v1/local/queue/set_priority", Tasks.Order, self.client_address[0])

        elif self.Command == "/syncserver/v1/global/queue/get_priority":
            ############################ GET PRIORITY ############################
            self.m_getpriority()
        elif self.Command == "/syncserver/v1/server/register":
            ############################ REGISTER ############################
            self.client = {}
            self.client["ip"] = self.client_address[0]
            self.client["port"] = int(self.Payload)
            self.bFoundClient = False
            for cl in Tasks.clientlist:
                if cl["ip"] == self.client_address[0] and cl["port"] == int(self.Payload):
                    self.bFoundClient = True

            if self.bFoundClient == False:
                logging.debug("Registering client:%s:%s", self.client_address[0],self.Payload)
                Tasks.clientlist.append(self.client)
            else:
                logging.debug("[%s:%s]Client already exists", self.client_address[0],self.Payload)
        elif self.Command == "/syncserver/v1/server/isconnected":
            ############################ IS CONNECTED ############################
            self.Output = {}
            self.Output["status"] = "OK"
            self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
        elif self.Command == "/syncserver/v1/server/shutdown":
            ############################ SHUTDOWN ############################
            logging.debug("Shutting down server")
            #go to each line manager and ask it to shut down
            Tasks.shutdown = True






        self.write_message('got it!')

    def on_close(self):
        print('connection closed')
        Command_Clients.remove(self)


class SyncServer(hfn.c_HelperFunctions):
    def __init__(self):
        global Tasks
        Tasks = dataclasses.c_SyncServerData()
    def run(self):
        # Port 0 means to select an arbitrary unused port
        HOST, PORT = "localhost",  8908
        self.app = tornado.web.Application(
            handlers=[
                (r"/progress", ProgressHandler),
                (r"/command", CommandHandler)

            ]
        )
        self.httpServer = tornado.httpserver.HTTPServer(self.app)
        self.httpServer.listen(PORT)
        logging.debug("Sync Server loop running on port:%s", PORT)
        self.mainLoop = tornado.ioloop.IOLoop.instance()
        self.mainLoop.start()



        # while not Tasks.shutdown:
        #      time.sleep(1)
        #      continue
        # logging.debug("Sync Server is shutdown")
if __name__ == '__main__':
   Server = SyncServer()
   Server.run()