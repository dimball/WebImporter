import threading
import socketserver
import json
import common as hfn
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

import dataclasses
import logging
import time
import client as cli


logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler,hfn.c_HelperFunctions):

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
    def m_setprogress(self,ID,progress):
        logging.debug("Setting progress for:%s:%s",ID, progress)
        Tasks.Jobs[ID].progress = progress
        self.m_NotifyClients("/webimporter/v1/global/queue/put", self.m_SerialiseSyncTasks(Tasks))
    def m_NotifyClients(self,command, payload):
        for cl in Tasks.clientlist:
            #if cl["ip"] == self.client_address[0]:
            self.Client = cli.Client(cl["ip"],cl["port"],Tasks)
            logging.debug("Sending tasks to:%s", self.client_address[0])
            self.Client.m_send(self.Client.m_create_data(command, payload))

    # def setup(self):
    def handle(self):
        #print("handling request")
        self.Client = cli.Client("localhost",9999, Tasks) #dummy client
        self.data = self.Client.m_receive_all(self.request)
        #self.data = self.request.recv(self.buffersize).decode('utf-8')
        self.data = json.loads(self.data)
        self.Command = self.data["command"]
        self.Payload = self.data["payload"]
        self.Output = {}

        if self.Command == "/syncserver/v1/global/queue/task/put":
            ############################ PUT ON TASKLIST ############################
            #when a new task is put into the global task list, then it needs to notify this to all registered clients (except for the one that sent the
            #request in the first place.

            logging.debug("number of global tasks:%s", len(Tasks.Jobs))
            self.Payload = json.loads(self.Payload)
            if len(self.Payload)>0:
                for data in self.Payload:
                    if not self.m_Is_ID_In_List(Tasks.Order,data["ID"]):


                        Tasks.Order.append(data["ID"])
                        Tasks.Jobs[data["ID"]] = dataclasses.c_Task(data["ID"])
                        Tasks.Jobs[data["ID"]].progress = data["Data"]["progress"]
                        Tasks.Jobs[data["ID"]].metadata = data["Data"]["metadata"]
                        for file in data["Data"]["filelist"]:
                            Tasks.Jobs[data["ID"]].filelist[file] = dataclasses.c_file(data["Data"]["filelist"][file]["size"])
                            Tasks.Jobs[data["ID"]].filelist[file].progress = data["Data"]["filelist"][file]["progress"]
                            Tasks.Jobs[data["ID"]].filelist[file].copied = data["Data"]["filelist"][file]["copied"]
                            Tasks.Jobs[data["ID"]].filelist[file].delete = data["Data"]["filelist"][file]["delete"]
                            Tasks.Jobs[data["ID"]].filelist[file].uploaded = data["Data"]["filelist"][file]["uploaded"]

                        Tasks.Jobs[data["ID"]].filelistOrder = data["Data"]["filelistOrder"]
                        logging.debug("Adding a task to the global list from:%s", self.client_address[0])

            #only send the job you have received to the clients

                    #self.m_NotifyClients("/webimporter/v1/global/queue/put",self.m_SerializeTask(Tasks.Jobs[data["ID"]]))
        elif self.Command == "/syncserver/v1/global/queue/task/put_no_reply":
            logging.debug("number of global tasks:%s", len(Tasks.Jobs))
            self.Payload = json.loads(self.Payload)
            if len(self.Payload)>0:
                for data in self.Payload:
                    if not self.m_Is_ID_In_List(Tasks.Order,data["ID"]):


                        Tasks.Order.append(data["ID"])
                        Tasks.Jobs[data["ID"]] = dataclasses.c_Task(data["ID"])
                        Tasks.Jobs[data["ID"]].progress = data["Data"]["progress"]
                        Tasks.Jobs[data["ID"]].metadata = data["Data"]["metadata"]
                        for file in data["Data"]["filelist"]:
                            Tasks.Jobs[data["ID"]].filelist[file] = dataclasses.c_file(data["Data"]["filelist"][file]["size"])
                            Tasks.Jobs[data["ID"]].filelist[file].progress = data["Data"]["filelist"][file]["progress"]
                            Tasks.Jobs[data["ID"]].filelist[file].copied = data["Data"]["filelist"][file]["copied"]
                            Tasks.Jobs[data["ID"]].filelist[file].delete = data["Data"]["filelist"][file]["delete"]
                            Tasks.Jobs[data["ID"]].filelist[file].uploaded = data["Data"]["filelist"][file]["uploaded"]

                        Tasks.Jobs[data["ID"]].filelistOrder = data["Data"]["filelistOrder"]
                        logging.debug("Adding a task to the global list from:%s", self.client_address[0])
        elif self.Command == "/syncserver/v1/global/queue/task/get_IDs":
            self.output = []
            for ID in Tasks.Jobs:
                self.output.append(ID)
            logging.debug(self.output)
            self.m_reply(json.dumps(self.output),self.request)

        elif self.Command == "/syncserver/v1/global/queue/task/request":
            ############################ GET TASK  ############################
            logging.debug("Sending requested Task IDs to client:%s", self.client_address[0])

            self.m_reply(self.m_SerialiseTaskList(self.Payload, False),self.request)
        elif self.Command == "/syncserver/v1/global/queue/task/get":
            ############################ GET TASK  ############################
            logging.debug("Sending tasks to client:%s", self.client_address[0])
            self.m_reply(self.m_SerialiseSyncTasks(Tasks, False),self.request)
        elif self.Command == "/syncserver/v1/global/queue/task/set_progress":
            ############################ SET PROGRESS ############################
            self.ID = self.Payload["ID"]
            self.progress = self.Payload["progress"]
            self.m_setprogress(self.ID,self.progress)
        elif self.Command == "/syncserver/v1/global/queue/set_priority":
            ############################ SET PRIORITY ############################
            logging.debug("Set priority list")
            self.data = self.Payload
            Tasks.Order = []
            for Data in self.data:
                Tasks.Order.append(Data)

            self.m_NotifyClients("/webimporter/v1/local/queue/set_priority", Tasks.Order)

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
    # def finish(self):
    #     print("cleaning up request")

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    pass

class server(hfn.c_HelperFunctions):
    def __init__(self):
        global Tasks
        Tasks = dataclasses.c_SyncServerData()
    def run(self):
        # Port 0 means to select an arbitrary unused port
        HOST, PORT = "localhost",  8908
        server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
        ip, port = server.server_address
        # Start a thread with the server -- that thread will then start one
        # more thread for each request
        server_thread = threading.Thread(target=server.serve_forever)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()
        server_thread.name = "Sync_Server"
        logging.debug("Sync Server loop running in thread:%s", server_thread.name)

        while not Tasks.shutdown:
             time.sleep(1)
             continue

        server.shutdown()
        logging.debug("Sync Server is shutdown")

serverThread = server()
serverThread.run()

