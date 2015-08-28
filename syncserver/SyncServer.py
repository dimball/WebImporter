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
import json
class ProgressHandler(tornado.websocket.WebSocketHandler, hfn.c_HelperFunctions):
    ### Handles progress data sent from the webimporterServer client. This is then used to notify all clients connected
    ### to this progress handler in the Tasks.ProgressClients list

    def check_origin(self, origin):
        self.m_StoreClient(origin, Tasks, "progress", False)
        return True
    def m_setTaskProgress(self,ID,progress):
        if ID in Tasks.Jobs:
            if progress > Tasks.Jobs[ID].progress:
                #logging.debug("Setting progress for:%s:%s", ID, progress)
                Tasks.Jobs[ID].progress = progress
                ### needs to only notify clients of the progress.
                self.SendData = {}
                self.SendData["ID"] = ID
                self.SendData["progress"] = progress
                self.m_NotifyClients("/webimporter/v1/global/queue/task/set_progress", self.SendData, Tasks.ProgressClients, Tasks)

    def m_setFileProgress(self,ID, file, progress):
        if ID in Tasks.Jobs:
            if progress > Tasks.Jobs[ID].filelist[file].progress:
                #logging.debug("Setting progress for:%s:%s", ID, progress)
                Tasks.Jobs[ID].filelist[file].progress = progress
                ### needs to only notify clients of the progress.
                self.SendData = {}
                self.SendData["ID"] = ID
                self.SendData["progress"] = progress
                self.SendData["file"] = file
                self.m_NotifyClients("/webimporter/v1/global/queue/task/file/set_progress", self.SendData, Tasks.ProgressClients, Tasks)
    def m_setTranscodeProgress(self,ID, file, progress):
        if ID in Tasks.Jobs:
            if progress > Tasks.Jobs[ID].filelist[file].transcodeProgress:
                #logging.debug("Setting progress for:%s:%s", ID, progress)
                Tasks.Jobs[ID].filelist[file].transcodeProgress = progress
                ### needs to only notify clients of the progress.
                self.SendData = {}
                self.SendData["ID"] = ID
                self.SendData["progress"] = progress
                self.SendData["file"] = file
                self.m_NotifyClients("/webimporter/v1/global/queue/task/file/set_progress", self.SendData, Tasks.ProgressClients, Tasks)

    def open(self):
        logging.debug('Syncserver new progress connection')
        Tasks.ProgressClients.append(self)
        self.write_message(self.m_create_data("/webimporter/v1/connection/connected", 1))
    def on_message(self, data):
        self.Data = json.loads(data)
        self.Command = self.Data["command"]
        self.Payload = self.Data["payload"]
        if self.Command == "/syncserver/v1/global/queue/task/set_progress":
            logging.debug('Progress data: %s', self.Payload)
            self.m_setTaskProgress(self.Payload["ID"], self.Payload["progress"])
        elif self.Command == "/syncserver/v1/global/queue/task/file/set_progress":
            logging.debug('Progress data: %s', self.Payload)
            self.m_setFileProgress(self.Payload["ID"], self.Payload["file"], self.Payload["progress"])
        elif self.Command == "/syncserver/v1/global/queue/task/file/transcode/set_progress":
            logging.debug('Progress data: %s', self.Payload)
            self.m_setTranscodeProgress(self.Payload["ID"], self.Payload["file"], self.Payload["progress"])

    def on_close(self):
        logging.debug('Progress socket connection closed %s', Tasks.GetClientNameFromHandler(self))
        Tasks.ProgressClients.remove(self)
        Tasks.RemoveHandlerFromClientList(self)

class CommandHandler(tornado.websocket.WebSocketHandler, hfn.c_HelperFunctions):
    ###Handles all commands coming in from the webimporterServer client. Anything that needs to be communicated to
    ### connected clients will be broadcasted using the connected Command clients in the Tasks.CommandClients list
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
    def check_origin(self, origin):
        self.m_StoreClient(origin, Tasks, "command")
        return True
    def open(self):
        logging.debug('new command connection from %s', Tasks.GetClientNameFromHandler(self))
        Tasks.CommandClients.append(self)
        #self.write_message(self.m_create_data("/webimporter/v1/connection/connected", 1))
    def on_message(self, message):
        self.data = json.loads(message)
        #logging.debug(self.data["command"])
        #logging.debug(len(self.data["payload"]["TaskList"]))
        self.Command = self.data["command"]
        self.Payload = self.data["payload"]
        self.Output = {}

        if self.Command == "/syncserver/v1/global/queue/task/put":
            ############################ PUT ON TASKLIST ############################
            #when a new task is put into the global task list, then it needs to notify this to all registered clients (except for the one that sent the
            #request in the first place.

            logging.debug("number of global tasks:%s", len(Tasks.Jobs))
            self.m_deSerializeTaskList(self.Payload, Tasks, self)
            self.IncomingTasks = []
            if len(self.Payload["TaskList"])>0:
                for data in self.Payload["TaskList"]:
                    self.IncomingTasks.append(Tasks.Jobs[data["ID"]])
                    logging.debug("Adding a task to the global list from:%s", Tasks.GetClientNameFromHandler(self))
                    # if not self.m_Is_ID_In_List(Tasks.Order,data["ID"]):

            #only send the job you have received to other clients other than the one who sent this in the first place
            self.m_NotifyClients("/webimporter/v1/global/queue/put",self.m_SerialiseTaskList(self.IncomingTasks, Tasks), Tasks.CommandClients, Tasks)
        elif self.Command == "/syncserver/v1/global/queue/task/put_no_reply":
            logging.debug("number of global tasks:%s", len(Tasks.Jobs))
            logging.debug("Adding %s tasks to the global list from: %s", len(self.Payload["TaskList"]), Tasks.GetClientNameFromHandler(self))
            self.m_deSerializeTaskList(self.Payload, Tasks, self)

        elif self.Command == "/syncserver/v1/global/queue/task/get_IDs":
            self.output = []
            for ID in Tasks.Jobs:
                self.output.append(ID)
            #logging.debug(json.dumps(self.output))
            self.write_message(self.m_create_data("/syncserver/v1/global/register", self.output))
            #self.write_message(json.dumps(self.output).encode("utf-8"))

        elif self.Command == "/syncserver/v1/global/queue/task/request":
            ############################ GET TASK  ############################
            logging.debug("Sending requested Task IDs to client:%s", Tasks.GetClientNameFromHandler(self))
            self.SendJobs = []
            for ID in self.Payload:
                self.SendJobs.append(Tasks.Jobs[ID])

            self.m_reply(self.m_create_data("/webimporter/v1/global/queue/put", self.m_SerialiseTaskList(self.SendJobs, Tasks, False) ), self)
        elif self.Command == "/syncserver/v1/global/queue/task/get":
            ############################ GET TASK  ############################
            logging.debug("Sending tasks to client:%s", Tasks.GetClientNameFromHandler(self))
            self.m_reply(self.m_SerialiseSyncTasks(Tasks, False), self)
        elif self.Command == "/syncserver/v1/global/queue/task/set_progress":
            ############################ SET PROGRESS ############################

            #notifies clients of the progress too
            self.m_setprogress(self.Payload["ID"],self.Payload["progress"])


        elif self.Command == "/syncserver/v1/global/queue/set_priority":
            ############################ SET PRIORITY ############################
            logging.debug("Set priority list")
            self.data = self.Payload
            Tasks.Order = []
            for Data in self.data:
                Tasks.Order.append(Data)

            self.m_NotifyClients("/webimporter/v1/local/queue/set_priority", Tasks.Order, Tasks.CommandClients, Tasks)

        elif self.Command == "/syncserver/v1/global/queue/get_priority":
            ############################ GET PRIORITY ############################
            self.m_getpriority()
        elif self.Command == "/syncserver/v1/server/register":
            ############################ REGISTER ############################
            self.m_reply(self.m_SerialiseSyncTasks(Tasks, False), self)

        elif self.Command == "/syncserver/v1/global/upload/queue/task/file/uploaded":
            ############################ Set uploaded flag ############################
            Tasks.Jobs[self.Payload["ID"]].filelist[self.Payload["file"]].uploaded = self.Payload["uploaded"]
            self.m_NotifyClients("/webimporter/v1/local/queue/task/file/uploaded", self.Payload, Tasks.CommandClients, Tasks)
        elif self.Command == "/syncserver/v1/global/upload/queue/task/file/transcoded":
            ############################ Set transcoded flag ############################
            Tasks.Jobs[self.Payload["ID"]].filelist[self.Payload["file"]].transcoded = self.Payload["transcoded"]
            self.m_NotifyClients("/webimporter/v1/local/queue/task/file/transcoded", self.Payload, Tasks.CommandClients, Tasks)

        elif self.Command == "/syncserver/v1/global/queue/task/metadata/set":
            ############################ Set metadata ############################
            Tasks.Jobs[self.Payload["ID"]].metadata = self.Payload["metadata"]
            self.m_NotifyClients("/webimporter/v1/local/queue/task/metadata/set", self.Payload, Tasks.CommandClients, Tasks)
        elif self.Command == "/syncserver/v1/global/queue/task/active":
            Tasks.Jobs[self.Payload["ID"]].active = self.Payload["active"]
            self.m_NotifyClients("/webimporter/v1/local/queue/task/active", self.Payload, Tasks.CommandClients, Tasks)
        elif self.Command == "/syncserver/v1/global/upload/queue/task/file/atomlink/update":
            Tasks.Jobs[self.Payload["ID"]].filelist[self.Payload["file"]].transferlink = self.Payload["transferlink"]
            Tasks.Jobs[self.Payload["ID"]].filelist[self.Payload["file"]].assetlink = self.Payload["assetlink"]

            self.m_NotifyClients("/webimporter/v1/local/queue/task/file/atomlink/update", self.Payload, Tasks.CommandClients, Tasks)


        elif self.Command == "/syncserver/v1/server/shutdown":
            ############################ SHUTDOWN ############################
            logging.debug("Shutting down server")
            #go to each line manager and ask it to shut down
            Tasks.shutdown = True

        #self.write_message('got it!')

    def on_close(self):
        logging.debug('Command socket connection closed: %s', Tasks.GetClientNameFromHandler(self))
        Tasks.CommandClients.remove(self)
        ## remove the tasks that have the handler attached to it.
        self.aRemove = []
        for ID in Tasks.Jobs:
            if Tasks.Jobs[ID].WSHandler == self:
                self.aRemove.append(ID)

        for ID in self.aRemove:
            logging.debug("Removing Job:%s", ID)
            del Tasks.Jobs[ID]

        Tasks.RemoveHandlerFromClientList(self)
        print(Tasks.clientlist)
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