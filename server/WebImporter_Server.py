import json
import common as hfn
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import dataclasses
import workers
import client as cl
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )

import commands as cm
import websocket
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.websocket
import tornado.gen

class c_ServerCommands():
    def m_setpriority(self,Payload):
        logging.debug("************Setting new priorities*************************")
        Tasks.Order = Payload
        self.counter = 1
        for ID in Tasks.Order:
            Tasks.Jobs[ID].order = self.counter
            if Tasks.Jobs[ID].type == "local":
                self.WriteJob(Tasks,ID)
            self.counter += 1
        self.counter = 0
        for ID in Tasks.Order:
            logging.debug("[%s] Order:%s | ID:%s",Tasks.Jobs[ID].type,self.counter,ID)
            self.counter += 1

        #only do this if the queue is active

        if len(Tasks.LineManagers)>0:
            self.m_put_tasks_on_queue()
    def m_setglobalpriority(self,Payload):
        if Tasks.syncserver_client.connected:
            Tasks.syncserver_client.m_send(Tasks.syncserver_client.m_create_data("/syncserver/v1/global/queue/set_priority", Payload))
        else:
            logging.debug("Sync server not available. Setting priority locally only")
            self.m_setpriority(Payload)

    def m_put_tasks_on_queue(self):
        #stop the jobs in the queue
        logging.debug("putting tasks on queue")
        self.OldStatus = {}
        for ID in Tasks.Jobs:
            self.OldStatus[ID] = Tasks.Jobs[ID].active
            Tasks.Jobs[ID].active = False
        for p in Tasks.LineManagers:
            while len(p.Threads)>0:
                continue
        #clear queue
        while not Tasks.task_queue.empty():
            Tasks.task_queue.get()
        #add task to queue in the right order
        for ID in Tasks.Order:
            if Tasks.Jobs[ID].type == "local":
                logging.debug("Putting task %s on queue", ID)
                Tasks.Jobs[ID].active = self.OldStatus[ID]
                Tasks.task_queue.put(Tasks.Jobs[ID])
    def m_activate_queue(self):
        for i in range(Tasks.WorkData["Line_Managers"]):
            if len(Tasks.LineManagers) < Tasks.WorkData["Line_Managers"]:
                self.LineManager = workers.c_LineCopyManager(Tasks, "Line_manager_" + str(i))
                self.LineManager.start()
                Tasks.LineManagers.append(self.LineManager)
            else:
                logging.debug("Line managers are already activated (%s defined in config)",Tasks.WorkData["Line_Managers"])
    def m_deactivate_queue(self):

        if len(Tasks.LineManagers)>0:
            logging.debug("Stopping line managers")
            for ID in Tasks.Jobs:
                self.m_pause_task(ID)
            while not Tasks.task_queue.empty():
                Tasks.task_queue.get()
            for p in Tasks.LineManagers:
                Tasks.task_queue.put(None)
            for p in Tasks.LineManagers:
                p.join()
                Tasks.LineManagers.remove(p)
        else:
            logging.debug("Line managers already stopped")
    def m_start_task(self,ID):
        if ID in Tasks.Jobs:
            if Tasks.Jobs[ID].state == "ready":
                if Tasks.Jobs[ID].active == False:
                    if Tasks.Jobs[ID].type == "local":
                        if Tasks.Jobs[ID].IsComplete() == False:
                            Tasks.Jobs[ID].active = True
                            Tasks.Jobs[ID].workerlist = {}
                            Tasks.Jobs[ID].progress = Tasks.Jobs[ID].GetCurrentProgress()
                            logging.debug("Activating task:%s", ID)
                else:
                    self.Output["status"] = "Task already started"
#                    Tasks.syncserver_client.m_reply(self.Output,self.request)
            else:
                self.Output["status"] = "Task is busy. Try again when it is ready"
 #               logging.debug("jejejsdfsdfsdfe")
 #               Tasks.syncserver_client.m_reply(self.Output,self.request)

        else:
         #   logging.debug("jejeje")
            self.Output["status"] = "Task does not exist"
          #  Tasks.syncserver_client.m_reply(self.Output,self.request)
    def m_get_tasks(self):
        self.aJobs = []
        for key,value in Tasks.Jobs.items():
            self.aJobs.append(key)
        self.Output["job"] = self.aJobs
        #logging.debug("sending : %s", json.dumps(self.Output))
        Tasks.syncserver_client.m_reply(self.Output, self)
    def m_pause_task(self,ID):
        Tasks.Jobs[ID].active = False
        self.Output["status"] = "Paused job " + ID
        Tasks.syncserver_client.m_reply(self.Output, self)
        self.WriteJob(Tasks,ID)
    def m_getpriority(self):

        self.data = Tasks.syncserver_client.m_send(Tasks.syncserver_client.m_create_data('/syncserver/v1/global/queue/get_priority'))
        Tasks.Order = []
        self.counter = 0
        for Data in self.data:
            Tasks.Order.append(Data["ID"])
            Tasks.Jobs[Data["ID"]].order = self.counter
            self.counter += 1
    def m_resume_job(self,ID):
        if ID in Tasks.Jobs:
            if Tasks.Jobs[ID].active == False:
                if Tasks.Jobs[ID].IsComplete() == False:
                    logging.debug("Resuming Job : %s",ID)
                    Tasks.Jobs[ID].active = True
                    Tasks.Jobs[ID].workerlist = {}
                else:
                    logging.debug("Cannot resume task: %s. Task is already complete. Restart Task if you want to do the job again", ID)
        else:
            self.Output["status"] = "Cannot resume as task does not exist"
            Tasks.syncserver_client.m_reply(self.Output,self.request)

        self.m_put_tasks_on_queue()
class c_Client_ProgressHandler(tornado.websocket.WebSocketHandler):
    ### This is the Progress handler taking care of progress data coming FROM the client connecting to this web importer instance .... Only needed to register connecting clients
    ### status is now pushed to the clients connected to it.

    """
    def m_status(self):
        if self.Payload in Tasks.Jobs:
            if Tasks.Jobs[self.Payload].active:
                if Tasks.Jobs[self.Payload].progress == -1:
                    self.Output["status"] = "Not Started"
                    self.Output["worker"] = {}
                elif Tasks.Jobs[self.Payload].progress < 100.0 and Tasks.Jobs[self.Payload].progress > -1:
                    self.Output["status"] = Tasks.Jobs[self.Payload].progress
                    self.Output["worker"] = {}
                    for worker,file in Tasks.Jobs[self.Payload].workerlist.items():
                        self.Output["worker"][worker] = [] #job
                        self.AddFile = {}
                        for file, progress in file.items():
                            self.Output["worker"][worker].append({file:progress})
                            if progress < 100.0:
                                self.AddFile[file] = progress
                        Tasks.Jobs[self.Payload].workerlist[worker] = self.AddFile
                elif Tasks.Jobs[self.Payload].progress == 100.0:
                    self.Output["status"] = "Job Complete"

                #self.write_message(json.dumps(self.Output))
                Tasks.syncserver_client.m_reply(self.Output, self)
            else:
                if Tasks.Jobs[self.Payload].progress == 100.0:
                    self.Output["status"] = "Job Complete"
                    Tasks.syncserver_client.m_reply(self.Output, self)
                else:
                    if Tasks.Jobs[self.Payload].progress < 100.0 and Tasks.Jobs[self.Payload].progress > 0:
                        self.Output["status"] = "Job paused"
                        Tasks.syncserver_client.m_reply(self.Output, self)
                    elif Tasks.Jobs[self.Payload].progress == -1:
                        self.Output["status"] = "Not Started"

                        Tasks.syncserver_client.m_reply(self.Output, self)
                    else:
                        self.Output["status"] = "In queue"
                        Tasks.syncserver_client.m_reply(self.Output, self)
    """
    def open(self):
        #print('new connection')
        Tasks.ProgressClients.append(self)
    def on_message(self, data):
        self.Data = json.loads(data)
        self.Command = self.Data["command"]
        self.Payload = self.Data["payload"]

    def on_close(self):
        logging.debug('connection closed')
        Tasks.ProgressClients.remove(self)
class c_Client_CommandHandler(tornado.websocket.WebSocketHandler, hfn.c_HelperFunctions, c_ServerCommands):
    ### This is the COMMAND handler taking care of commands coming FROM the client connecting to this web importer instance. This is needed
    def open(self):
        print('new connection')
        Tasks.CommandClients.append(self)
        #self.write_message("connected")
    def on_message(self, data):
        self.Data = json.loads(data)
        self.Command = self.Data["command"]
        self.Payload = self.Data["payload"]
        #logging.debug("type of incoming:%s", type(self.Payload))
        self.Output = {}
        if self.Command == "/webimporter/v1/queue/task/create":
            ############################ CREATE TASK ############################
            self.Thread = cm.c_createTask(self.Payload, Tasks)
            self.Thread.start()
            #self.write_message(u"Your command was:" + self.Command)
        elif self.Command == "/webimporter/v1/queue/task/start":
            ############################ ACTIVATE TASK ############################
            self.ID = self.Payload
            self.m_start_task(self.ID)
        elif self.Command == "/webimporter/v1/queue/task/restart":
            ############################ RESTART TASK ############################
            self.ID = self.Payload
            self.Thread = cm.c_restart_task(self.ID, Tasks)
            self.Thread.start()
        elif self.Command == "/webimporter/v1/queue/task/resume":
            ############################ RESUME TASK ############################
            self.ID = self.Payload
            self.m_resume_job(self.ID)
        elif self.Command == "/webimporter/v1/queue/status":
            ############################ GET STATUS ############################
            self.m_status()
        elif self.Command == "/webimporter/v1/queue/task/get_all":
            ############################ GET TASK ############################
            self.m_get_tasks()

        elif self.Command == "/webimporter/v1/global/queue/put":
            ############################ PUT TASK ON GLOBAL LIST ############################
            #self.data = json.loads(self.Payload)
            logging.debug("Received tasks from syncserver:%s", len(self.Payload["TaskList"]))
            self.m_deSerializeTaskList(self.Payload, Tasks)



        # elif self.Command == "get_active_tasks":
        #     self.m_get_active_tasks()
        elif self.Command == "/webimporter/v1/queue/task/pause":
            ############################ PAUSE TASK ############################
            self.ID = self.Payload
            self.m_pause_task(self.ID)
        elif self.Command == "/webimporter/v1/queue/task/remove_completed":
            ############################ REMOVE COMPLETED TASKS ############################
            self.Thread = cm.c_remove_completed_tasks(self.Payload, Tasks)
            self.Thread.start()
        elif self.Command == "/webimporter/v1/queue/task/remove_incomplete":
            ############################ REMOVE INCOMPLETE TASKS ############################
            self.Thread = cm.m_remove_incomplete_tasks(self.Payload, Tasks)
            self.Thread.start()
        elif self.Command == "/webimporter/v1/queue/task/modify":
            ############################ MODIFY TASK ############################
            self.ID = self.Payload["ID"]
            self.Payload = self.Payload["Payload"]
            self.Thread = cm.c_modify_task(self.ID, self.Payload, Tasks)
            self.Thread.start()
        elif self.Command == "/webimporter/v1/local/queue/set_priority":
            ############################ SET LOCAL PRIORITY ON TASKS ############################
            self.m_setpriority(self.Payload)
        elif self.Command == "/webimporter/v1/global/queue/set_priority":
            ############################ SET GLOBAL PRIORITY ON TASKS ############################
            #send this information to the sync server. The server will then notify all registered clients and invoke the local set priority command with the assosciated payload.
            self.m_setglobalpriority(self.Payload)
        elif self.Command == "/webimporter/v1/global/queue/get_priority":
            ############################ GET GLOBAL PRIORITY ON TASKS ############################
            self.m_getpriority()
        elif self.Command == "/webimporter/v1/queue/activate":
            ############################ ACTIVATE QUEUE MANAGERS ############################
            self.m_activate_queue()
        elif self.Command == "/webimporter/v1/queue/deactivate":
            ############################ DEACTIVATE QUEUE MANAGERS ############################
            self.m_deactivate_queue()
        elif self.Command == "/webimporter/v1/queue/put_tasks":
            ############################ PUT LOCAL TASKS ON LOCAL QUEUE ############################
            self.m_put_tasks_on_queue()
        elif self.Command == "/webimporter/v1/server/shutdown":
            ############################ SHUTDOWN SERVER ############################
            logging.debug("Shutting down server")
            #go to each line manager and ask it to shut down
            self.m_deactivate_queue()
            Tasks.MainLoop.stop()
        #print('Command data: %s', json.loads(data))

        #self.write_message("None")
    def on_close(self):
        print('connection closed')
        Tasks.CommandClients.remove(self)
class c_SyncServer_CommandHandler(hfn.c_HelperFunctions, c_ServerCommands):
    ### this is the command handler for messages coming FROM the sync server
    def on_message(self, ws, data):
        logging.debug("Message from the command SYNC SERVER was: %s", data)
        self.Data = json.loads(data)
        self.Command = self.Data["command"]
        self.Payload = self.Data["payload"]
        if self.Command == "/webimporter/v1/connection/connected":
            logging.debug("Received connected response from command server")
        elif self.Command == "/webimporter/v1/global/queue/put":
            ############################ PUT TASK ON GLOBAL LIST ############################
            logging.debug("Received tasks from syncserver:%s", len(self.Payload["TaskList"]))
            self.m_deSerializeTaskList(self.Payload, Tasks)
            ###push the new data to the client connected
            self.m_NotifyClients("/client/v1/local/queue/task/put",self.Payload, Tasks.CommandClients)
            #Tasks.syncserver_client.m_send(self.m_create_data("/client/v1/local/queue/task/put", self.Payload))


        elif self.Command == "/webimporter/v1/local/queue/set_priority":
            ############################ SET LOCAL PRIORITY ON TASKS ############################
            self.m_setpriority(self.Payload)
class c_SyncServer_ProgressHandler(hfn.c_HelperFunctions):
    ###this is the progress handler for messages coming FROM the sync server

    def on_message(self, ws, data):
        #logging.debug("Message to the progress client was: %s", data)
        self.Data = json.loads(data)
        self.Command = self.Data["command"]
        self.Payload = self.Data["payload"]
        if self.Command == "/webimporter/v1/global/queue/task/set_progress":
            logging.debug("Received TASK progress from syncserver:%s:%s", self.Payload["ID"], self.Payload["progress"])
            Tasks.Jobs[self.Payload["ID"]].progress = self.Payload["progress"]


        elif self.Command == "/webimporter/v1/global/queue/task/file/set_progress":
            logging.debug("Received FILE progress from syncserver:%s:%s", self.Payload["file"], self.Payload["progress"])
            Tasks.Jobs[self.Payload["ID"]].filelist[self.Payload["file"]].progress = self.Payload["progress"]

class TornadoServer(hfn.c_HelperFunctions):
     def on_message(self, ws, message):
        logging.debug("Message to the client was: %s", message)

     def __init__(self):
        global Tasks
        Tasks = dataclasses.c_ServerData()
        #read config
        Tasks.WorkData = self.m_ReadConfig()
        Tasks.syncserver_client = cl.threaded_Websocket_Client(Tasks.WorkData["syncserver_ip"],Tasks.WorkData["syncserver_port"],Tasks, "command", c_SyncServer_CommandHandler())
        Tasks.syncserver_progress_client = cl.threaded_Websocket_Client(Tasks.WorkData["syncserver_ip"],Tasks.WorkData["syncserver_port"],Tasks, "progress", c_SyncServer_ProgressHandler())

     def m_ReadConfig(self):
        self.dict_WorkData = {}
        self.tree = ET.ElementTree(file="./config.xml")
        self.config = self.tree.getroot()
        self.dict_WorkData["Line_Managers"] = int(self.config.find("lineworkers").text)
        self.dict_WorkData["CopyWorkers_Per_Line"] = int(self.config.find("copyworkers").text)
        self.dict_WorkData["sTargetDir"] = self.config.find("path").text
        self.dict_WorkData["local_serverport"] = int(self.config.find("local_serverport").text)
        self.dict_WorkData["Num_TCP_port"] = int(self.config.find("numtcpport").text)
        self.dict_WorkData["large_file_threshold"] = int(self.config.find("largefilethreshold").text)
        self.dict_WorkData["syncserver_ip"] = self.config.find("syncserver_ip").text
        self.dict_WorkData["syncserver_port"] = self.config.find("syncserver_port").text
        return self.dict_WorkData
     def m_ReadJobList(self, bReport=True):
        logging.debug("Init local TCP server: Reading existing local jobs from %s", Tasks.WorkData["sTargetDir"])
        self.aFiles = self.get_xmljobs(Tasks)
        self.filecounter = 0

        for f in self.aFiles:
            logging.debug(f)
            self.tree = ET.ElementTree(file=f)
            self.root = self.tree.getroot()
            Tasks.Jobs[self.root.attrib["ID"]] = dataclasses.c_Task(self.root.attrib["ID"])
            Tasks.Jobs[self.root.attrib["ID"]].state = self.root.attrib["state"]
            Tasks.Jobs[self.root.attrib["ID"]].active = self.StringToBool(self.root.attrib["active"])
            Tasks.Jobs[self.root.attrib["ID"]].order = int(self.root.attrib["order"])

            self.bIsActive = True
            self.CopiedFiles = 0
            for file in self.root.find("FileList").findall("File"):
                Tasks.Jobs[self.root.attrib["ID"]].filelistOrder.append(file.attrib["file"])
                Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]] = dataclasses.c_file(int(file.attrib["size"]))

                Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]].copied = self.StringToBool(file.attrib["copied"])
                if self.StringToBool(file.attrib["copied"]) == True:
                    Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]].progress = 100.0
                    self.CopiedFiles += 1

                Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]].delete = self.StringToBool(file.attrib["delete"])
                Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]].uploaded = self.StringToBool(file.attrib["uploaded"])
                Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]].size = int(file.attrib["size"])
            if len(Tasks.Jobs[self.root.attrib["ID"]].filelist)>0:
                Tasks.Jobs[self.root.attrib["ID"]].progress = (self.CopiedFiles/len(Tasks.Jobs[self.root.attrib["ID"]].filelist))*100
            else:

                Tasks.Jobs[self.root.attrib["ID"]].progress = 0.0

        self.PreviousID = None
        Tasks.Order = []
        self.TempList = []

        for ID in Tasks.Jobs:
            if self.PreviousID != None:
                if Tasks.Jobs[ID].order > self.PreviousID.order:
                    #if the next task is order id is larger than the previous then add it AFTER the previous one
                    self.TempList.append(Tasks.Jobs[ID])
                    Tasks.Order.append(ID)
                else:
                    #if it is smaller than the previous order id then add it BEFORE the previous one
                    self.listindex = self.TempList.index(self.PreviousID)
                    self.TempList.insert(self.listindex, Tasks.Jobs[ID])
                    Tasks.Order.insert(self.listindex, ID)
            else:
                self.TempList.append(Tasks.Jobs[ID])
                Tasks.Order.append(ID)

            self.PreviousID = Tasks.Jobs[ID]

        if bReport:
            for ID in Tasks.Order:
                self.aIncompleteFiles = 0
                if ID in Tasks.Jobs:
                    for file in Tasks.Jobs[ID].filelist:
                        if Tasks.Jobs[ID].filelist[file].copied == True:
                            self.aIncompleteFiles += 1
                    if Tasks.Jobs[ID].type == "local":
                        logging.debug("Loading LOCAL task:[%s] %s : %s files. %s percent complete", Tasks.Jobs[ID].order, ID, len(Tasks.Jobs[ID].filelist), (self.aIncompleteFiles/len(Tasks.Jobs[ID].filelist)*100))

        logging.debug("finished reading")
     def run(self):
        app = tornado.web.Application(
        handlers=[
            (r"/progress", c_Client_ProgressHandler),
            (r"/command", c_Client_CommandHandler)
        ]
        )
        self.httpServer = tornado.httpserver.HTTPServer(app)
        self.httpServer.listen(Tasks.WorkData["local_serverport"])
        logging.debug ("Webimporter listening on port:%s", Tasks.WorkData["local_serverport"])

        #read the jobs locally first
        self.m_ReadJobList(not Tasks.syncserver_client.connected)
        if Tasks.syncserver_client.connected:
            #send all the jobs that has been read locally to the sync server. Do not get anything in return

            #ask the sync server what jobs do you have. Only send IDs, do not send anything else

            Tasks.SimpleClient = cl.simple_connection(Tasks, "command")
            self.response = Tasks.SimpleClient.m_request(self.m_create_data('/syncserver/v1/global/queue/task/get_IDs'))
#            logging.debug("reply:%s", self.response)
            self.dictID = {}
            self.GetTask = []
            self.SendTask = []

            self.response = json.loads(self.response)
            for ID in self.response:
                self.dictID[ID] = ID
                #tasks that I do not have
                if not ID in Tasks.Order:
                    self.GetTask.append(ID)

            for ID in Tasks.Order:
                if not ID in self.dictID:
                    self.SendTask.append(Tasks.Jobs[ID])

            logging.debug("Uploading tasks:%s | Downloading tasks:%s", len(self.SendTask), len(self.GetTask))
            #if I have some jobs that you do not have, then send them only. the sent tasks will be appended to the global list.

            if len(self.SendTask) > 0:
                logging.debug("Sending tasks that do not exists on sync server:%s", len(self.SendTask))
                Tasks.syncserver_client.m_send(self.m_create_data('/syncserver/v1/global/queue/task/put_no_reply', self.m_SerialiseTaskList(self.SendTask, Tasks)))

            if len(self.GetTask)>0:
                #request only the jobs that i need
                self.response = Tasks.SimpleClient.m_request(self.m_create_data('/syncserver/v1/global/queue/task/request', self.GetTask))
                #the data will come back with an dictionary with a list of the order of the IDs and the a list of the data that we requested
                self.m_deSerializeTaskList(json.loads(self.response), Tasks)

            self.m_show_tasks(Tasks)

        Tasks.MainLoop = tornado.ioloop.IOLoop.instance()
        logging.debug("starting mainloop")
        Tasks.MainLoop.start()

        Tasks.syncserver_client.connection.close()
        Tasks.syncserver_progress_client.connection.close()
