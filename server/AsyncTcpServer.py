import threading
import socketserver
import json
import uuid
import common as hfn
import os
import shutil
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import dataclasses
import workers
import time

import client as cl
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )

import commands as cm

from websocket import create_connection
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.websocket
import tornado.gen
from tornado.options import define, options


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler,hfn.c_HelperFunctions):
    def m_create_task(self, Payload):
        self.ID = str(uuid.uuid4())
        logging.debug("Creating task : %s", self.ID)
        Tasks.Jobs[self.ID] = dataclasses.c_Task(self.ID)
        self.Data["ID"] = self.ID
        self.Data["command"]="expand_files"
        self.Data["payload"]= Payload
        self.Output["status"] = self.ID
        Tasks.Jobs[self.ID].state = "busy"

        Tasks.Order.append(self.ID)
        self.highestorderID = 0
        for JobID in Tasks.Jobs:
#            logging.debug("job id is:%s,%s", JobID, Tasks.Jobs[JobID].order)
            if Tasks.Jobs[JobID].order > self.highestorderID:
                self.highestorderID = int(Tasks.Jobs[JobID].order)

        logging.debug("Highest order ID:%s", self.highestorderID+1)
        Tasks.Jobs[self.ID].order = self.highestorderID+1
        Tasks.Jobs[self.ID].progress = -1
        self.FileListData = self.FileExpand(self.ID,Payload)
        Tasks.Jobs[self.ID].filelist = self.FileListData[0]
        Tasks.Jobs[self.ID].filelistOrder = self.FileListData[1]
        logging.debug("Number of files:%s", len(Tasks.Jobs[self.ID].filelist))
        Tasks.Jobs[self.ID].state = "ready"

        '''
        When creating a task, also send the data to the sync server. Send the ID here. When metadata is created, then send the ID with the metadata so
        that other clients can see this as well.
        '''
        if Tasks.syncserver_client.connected:
            Tasks.syncserver_client.m_send(Tasks.syncserver_client.m_create_data('/syncserver/v1/global/queue/task/put',self.m_SerialiseTaskList([Tasks.Jobs[self.ID]], Tasks)))
        self.WriteJob(Tasks,self.ID)
        #self.request.sendall(bytes(json.dumps(self.Output), 'utf-8'))
    def m_put_tasks_on_queue(self):
        #stop the jobs in the queue
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
        Tasks.syncserver_client.m_reply(self.Output,self.request)
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

                Tasks.syncserver_client.m_reply(self.Output,self.request)

            else:
                if Tasks.Jobs[self.Payload].progress == 100.0:
                    self.Output["status"] = "Job Complete"
                    Tasks.syncserver_client.m_reply(self.Output,self.request)
                else:
                    if Tasks.Jobs[self.Payload].progress < 100.0 and Tasks.Jobs[self.Payload].progress > 0:
                        self.Output["status"] = "Job paused"
                        Tasks.syncserver_client.m_reply(self.Output,self.request)
                    elif Tasks.Jobs[self.Payload].progress == -1:
                        self.Output["status"] = "Not Started"

                        Tasks.syncserver_client.m_reply(self.Output,self.request)
                    else:
                        self.Output["status"] = "In queue"
                        Tasks.syncserver_client.m_reply(self.Output,self.request)
    def m_remove_completed_tasks(self):
        self.aList = []
        for ID,job in Tasks.Jobs.items():
            self.aList.append(ID)
        for ID in self.aList:
            if Tasks.Jobs[ID].IsComplete():
                self.fullpath = Tasks.WorkData["sTargetDir"] + ID
                if os.path.isdir(self.fullpath):
                    shutil.rmtree(os.path.normpath(self.fullpath),onerror=self.remove_readonly)
                    del Tasks.Jobs[ID]
                    logging.debug("Removing completed Job:%s",ID)
                else:
                    logging.debug("%s is not a folder",self.fullpath)
            else:
                logging.debug("Job is not complete yet:%s",ID)
    def m_remove_incomplete_tasks(self):
        self.aList = []
        for ID,job in Tasks.Jobs.items():
            self.aList.append(ID)
        for ID in self.aList:
            if not Tasks.Jobs[ID].IsComplete() and not Tasks.Jobs[ID].active:
                self.fullpath = Tasks.WorkData["sTargetDir"] + ID
                if os.path.isdir(self.fullpath):
                    shutil.rmtree(os.path.normpath(self.fullpath),onerror=self.remove_readonly)
                    del Tasks.Jobs[ID]
                    logging.debug("Removing incomplete Job:%s",ID)
                else:
                    logging.debug("%s is not a folder",self.fullpath)
            else:
                logging.debug("Job is still active:" + ID)
    def m_pause_task(self,ID):
        Tasks.Jobs[ID].active = False
        self.Output["status"] = "Paused job " + ID
        Tasks.syncserver_client.m_reply(self.Output,self.request)
        self.WriteJob(Tasks,ID)
    def m_restart_task(self,ID):
        self.Output["status"] = "test"
        self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
        if ID in Tasks.Jobs:
            if Tasks.Jobs[ID].state == "ready":
                if Tasks.Jobs[ID].active == False:
                    Tasks.Jobs[ID].active = True
                    Tasks.Jobs[ID].workerlist = {}
                    Tasks.Jobs[ID].ResetFileStatus()
                    Tasks.Jobs[ID].progress = -1
                    for folder in os.listdir(Tasks.WorkData["sTargetDir"] + ID):
                        self.fullpath = Tasks.WorkData["sTargetDir"] + ID + "/" + folder + "/"
                        if os.path.isdir(self.fullpath):
                            shutil.rmtree(os.path.normpath(self.fullpath),onerror=self.remove_readonly)
                        else:
                            logging.debug("%s is not a folder",self.fullpath)
                    Tasks.Jobs[ID].progress = Tasks.Jobs[ID].GetCurrentProgress()
                    logging.debug("Task is being restarted: %s. Remember to put the tasks on the queue again", ID)
                    self.WriteJob(Tasks,ID)
                    #Tasks.task_queue.put(Tasks.Jobs[ID])
                else:
                    logging.debug("Task is already active:%s", ID)
            else:
                self.Output["status"] = "Task is busy. Try again when it is ready"
                Tasks.syncserver_client.m_reply(self.Output,self.request)
        else:
            self.Output["status"] = "Task does not exist"
            Tasks.syncserver_client.m_reply(self.Output,self.request)
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
    def m_modify_task(self,ID,Payload):
        self.ID = ID
        self.Payload = Payload

        if not Tasks.Jobs[ID].active and Tasks.Jobs[ID].type == "local":
            logging.debug("Getting files and storing their sizes")
            self.incomingFiles = self.FileExpand(self.ID, self.Payload)
            Tasks.Jobs[self.ID].state = "busy"

            if len(Tasks.Jobs[self.ID].filelist) < len(self.incomingFiles):
                #print("there are LESS files in current list")
                #if there is less files in the current list then iterate over the current list against the incoming files. If file in current list
                #exist in incoming files, then keep file. If file in current list does not exist in incoming list, then mark for deletion.
                for file in Tasks.Jobs[self.ID].filelist:
                    Tasks.Jobs[self.ID].filelist[file].delete = True
                    if file in self.incomingFiles == True:
                        Tasks.Jobs[self.ID].filelist[file].delete = False


                    if Tasks.Jobs[self.ID].filelist[file].delete == True:
                        self.head,self.tail = os.path.splitdrive(file)
                        self.dstfile = os.path.normpath(Tasks.WorkData["sTargetDir"] + self.ID + "/" + self.tail)

                        if os.path.exists(self.dstfile):
                            logging.debug("deleting:%s",self.dstfile)
                            shutil.rmtree(self.dstfile,onerror=self.remove_readonly)
                            #also remove folder if the folder is empty.
                            #check if the directory that the file was in can be removed. If it can be removed then remove it.
                            if len(os.listdir(os.path.dirname(self.dstfile)))== 0:
                                logging.debug("Removing directory:%s",os.path.dirname(self.dstfile))
                                os.rmdir(os.path.dirname(self.dstfile))

                Tasks.Jobs[self.ID].filelist = self.incomingFiles
            else:
                #print("there are MORE files in current list")
                #if there are more files in the current list than in the incoming list, then iterate over the incoming list. If the file in the incoming list
                #exists in the current list AND the file in the current list has been marked as copied, then keep the file. If it does not exists

                for file in Tasks.Jobs[self.ID].filelist:
                    Tasks.Jobs[self.ID].filelist[file].delete = True

                for file in self.incomingFiles:
                    #if incoming file exists in the current filelist
                    if file in Tasks.Jobs[self.ID].filelist:
                        #mark the file in the current list to NOT to be deleted
                        Tasks.Jobs[self.ID].filelist[file].delete = False

                for file in Tasks.Jobs[self.ID].filelist:
                    if Tasks.Jobs[self.ID].filelist[file].delete:
                        self.head,self.tail = os.path.splitdrive(file)
                        self.dstfile = os.path.normpath(Tasks.WorkData["sTargetDir"] + self.ID + "/" + self.tail)
                        if os.path.exists(self.dstfile):
                            logging.debug("deleting:%s",self.dstfile)
                            shutil.rmtree(self.dstfile,onerror=self.remove_readonly)
                            #also remove folder if the folder is empty.
                            #check if the directory that the file was in can be removed. If it can be removed then remove it.
                            if len(os.listdir(os.path.dirname(self.dstfile)))== 0:
                                logging.debug("Removing directory:%s",os.path.dirname(self.dstfile))
                                os.rmdir(os.path.dirname(self.dstfile))


                Tasks.Jobs[self.ID].filelist = self.incomingFiles

            Tasks.Jobs[self.ID].state = "ready"
            Tasks.Jobs[self.ID].active = False
            logging.debug("modified task: %s",self.ID)
            self.WriteJob(Tasks,self.ID)

    def setup(self):
        #print("Setting up request")
        self.Data = {}
        self.Output = {}
    def handle(self):
        #print("handling request")
        #self.data = self.request.recv(self.buffersize).decode('utf-8')

        self.data = self.m_receive_all(self.request)


       # logging.debug(self.data)
        self.data = json.loads(self.data)
        self.Command = self.data["command"]
        self.Payload = self.data["payload"]
        self.Output = {}
        if self.Command == "/webimporter/v1/queue/task/create":
            ############################ CREATE TASK ############################
            self.m_create_task(self.Payload)
        elif self.Command == "/webimporter/v1/queue/task/start":
            ############################ ACTIVATE TASK ############################
            self.ID = self.Payload
            self.m_start_task(self.ID)
        elif self.Command == "/webimporter/v1/queue/task/restart":
            ############################ RESTART TASK ############################
            self.ID = self.Payload
            self.m_restart_task(self.ID)
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
        elif self.Command == "/webimporter/v1/global/queue/task/set_progress":
            logging.debug("Received progress from syncserver:%s:%s", self.Payload["ID"], self.Payload["progress"])
            Tasks.Jobs[self.Payload["ID"]].progress = self.Payload["progress"]
        elif self.Command == "/webimporter/v1/global/queue/put":
            ############################ PUT TASK ON GLOBAL LIST ############################
            #self.data = json.loads(self.Payload)
            logging.debug("Received tasks from syncserver:%s", len(self.Payload["TaskList"]))
            self.m_deSerializeTaskList(self.data, Tasks)
        # elif self.Command == "get_active_tasks":
        #     self.m_get_active_tasks()
        elif self.Command == "/webimporter/v1/queue/task/pause":
            ############################ PAUSE TASK ############################
            self.ID = self.Payload
            self.m_pause_task(self.ID)
        elif self.Command == "/webimporter/v1/queue/task/remove_completed":
            ############################ REMOVE COMPLETED TASKS ############################
            self.m_remove_completed_tasks()
        elif self.Command == "/webimporter/v1/queue/task/remove_incomplete":
            ############################ REMOVE INCOMPLETE TASKS ############################
            self.m_remove_incomplete_tasks()
        elif self.Command == "/webimporter/v1/queue/task/modify":
            ############################ MODIFY TASK ############################
            self.ID = self.Payload["ID"]
            self.Payload = self.Payload["Payload"]
            self.m_modify_task(self.ID,self.Payload)
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
            Tasks.shutdown = True

    # def finish(self):
    #     print("cleaning up request")
class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    pass
class server(hfn.c_HelperFunctions):
    def __init__(self):
        global Tasks
        Tasks = dataclasses.c_ServerData()
        #read config
        Tasks.WorkData = self.m_ReadConfig()

        self.Command = create_connection('ws://localhost:9765/command')


        #registering with the sync server. Determine if it is available or not. If not, will run without the sync server however only locally.
        Tasks.syncserver_client = cl.Client(Tasks.WorkData["syncserver_ip"],Tasks.WorkData["syncserver_port"],Tasks)
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
                Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]] = dataclasses.c_file(os.path.getsize(file.attrib["file"]))

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
    def run(self):
        # Port 0 means to select an arbitrary unused port
        HOST, PORT = "localhost",  Tasks.WorkData["local_serverport"]
        server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
        ip, port = server.server_address
        # Start a thread with the server -- that thread will then start one
        # more thread for each request
        server_thread = threading.Thread(target=server.serve_forever)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()
        server_thread.name = "Main_Server"
        #read the jobs locally first
        self.m_ReadJobList(not Tasks.syncserver_client.connected)


        if Tasks.syncserver_client.connected:
            #send all the jobs that has been read locally to the sync server. Do not get anything in return

            #ask the sync server what jobs do you have. Only send IDs, do not send anything else
            self.response = Tasks.syncserver_client.m_send(Tasks.syncserver_client.m_create_data('/syncserver/v1/global/queue/task/get_IDs'))
            self.response = json.loads(self.response)
            self.dictID = {}
            self.GetTask = []
            if self.response != None:
                for ID in self.response:
                    self.dictID[ID] = ID
                    #tasks that I do not have
                    if not ID in Tasks.Order:
                        self.GetTask.append(ID)

            self.SendTask = []

            for ID in Tasks.Order:
                if not ID in self.dictID:
                    self.SendTask.append(Tasks.Jobs[ID])


            #if I have some jobs that you do not have, then send them only. the sent tasks will be appended to the global list.
            logging.debug("Uploading tasks:%s | Downloading tasks:%s", len(self.SendTask), len(self.GetTask))
            if len(self.SendTask) > 0:
                logging.debug("Sending tasks that do not exists on sync server:%s", len(self.SendTask))
                Tasks.syncserver_client.m_send(Tasks.syncserver_client.m_create_data('/syncserver/v1/global/queue/task/put_no_reply', self.m_SerialiseTaskList(self.SendTask, Tasks)))

            if len(self.GetTask)>0:
                #request only the jobs that i need
                self.response = Tasks.syncserver_client.m_send(Tasks.syncserver_client.m_create_data('/syncserver/v1/global/queue/task/request', self.GetTask))
                if self.response != None:
                    #the data will come back with an dictionary with a list of the order of the IDs and the a list of the data that we requested
                    self.m_deSerializeTaskList(self.response, Tasks)

            self.m_show_tasks(Tasks)

        logging.debug("Server loop running in thread:%s", server_thread.name)

        while not Tasks.shutdown:
             time.sleep(1)
             continue

        logging.debug("Exiting server loop")
        while not Tasks.task_queue.empty():
            Tasks.task_queue.get()

        for p in Tasks.LineManagers:
            if p.isAlive():
                Tasks.task_queue.put(None)

        for p in Tasks.LineManagers:
            p.join()

        server.shutdown()
        logging.debug("Server is shutdown")


class ProgressHandler(tornado.websocket.WebSocketHandler):
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
    def open(self):
        print('new connection')
        Tasks.ProgressClients.append(self)
        self.write_message("connected")

    def on_message(self, data):
        self.Data = json.loads(data)
        self.Command = self.Data["command"]
        self.Payload = self.Data["payload"]

        if self.Command == "/webimporter/v1/queue/status":
            self.m_status()


        #print('Progress data: %s', json.loads(data))
        else:
            self.write_message("None")

    def on_close(self):
        print('connection closed')
        Tasks.ProgressClients.remove(self)

class CommandHandler(tornado.websocket.WebSocketHandler, hfn.c_HelperFunctions):
    def m_put_tasks_on_queue(self):
        #stop the jobs in the queue
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



    def open(self):
        print('new connection')
        Tasks.CommandClients.append(self)
        #self.write_message("connected")

    def on_message(self, data):

        self.Data = json.loads(data)
        self.Command = self.Data["command"]
        self.Payload = self.Data["payload"]
        logging.debug("type of incoming:%s", type(self.Payload))
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
        elif self.Command == "/webimporter/v1/global/queue/task/set_progress":
            logging.debug("Received progress from syncserver:%s:%s", self.Payload["ID"], self.Payload["progress"])
            Tasks.Jobs[self.Payload["ID"]].progress = self.Payload["progress"]
        elif self.Command == "/webimporter/v1/global/queue/put":
            ############################ PUT TASK ON GLOBAL LIST ############################
            #self.data = json.loads(self.Payload)
            logging.debug("Received tasks from syncserver:%s", len(self.Payload["TaskList"]))
            self.m_deSerializeTaskList(self.data, Tasks)
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
        print('Command data: %s', json.loads(data))

        self.write_message("None")

    def on_close(self):
        print('connection closed')
        Tasks.CommandClients.remove(self)

class TornadoServer(hfn.c_HelperFunctions):
     def __init__(self):
        global Tasks
        Tasks = dataclasses.c_ServerData()
        #read config
        Tasks.WorkData = self.m_ReadConfig()
        Tasks.syncserver_client = cl.Client(Tasks.WorkData["syncserver_ip"],Tasks.WorkData["syncserver_port"],Tasks, "command")
        Tasks.syncserver_progress_client = cl.Client(Tasks.WorkData["syncserver_ip"],Tasks.WorkData["syncserver_port"],Tasks, "progress")
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
                Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]] = dataclasses.c_file(os.path.getsize(file.attrib["file"]))

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

     def run(self):
        self.m_ReadJobList(False)
        app = tornado.web.Application(
        handlers=[
            (r"/progress", ProgressHandler),
            (r"/command", CommandHandler)
        ]
        )
        self.httpServer = tornado.httpserver.HTTPServer(app)
        self.httpServer.listen(Tasks.WorkData["local_serverport"])
        logging.debug ("Webimporter listening on port:%s", Tasks.WorkData["local_serverport"])
        Tasks.MainLoop = tornado.ioloop.IOLoop.instance()
        Tasks.MainLoop.start()

        Tasks.syncserver_client.connection.close()
        Tasks.syncserver_progress_client.connection.close()
