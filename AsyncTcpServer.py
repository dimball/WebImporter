import threading
import socketserver
import json
import uuid
import helper as hfn
import os
import shutil
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET



import dataclasses

import workers
import time
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
import socket

class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler,hfn.c_HelperFunctions):
    def m_create_task(self,ID,Payload):
        logging.debug("Creating task : %s",ID)
        Tasks.Jobs[ID] = dataclasses.c_Task(ID)
        self.Data["ID"] = ID
        self.Data["command"]="expand_files"
        self.Data["payload"]= Payload
        self.Output["status"] = ID
        Tasks.Jobs[ID].state = "busy"

        Tasks.Order.append(ID)
        self.highestorderID = 0
        for JobID in Tasks.Jobs:
#            logging.debug("job id is:%s,%s", JobID, Tasks.Jobs[JobID].order)
            if Tasks.Jobs[JobID].order > self.highestorderID:
                self.highestorderID = int(Tasks.Jobs[JobID].order)

        logging.debug("Highest order ID:%s", self.highestorderID+1)
        Tasks.Jobs[ID].order = self.highestorderID+1
        Tasks.Jobs[ID].progress = -1
        Tasks.Jobs[ID].filelist = self.FileExpand(ID,Payload)
        logging.debug("Number of files:%s", len(Tasks.Jobs[ID].filelist))
        Tasks.Jobs[ID].state = "ready"

        '''
        When creating a task, also send the data to the sync server. Send the ID here. When metadata is created, then send the ID with the metadata so
        that other clients can see this as well.
        '''
        if Tasks.syncserver_client.connected:
            Tasks.syncserver_client.m_send(Tasks.syncserver_client.m_create_data('/syncserver/v1/global/queue/task/put',Tasks.syncserver_client.m_SerialiseSyncTasks()))
        self.WriteJob(Tasks,ID)
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
                    self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
            else:
                self.Output["status"] = "Task is busy. Try again when it is ready"
                self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
        else:
            self.Output["status"] = "Task does not exist"
            self.request.sendall(bytes(self.Output,'utf-8'))
    def m_get_tasks(self):
        self.aJobs = []
        for key,value in Tasks.Jobs.items():
            self.aJobs.append(key)
        self.Output["job"] = self.aJobs
        self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
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


                self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
            else:
                if Tasks.Jobs[self.Payload].progress == 100.0:
                    self.Output["status"] = "Job Complete"
                    self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
                else:
                    if Tasks.Jobs[self.Payload].progress < 100.0 and Tasks.Jobs[self.Payload].progress > 0:
                        self.Output["status"] = "Job paused"
                        self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
                    elif Tasks.Jobs[self.Payload].progress == -1:
                        self.Output["status"] = "Not Started"
                        self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
                    else:
                        self.Output["status"] = "In queue"
                        self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
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
        self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
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
                self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
        else:
            self.Output["status"] = "Task does not exist"
            self.request.sendall(bytes(self.Output,'utf-8'))
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
    def m_getpriority(self):

        self.response = Tasks.syncserver_client.m_send(Tasks.syncserver_client.m_create_data('/syncserver/v1/global/queue/get_priority'))
        self.data = json.loads(self.response)
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
            self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))

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
    def m_process_tasks_from_syncserver(self, Payload):

        if len(Payload)>0:
            Tasks.Order = []
            self.counter = 1
            for Data in Payload:
                Tasks.Order.append(Data["ID"])
                if not Data["ID"] in Tasks.Jobs:
                    Tasks.Jobs[Data["ID"]] = dataclasses.c_Task(Data["ID"])
                    Tasks.Jobs[Data["ID"]].type = Data["Data"]["type"]
                Tasks.Jobs[Data["ID"]].order = self.counter
                Tasks.Jobs[Data["ID"]].progress = Data["Data"]["progress"]
                Tasks.Jobs[Data["ID"]].metadata = Data["Data"]["metadata"]
                self.counter += 1

            if Data["report"]:
                for ID in Tasks.Order:
                    if Tasks.Jobs[ID].type == "local":
                        logging.debug("Loading LOCAL task:[%s] %s : %s percent complete", Tasks.Jobs[ID].order, ID, Tasks.Jobs[ID].progress)
                    else:
                        logging.debug("Loading GLOBAL task from sync server:[%s] %s. %s percent complete", Tasks.Jobs[ID].order, ID, Tasks.Jobs[ID].progress)

    def setup(self):
        #print("Setting up request")
        self.Data = {}
        self.Output = {}
    def handle(self):
        #print("handling request")
        self.buffersize = 1024*1024*5
        self.data = self.request.recv(self.buffersize).decode('utf-8')
        self.data = json.loads(self.data)
        self.Command = self.data["command"]
        self.Payload = self.data["payload"]
        self.Output = {}
        if self.Command == "/webimporter/v1/queue/task/create":
            self.ID = str(uuid.uuid4())
            self.m_create_task(self.ID,self.Payload)
        elif self.Command == "/webimporter/v1/queue/task/start":
            self.ID = self.Payload
            self.m_start_task(self.ID)
        elif self.Command == "/webimporter/v1/queue/task/restart":
            self.ID = self.Payload
            self.m_restart_task(self.ID)
        elif self.Command == "/webimporter/v1/queue/task/resume":
            self.ID = self.Payload
            self.m_resume_job(self.ID)
        elif self.Command == "/webimporter/v1/queue/status":
            self.m_status()
        elif self.Command == "/webimporter/v1/queue/task/get_all":
            self.m_get_tasks()
        elif self.Command == "/webimporter/v1/global/queue/put":
            self.data = json.loads(self.Payload)
            logging.debug("Received tasks from syncserver:%s", len(self.data))
            self.m_process_tasks_from_syncserver(self.data)
        # elif self.Command == "get_active_tasks":
        #     self.m_get_active_tasks()
        elif self.Command == "/webimporter/v1/queue/task/pause":
            self.ID = self.Payload
            self.m_pause_task(self.ID)
        elif self.Command == "/webimporter/v1/queue/task/remove_completed":
            self.m_remove_completed_tasks()
        elif self.Command == "/webimporter/v1/queue/task/remove_incomplete":
            self.m_remove_incomplete_tasks()
        elif self.Command == "/webimporter/v1/queue/task/modify":
            self.ID = self.Payload["ID"]
            self.Payload = self.Payload["Payload"]
            self.m_modify_task(self.ID,self.Payload)
        elif self.Command == "/webimporter/v1/local/queue/set_priority":
            self.m_setpriority(self.Payload)
        elif self.Command == "/webimporter/v1/global/queue/set_priority":
            #send this information to the sync server. The server will then notify all registered clients and invoke the local set priority command with the assosciated payload.
            if Tasks.syncserver_client.connected:
                Tasks.syncserver_client.m_send(Tasks.syncserver_client.m_create_data("/syncserver/v1/global/queue/set_priority", self.Payload))
            else:
                logging.debug("Sync server not available. Setting priority locally only")
                self.m_setpriority(self.Payload)

        elif self.Command == "/webimporter/v1/global/queue/get_priority":
            self.m_getpriority()
        elif self.Command == "/webimporter/v1/queue/activate":
            self.m_activate_queue()
        elif self.Command == "/webimporter/v1/queue/deactivate":
            self.m_deactivate_queue()
        elif self.Command == "/webimporter/v1/queue/put_tasks":
            self.m_put_tasks_on_queue()

        elif self.Command == "/webimporter/v1/server/shutdown":
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
        self.aLineManagers = []
        global Tasks
        Tasks = dataclasses.c_data()

        Tasks.WorkData = self.m_ReadConfig()

        Tasks.syncserver_client = hfn.Client(Tasks.WorkData["syncserver_ip"],Tasks.WorkData["syncserver_port"],Tasks)

        #send currentjobs to the sync server?

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
                Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]] = dataclasses.c_file(os.path.getsize(file.attrib["file"]))

                Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]].copied = self.StringToBool(file.attrib["copied"])
                if file.attrib["copied"] == True:
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
        #send all the jobs that has been read locally to the sync server.
        if Tasks.syncserver_client.connected:
            Tasks.syncserver_client.m_send(Tasks.syncserver_client.m_create_data('/syncserver/v1/global/queue/task/put', Tasks.syncserver_client.m_SerialiseSyncTasks()))
        #the put task on the sync server will notify all registered clients about the tasks sent from this client. This also includes the order of the tasks.

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



