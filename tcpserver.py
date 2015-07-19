import socket
import queue
import os
import uuid
import json
import time
import helper as hfn
import dataclasses
import shutil
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import logging
import workers
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
import threading
class c_PreTaskWorker(threading.Thread,hfn.c_HelperFunctions):
    def __init__(self,Data):
        threading.Thread.__init__(self)
        self.Data = Data
        self.dict_Data = self.Data["dict_WorkData"]
        self.dict_Jobs = self.Data["dict_Jobs"]
        self.ID = self.Data["ID"]
        self.command = self.Data["command"]
        print("Running command:%s", self.command)
    def m_expand_files(self):
        self.payload = self.Data["payload"]
        self.dict_Jobs[self.ID].state = "busy"
        self.dict_Jobs[self.ID].filelist = self.FileExpand(self.ID,self.payload)
        self.dict_Jobs[self.ID].state = "ready"
    def m_restart_task(self):
        self.task_queue = self.Data["task_queue"]
        if self.ID in self.dict_Jobs:
            self.dict_Jobs[self.ID].active = True
            self.dict_Jobs[self.ID].workerlist = {}
            self.dict_Jobs[self.ID].ResetFileStatus()
            self.dict_Jobs[self.ID].progress = -1
            for folder in os.listdir(self.dict_Data["sTargetDir"]+ self.ID):
                self.fullpath = self.dict_Data["sTargetDir"] + self.ID + "/" + folder + "/"
                if os.path.isdir(self.fullpath):
                    shutil.rmtree(os.path.normpath(self.fullpath),onerror=self.remove_readonly)
                else:
                    logging.debug("%s is not a folder",self.fullpath)
            self.dict_Jobs[self.ID].progress = self.dict_Jobs[self.ID].GetCurrentProgress()
            self.task_queue.put(self.dict_Jobs[self.ID])
    def m_remove_task(self):
        self.task_queue = self.Data["task_queue"]
        if self.ID in self.dict_Jobs:
            self.fullpath = self.dict_Data["sTargetDir"] + self.ID
            if os.path.isdir(self.fullpath):
                shutil.rmtree(os.path.normpath(self.fullpath),onerror=self.remove_readonly)
                logging.debug("Removing completed Job:%s",self.ID)
            else:
                logging.debug("%s is not a folder",self.fullpath)

        del self.dict_Jobs[self.ID]
    def m_modify_task(self):
        self.incomingFiles = self.FileExpand(self.ID, self.Payload)
        self.aDeleteList = []
        self.dict_Data[self.ID].state = "busy"
        if len(self.dict_Data[self.ID].filelist) < len(self.incomingFiles):
            #if there is less files in the current list then iterate over the current list against the incoming files. If file in current list
            #exist in incoming files, then keep file. If file in current list does not exist in incoming list, then mark for deletion.

            for file,state in self.dict_Data[self.ID].filelist:
                if file in self.incomingFiles == False:
                    self.aDeleteList.append(file)
                else:
                    self.incomingFiles[file] = self.dict_Data[self.ID].filelist[file]
            #delete files

            for delfile in self.aDeleteList:
                os.remove(delfile)
                #also remove folder if the folder is empty.

            self.dict_Data[self.ID].filelist = self.incomingFiles
        else:
            #if there are more files in the current list than in the incoming list, then iterate over the incoming list. If the file in the incoming list
            #exists in the current list AND the file in the current list has been marked as copied, then keep the file. If it does not exists

            for file,state in self.incomingFiles:
                self.dict_Data[self.ID].filelist[file].delete = True
                if file in self.incomingFiles == True and state["copied"] == True:
                    self.dict_Data[self.ID].filelist[file].delete = False
                    self.incomingFiles[file] = self.dict_Data[self.ID].filelist[file]

            for file, state in self.dict_Data[self.ID].filelist:
                if state["delete"] == True:
                    os.remove(file)

            self.dict_Data[self.ID].filelist = self.incomingFiles

        self.dict_Data[self.ID].state = "ready"
        if os.path.exists(self.dict_Data["sTargetDir"] + self.ID):
            self.dict_Data[self.ID].state = "busy"
            shutil.rmtree(self.dict_Data["sTargetDir"] + self.ID)
            self.dict_Data[self.ID].state = "ready"
    def run(self):
        if self.command == "restart_task":
            self.m_restart_task()
        elif self.command == "expand_files":
            self.m_expand_files()
        elif self.command == "modify_task":
            self.m_modify_task()
        elif self.command == "remove_task":

            self.m_remove_task()
class TCPServer(hfn.c_HelperFunctions):
    def __init__(self):
        self.dict_WorkData = self.m_ReadConfig()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('localhost',(self.dict_WorkData["serverport"])))
        self.socket.listen(self.dict_WorkData["Num_TCP_port"])
        self.task_queue = queue.Queue()
        self.dict_Jobs = {}
        self.dict_fileprogress = {}
        self.aLineManagers = []

        self.Data = {}
        self.Data["dict_WorkData"] = self.dict_WorkData
        self.Data["dict_Jobs"] = self.dict_Jobs

        for i in range(self.dict_WorkData["Line_Managers"]):
            self.LineManager = workers.c_LineCopyManager(self.task_queue, self.dict_Jobs, self.dict_WorkData, "Line_manager_" + str(i))
            self.LineManager.start()
            self.aLineManagers.append(self.LineManager)
        self.buffersize = 1024*1024*5
        self.m_ReadJobList()
        self.threads = []
        self.run()
    def m_ReadConfig(self):
        self.dict_WorkData = {}
        self.tree = ET.ElementTree(file="./config.xml")
        self.config = self.tree.getroot()
        self.dict_WorkData["Line_Managers"] = int(self.config.find("lineworkers").text)
        self.dict_WorkData["CopyWorkers_Per_Line"] = int(self.config.find("copyworkers").text)
        self.dict_WorkData["sTargetDir"] = self.config.find("path").text
        self.dict_WorkData["serverport"] = int(self.config.find("serverport").text)
        self.dict_WorkData["Num_TCP_port"] = int(self.config.find("numtcpport").text)
        self.dict_WorkData["large_file_threshold"] = int(self.config.find("largefilethreshold").text)
        return self.dict_WorkData
    def StringToBool(self,input):
        if input == "True":
            return True
        else:
            return False
    def m_ReadJobList(self):
        logging.debug("Init TCP server: Reading existing jobs from %s", self.dict_WorkData["sTargetDir"])
        self.aFiles = self.get_filepaths(self.dict_WorkData["sTargetDir"])

        for f in self.aFiles:
            self.head,self.tail = os.path.split(f)
            self.head, self.tail = (os.path.splitext(self.tail))
            if self.tail.lower() == ".xml":
                self.tree = ET.ElementTree(file=f)
                self.task = self.tree.getroot()
                self.dict_Jobs[self.task.attrib["ID"]] = dataclasses.c_Task(self.task.attrib["ID"])
                self.dict_Jobs[self.task.attrib["ID"]].state = self.task.attrib["state"]
                self.dict_Jobs[self.task.attrib["ID"]].active = self.task.attrib["active"]

                self.bIsActive = True
                self.CopiedFiles = 0
                for file in self.task.find("FileList").findall("File"):
                    self.dict_Jobs[self.task.attrib["ID"]].filelist[file.attrib["file"]] = dataclasses.c_file(int(file.attrib["size"]))

                    self.dict_Jobs[self.task.attrib["ID"]].filelist[file.attrib["file"]].copied = self.StringToBool(file.attrib["copied"])
                    if file.attrib["copied"] == False:
                        self.bIsActive = False
                    else:
                        self.dict_Jobs[self.task.attrib["ID"]].filelist[file.attrib["file"]].progress = 100.0
                        self.CopiedFiles += 1
                    self.dict_Jobs[self.task.attrib["ID"]].filelist[file.attrib["file"]].delete = self.StringToBool(file.attrib["delete"])
                    self.dict_Jobs[self.task.attrib["ID"]].filelist[file.attrib["file"]].uploaded = self.StringToBool(file.attrib["uploaded"])

                self.dict_Jobs[self.task.attrib["ID"]].active = self.bIsActive
                self.dict_Jobs[self.task.attrib["ID"]].progress = (self.CopiedFiles/len(self.dict_Jobs[self.task.attrib["ID"]].filelist))*100


        for f in self.dict_Jobs:
            self.aIncompleteFiles = 0
            for file in self.dict_Jobs[f].filelist:
                if self.dict_Jobs[f].filelist[file].copied == False:
                    self.aIncompleteFiles += 1
            logging.debug("Loading task:[%s] %s : %s files. %s Incomplete", self.dict_Jobs[f].active, f, len(self.dict_Jobs[f].filelist), self.aIncompleteFiles)


    def m_create_task(self):
        logging.debug("Creating task : %s",str(self.ID))
        self.dict_Jobs[str(self.ID)] = dataclasses.c_Task(self.ID)
        time.sleep(0.1)
        self.Data["ID"] = str(self.ID)
        self.Data["command"]="expand_files"
        self.Data["payload"]=self.Payload

        self.FileExpander = c_PreTaskWorker(self.Data)
        self.FileExpander.start()
        self.Output["status"] = str(self.ID)
        self.client.send(bytes(json.dumps(self.Output) ,'utf-8'))
        self.FileExpander.join()

    def m_start_task(self):
        if self.Payload in self.dict_Jobs:
            if self.dict_Jobs[self.Payload].state == "ready":
                if self.dict_Jobs[self.Payload].active == False:
                    if self.dict_Jobs[self.Payload].IsComplete() == False:
                        self.dict_Jobs[self.Payload].active = True
                        self.dict_Jobs[self.Payload].workerlist = {}
                        self.dict_Jobs[self.Payload].progress = self.dict_Jobs[self.Payload].GetCurrentProgress()
                        self.task_queue.put(self.dict_Jobs[self.Payload])
                else:
                    self.Output["status"] = "Task already started"
                    self.client.send(bytes(json.dumps(self.Output),'utf-8'))
            else:
                self.Output["status"] = "Task is busy. Try again when it is ready"
                self.client.send(bytes(json.dumps(self.Output),'utf-8'))
        else:
            self.Output["status"] = "Task does not exist"
            self.client.send(bytes(self.Output,'utf-8'))

    def m_restart_task(self):
        self.Data["command"] = "restart_task"
        self.Data["ID"] = self.Payload
        self.Data["task_queue"] = self.task_queue
        self.RestartWorker = c_PreTaskWorker(self.Data)
        self.RestartWorker.start()

        if self.Payload in self.dict_Jobs:
            if self.dict_Jobs[self.Payload].state == "ready":
                if self.dict_Jobs[self.Payload].active == False:
                    if self.dict_Jobs[self.Payload].IsComplete() == True:
                        self.Output["status"] = "Task is being prepared for restart"
                        self.client.send(bytes(json.dumps(self.Output),'utf-8'))
                else:
                    self.Output["status"] = "Task already started"
                    self.client.send(bytes(json.dumps(self.Output),'utf-8'))
            else:
                self.Output["status"] = "Task is busy. Try again when it is ready"
                self.client.send(bytes(json.dumps(self.Output),'utf-8'))
        else:
            self.Output["status"] = "Task does not exist"
            self.client.send(bytes(self.Output,'utf-8'))
    def m_resume_job(self):
        if self.Payload in self.dict_Jobs:
            if self.dict_Jobs[self.Payload].IsComplete() == False:
                logging.debug("Resuming Job : %s",str(self.Payload))
                self.dict_Jobs[self.Payload].active = True
                self.dict_Jobs[self.Payload].workerlist = {}
                self.task_queue.put(self.dict_Jobs[self.Payload])
            else:
                logging.debug("Cannot resume task: %s. Task is already complete. Restart Task if you want to do the job again",str(self.Payload))
        else:
            self.Output["status"] = "Cannot resume as task does not exist"
            self.client.send(bytes(json.dumps(self.Output),'utf-8'))
    def m_status(self):
        if self.Payload in self.dict_Jobs:
            if self.dict_Jobs[self.Payload].active:
                if self.dict_Jobs[self.Payload].progress == -1:
                    self.Output["status"] = "Not Started"
                    self.Output["worker"] = {}
                elif self.dict_Jobs[self.Payload].progress < 100.0:
                    self.Output["status"] = self.dict_Jobs[self.Payload].progress
                    self.Output["worker"] = {}
                    for worker,file in self.dict_Jobs[self.Payload].workerlist.items():
                        self.Output["worker"][worker] = [] #job
                        self.AddFile = {}
                        for file, progress in file.items():
                            self.Output["worker"][worker].append({file:progress})
                            if progress < 100.0:
                                self.AddFile[file] = progress
                        self.dict_Jobs[self.Payload].workerlist[worker] = self.AddFile
                elif self.dict_Jobs[self.Payload].progress == 100.0:
                    self.Output["status"] = "Job Complete"


                self.client.send(bytes(json.dumps(self.Output),'utf-8'))
            else:
                if self.dict_Jobs[self.Payload].progress == 100.0:
                    self.Output["status"] = "Job Complete"
                    self.client.send(bytes(json.dumps(self.Output),'utf-8'))
                else:
                    if self.dict_Jobs[self.Payload].progress < 100.0 and self.dict_Jobs[self.Payload].progress > 0:
                        self.Output["status"] = "Job paused"
                        self.client.send(bytes(json.dumps(self.Output),'utf-8'))
                    elif self.dict_Jobs[self.Payload].progress == -1:
                        self.Output["status"] = "Not Started"
                        self.client.send(bytes(json.dumps(self.Output),'utf-8'))
                    else:
                        self.Output["status"] = "In queue"
                        self.client.send(bytes(json.dumps(self.Output),'utf-8'))
    def m_get_tasks(self):
        self.aJobs = []
        for key,value in self.dict_Jobs.items():
            self.aJobs.append(key)
        self.Output["job"] = self.aJobs
        self.client.send(bytes(json.dumps(self.Output),'utf-8'))
    def m_get_active_tasks(self):
        self.aJobs = []
        self.JobIDString = ""
        for key,value in self.dict_Jobs.items():
            if value.active == True:
                self.aJobs.append(key)
        self.Output["job"] = self.aJobs
        self.client.send(bytes(json.dumps(self.Output),'utf-8'))
    def m_pause_task(self):
        self.dict_Jobs[self.Payload].active = False
        self.Output["status"] = "Paused job " + self.Payload
        self.client.send(bytes(json.dumps(self.Output),'utf-8'))
        self.WriteJob(self.dict_WorkData,self.dict_Jobs,self.Payload)
    def m_remove_completed_tasks(self):
        self.DeleteList = []
        self.workers = []
        for pl,job in self.dict_Jobs.items():
            if self.dict_Jobs[pl].progress == 100.0:
                self.DeleteList.append(pl)
                self.Data["command"] = "remove_task"
                self.Data["ID"] = pl
                self.Data["task_queue"] = self.task_queue
                self.RemoveWorker = c_PreTaskWorker(self.Data)
                self.RemoveWorker.start()
                self.threads.append(self.RemoveWorker)
            else:
                logging.debug("Job is not complete yet:%s",pl)



    def m_remove_incomplete_tasks(self):
        self.DeleteList = []
        for pl,job in self.dict_Jobs.items():
            if job.progress < 100.0 and job.active == False:
                #self.NewList[pl] = job
                self.DeleteList.append(pl)
                logging.debug("Removing incomplete Job:%s",pl)
            else:
                if self.dict_Jobs[pl].progress == 100.0:
                    logging.debug("Job is completed. Not removing:%s",pl)
                else:
                    logging.debug("Job is still active:" + pl)

        for ID in self.DeleteList:
            del self.dict_Jobs[ID]
            self.WriteJob(self.dict_WorkData,self.dict_Jobs,ID)
    def m_modify_task(self):
        self.ID = self.Payload["ID"]
        self.Payload = self.Payload["Payload"]
        if self.dict_Jobs[self.ID].active == False:
            self.dict_Jobs[self.ID].PauseIndex = 0
            self.dict_Jobs[self.ID].Payload = self.Payload
            print("Task [" + self.ID + "] has been modified:")
            #send remove tree to a seperate thread
            # RemoveWorkerProcess = c_RemoveWorker(dict_WorkData,self.ID)
            # RemoveWorkerProcess.start()
            # RemoveWorkerProcess.join()



            #some logic has to happen here to either remove the previous content, or do a smart diff to check
            #if that are in the new self.Payload has been copied already and skip those. Other files that are not in the
            #new payload must be deleted.
            self.WriteJob(self.dict_WorkData,self.dict_Jobs,self.ID)


    def run(self):
        #this can only process one tcp request at a time. so if a request is taking long to process, then
        #the server will be unresponsive until it is done with previous request.
        while True:
            self.client, self.address = self.socket.accept()
            #logger.debug("{u} connected".format(u=address))
            self.data = self.client.recv(self.buffersize).decode('utf-8')
            self.data = json.loads(self.data)
            self.ID = uuid.uuid4()
            self.Command = self.data["command"]
            self.Payload = self.data["payload"]
            self.Output = {}
            if self.Command == "create_copytask":
                self.m_create_task()
            elif self.Command == "start_task":
                self.m_start_task()
            elif self.Command == "restart_task":
                self.m_restart_task()
            elif self.Command == "resume_job":
                self.m_resume_job()
            elif self.Command == "status":
                self.m_status()
            elif self.Command == "get_tasks":
                self.m_get_tasks()
            elif self.Command == "get_active_tasks":
                self.m_get_active_tasks()
            elif self.Command == "pause_job":
                self.m_pause_task()
            elif self.Command == "remove_completed_tasks":
                self.m_remove_completed_tasks()
            elif self.Command == "remove_incomplete_tasks":
                self.m_remove_incomplete_tasks()
            elif self.Command == "modify_task":
                self.m_modify_task()
            elif self.Command == "shutdown_server":
                logging.debug("Shutting down server")
                for p in self.aLineManagers:
                    self.task_queue.put(None)
                time.sleep(1)
                for p in self.workers:
                    p.join()
                for p in self.aLineManagers:
                    p.join()
                break

            self.client.close()
