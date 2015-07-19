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
import logging
import workers
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler,hfn.c_HelperFunctions):
    def m_create_task(self,ID,Payload):
        logging.debug("Creating task : %s",ID)
        Tasks.Jobs[ID] = dataclasses.c_Task(ID)
        self.Data["ID"] = ID
        self.Data["command"]="expand_files"
        self.Data["payload"]= Payload
        self.Output["status"] = ID
        Tasks.Jobs[ID].state = "busy"
        Tasks.Jobs[ID].progress = -1
        Tasks.Jobs[ID].filelist = self.FileExpand(ID,Payload)
        logging.debug("Number of tasks:%s", len(Tasks.Jobs[ID].filelist))
        Tasks.Jobs[ID].state = "ready"
        self.request.sendall(bytes(json.dumps(self.Output), 'utf-8'))
    def m_start_task(self,ID):
        if ID in Tasks.Jobs:
            if Tasks.Jobs[ID].state == "ready":
                if Tasks.Jobs[ID].active == False:
                    if Tasks.Jobs[ID].IsComplete() == False:
                        Tasks.Jobs[ID].active = True
                        Tasks.Jobs[ID].workerlist = {}
                        Tasks.Jobs[ID].progress = Tasks.Jobs[ID].GetCurrentProgress()
                        Tasks.task_queue.put(Tasks.Jobs[ID])
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
                print(Tasks.Jobs[self.Payload].progress)
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
            if Tasks.Jobs[ID].IsComplete() == False and Tasks.Jobs[ID].active == False:
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
        self.WriteJob(Tasks.WorkData,Tasks.Jobs,ID)
    def m_restart_task(self,ID):
        if ID in Tasks.Jobs:
            if Tasks.Jobs[ID].state == "ready":
                    if Tasks.Jobs[ID].IsComplete() == True:
                        self.Output["status"] = "Task is being prepared for restart"
                        self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
                        Tasks.Jobs[ID].active = True
                        Tasks.Jobs[ID].workerlist = {}
                        Tasks.Jobs[ID].ResetFileStatus()
                        Tasks.Jobs[ID].progress = -1
                        for folder in os.listdir(Tasks.WorkData["sTargetDir"]+ ID):
                            self.fullpath = Tasks.WorkData["sTargetDir"] + ID + "/" + folder + "/"
                            if os.path.isdir(self.fullpath):
                                shutil.rmtree(os.path.normpath(self.fullpath),onerror=self.remove_readonly)
                            else:
                                logging.debug("%s is not a folder",self.fullpath)
                        Tasks.Jobs[ID].progress = Tasks.Jobs[ID].GetCurrentProgress()
                        logging.debug("Task is being restarted: %s", ID)
                        Tasks.task_queue.put(Tasks.Jobs[ID])
                        self.Output["status"] = "Restarting task> " + ID
                        self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
            else:
                self.Output["status"] = "Task is busy. Try again when it is ready"
                self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
        else:
            self.Output["status"] = "Task does not exist"
            self.request.sendall(bytes(self.Output,'utf-8'))
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
        if self.Command == "create_copytask":
            self.ID = str(uuid.uuid4())
            self.m_create_task(self.ID,self.Payload)
        elif self.Command == "start_task":
            self.ID = self.Payload
            self.m_start_task(self.ID)
        elif self.Command == "restart_task":
            self.ID = self.Payload
            self.m_restart_task(self.ID)
        # elif self.Command == "resume_job":
        #     self.m_resume_job()
        elif self.Command == "status":
            self.m_status()
        elif self.Command == "get_tasks":
            self.m_get_tasks()
        # elif self.Command == "get_active_tasks":
        #     self.m_get_active_tasks()
        elif self.Command == "pause_job":
            self.ID = self.Payload
            self.m_pause_task(self.ID)
        elif self.Command == "remove_completed_tasks":
            self.m_remove_completed_tasks()
        elif self.Command == "remove_incomplete_tasks":
            self.m_remove_incomplete_tasks()
        # elif self.Command == "modify_task":
        #     self.m_modify_task()
        elif self.Command == "shutdown_server":
            logging.debug("Shutting down server")
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
        self.m_ReadJobList()
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
    def m_ReadJobList(self):
        logging.debug("Init TCP server: Reading existing jobs from %s", Tasks.WorkData["sTargetDir"])
        self.aFiles = self.get_filepaths(Tasks.WorkData["sTargetDir"])
        for f in self.aFiles:
            self.head,self.tail = os.path.split(f)
            self.head, self.tail = (os.path.splitext(self.tail))
            if self.tail.lower() == ".xml":
                self.tree = ET.ElementTree(file=f)
                self.root = self.tree.getroot()
                Tasks.Jobs[self.root.attrib["ID"]] = dataclasses.c_Task(self.root.attrib["ID"])
                Tasks.Jobs[self.root.attrib["ID"]].state = self.root.attrib["state"]
                Tasks.Jobs[self.root.attrib["ID"]].active = self.root.attrib["active"]

                self.bIsActive = True
                self.CopiedFiles = 0
                for file in self.root.find("FileList").findall("File"):
                    Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]] = dataclasses.c_file()

                    Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]].copied = self.StringToBool(file.attrib["copied"])
                    if file.attrib["copied"] == False:
                        self.bIsActive = False
                    else:
                        Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]].progress = 100.0
                        self.CopiedFiles += 1
                    Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]].delete = self.StringToBool(file.attrib["delete"])
                    Tasks.Jobs[self.root.attrib["ID"]].filelist[file.attrib["file"]].uploaded = self.StringToBool(file.attrib["uploaded"])

                Tasks.Jobs[self.root.attrib["ID"]].active = self.bIsActive
                Tasks.Jobs[self.root.attrib["ID"]].progress = (self.CopiedFiles/len(Tasks.Jobs[self.root.attrib["ID"]].filelist))*100
        for f in Tasks.Jobs:
            self.aIncompleteFiles = 0
            for file in Tasks.Jobs[f].filelist:
                if Tasks.Jobs[f].filelist[file].copied == False:
                    self.aIncompleteFiles += 1
            logging.debug("Loading task:[%s] %s : %s files. %s Incomplete", Tasks.Jobs[f].active, f, len(Tasks.Jobs[f].filelist), self.aIncompleteFiles)

    def run(self):
        # Port 0 means to select an arbitrary unused port
        HOST, PORT = "localhost", 9090
        server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
        ip, port = server.server_address
        # Start a thread with the server -- that thread will then start one
        # more thread for each request
        server_thread = threading.Thread(target=server.serve_forever)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()
        for i in range(self.dict_WorkData["Line_Managers"]):
            self.LineManager = workers.c_LineCopyManager(Tasks, "Line_manager_" + str(i))
            self.LineManager.start()
            self.aLineManagers.append(self.LineManager)

        logging.debug("Server loop running in thread:%s", server_thread.name)
        while Tasks.shutdown == False:
            message = "waiting_until_shutdown_command"

        for p in self.aLineManagers:
            Tasks.task_queue.put(None)

        for p in self.aLineManagers:
            p.join()
        server.shutdown()



