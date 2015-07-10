import threading
import queue
import logging
import socket
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
import os
import uuid
import shutil
import json
import time
#The copy worker is a class that runs constantly on a thread. If there is something in the task queue, then it will do it,
class c_FileProgressMonitor(threading.Thread):
    def __init__(self,srcfile, tgtfile, dict_Jobs, ID,worker_name):
        threading.Thread.__init__(self)
        self.worker_name = worker_name
        self.ID = ID
        self.dict_Jobs = dict_Jobs
        self.srcfile = srcfile
        self.tgtfile = tgtfile
        self.size = os.path.getsize(self.srcfile)
        self.name = "Progress monitor"
        self.dict_Jobs[self.ID].fileprogress[self.worker_name][self.srcfile] = 0.0
    def run(self):

        while os.path.exists(self.tgtfile) == False:
            #logging.debug("waiting for file to exist at target destination")
            time.sleep(0.05)
#        logging.debug("File check started:%s", self.srcfile)

        self.currentSize = os.path.getsize(self.tgtfile)
        #self.dict_Jobs[self.ID].fileprogress[self.worker_name]["progress"] = self.ProgressData["progress"]

        while self.currentSize < self.size:
            self.currentSize = os.path.getsize(self.tgtfile)
            self.dict_Jobs[self.ID].fileprogress[self.worker_name][self.srcfile] = ((self.currentSize/self.size)*100.0)
            if self.dict_Jobs[self.ID].active == False:
                logging.debug("Aborted file check")
                break
            time.sleep(0.01)
        #if it has broken out of the loop then it should be 100.0

        self.dict_Jobs[self.ID].fileprogress[self.worker_name][self.srcfile] = 100.0
#        logging.debug("File check complete:%s", self.srcfile)
class c_CopyWorker(threading.Thread):
    def __init__(self,dict_Jobs, dict_Data, worker_name,worker_queue,result_queue, operand):
        threading.Thread.__init__(self)
        self.operand = operand
        self.name = worker_name
        self.dict_Jobs = dict_Jobs
        self.dict_Data = dict_Data
        self.worker_name = worker_name
        self.worker_queue = worker_queue
        self.result_queue = result_queue
        logging.debug("Copy worker Started")
    def copyFile(self,src, dst, buffer_size=10485760, perserveFileDate=True):
        '''
        Copies a file to a new location. Much faster performance than Apache Commons due to use of larger buffer
        @param src:    Source File
        @param dst:    Destination File (not file path)
        @param buffer_size:    Buffer size to use during copy
        @param perserveFileDate:    Preserve the original file date
        '''
        #    Check to make sure destination directory exists. If it doesn't create the directory
        self.dstParent, self.dstFileName = os.path.split(dst)
        if(not(os.path.exists(self.dstParent))):
            os.makedirs(self.dstParent)

        #    Optimize the buffer for small files
        self.buffer_size = min(buffer_size,os.path.getsize(src))
        if(buffer_size == 0):
            buffer_size = 1024

        if shutil._samefile(src, dst):
            raise shutil.Error("`%s` and `%s` are the same file" % (src, dst))
        for fn in [src, dst]:
            try:
                st = os.stat(fn)
            except OSError:
                # File most likely does not exist
                pass
            else:
                # XXX What about other special files? (sockets, devices...)
                if shutil.stat.S_ISFIFO(st.st_mode):
                    raise shutil.SpecialFileError("`%s` is a named pipe" % fn)
        with open(src, 'rb') as fsrc:
            with open(dst, 'wb') as fdst:
                shutil.copyfileobj(fsrc, fdst, buffer_size)

        if(perserveFileDate):
            shutil.copystat(src, dst)
    def run(self):
        while True:
            self.next_task = self.worker_queue.get()
            if self.next_task == None:
                logging.debug("breaking out of thread")
                break

            self.bContinue = False
            self.ID = self.next_task["id"]
            self.srcfile = self.next_task["file"]
            if self.operand == ">":
                if os.path.getsize(self.srcfile) > (1024*1024*self.dict_Data["large_file_threshold"]):
                    self.bContinue = True
            elif self.operand == "<":
                if os.path.getsize(self.srcfile) < (1024*1024*self.dict_Data["large_file_threshold"]):
                    self.bContinue = True

            if self.bContinue == True:
                self.head,self.tail = os.path.splitdrive(self.srcfile)
                self.dstfile = os.path.normpath(self.dict_Data["sTargetDir"] + self.ID + "/" + self.tail)


                if os.path.isfile(self.srcfile):
                    #logging.debug('%s ==> %s', self.srcfile, (self.dstfile))
                    self.pmon = c_FileProgressMonitor(self.srcfile,self.dstfile, self.dict_Jobs, self.ID, self.worker_name)
                    self.pmon.start()
                    self.copyFile(self.srcfile,self.dstfile)
                    self.pmon.join()
                    self.worker_queue.task_done()
                else:
                    print("%s does not exist",self.srcfile)

                self.result_queue.put(self.next_task)
                if self.dict_Jobs[self.ID].active == False:
                    logging.debug("Aborting (setting task to inactive, breaking out of thread) paused at:%s",str(self.dict_Jobs[self.ID].PauseIndex))
                    break

        logging.debug("Shutting down copy worker")
        return
class c_RemoveWorker(threading.Thread):
    def __init__(self,dict_Data,ID):
        threading.Thread.__init__(self)
        self.dict_Data = dict_Data
        self.ID = ID
    def run(self):
        if os.path.exists(self.dict_Data["sTargetDir"] + self.ID):
            shutil.rmtree(self.dict_Data["sTargetDir"] + self.ID)

class c_LineCopyManager(threading.Thread):
    def __init__(self,task_queue,dict_Jobs,dict_Data, manager_name):
        threading.Thread.__init__(self)
        self.task_queue = task_queue
        self.dict_Jobs = dict_Jobs
        self.dict_Data = dict_Data
        self.manager_name = manager_name
        self.name = self.manager_name
        self.worker_queue = queue.Queue()
        self.large_worker_queue = queue.Queue()
        self.result_queue = queue.Queue()
    def run(self):
        logging.debug("Line manager started:")
        while True:
            self.next_task = self.task_queue.get()
            if self.next_task is None:
                break

            self.ID = self.next_task.TaskID
            self.Payload = self.next_task.Payload
            logging.debug("[%s] Processing task:[%s] ==> %s",self.manager_name,self.ID,self.dict_Data["sTargetDir"])
            self.files = []
            for FileObj in self.Payload:
                if FileObj["type"] == "folder":
                    self.aFilePaths = self.get_filepaths(FileObj["data"])
                    for f in self.aFilePaths:
                        self.files.append(f)
                elif FileObj["type"] == "file":
                    self.files.append(FileObj["data"])

            for i in range(self.dict_Jobs[self.ID].PauseIndex, len(self.files)):
                self.CopyQueueItem = {}
                self.CopyQueueItem["file"] = self.files[i]
                self.head,self.tail = os.path.splitdrive(self.files[i])
                self.dstfile = os.path.normpath(self.dict_Data["sTargetDir"] + self.ID + "/" + self.tail)
                if not os.path.exists(os.path.dirname(self.dstfile)):
                    os.makedirs(os.path.dirname(self.dstfile))

                #self.TotalSize += os.path.getsize(self.files[i])

                self.CopyQueueItem["id"] = self.ID
                self.worker_queue.put(self.CopyQueueItem)
                self.large_worker_queue.put(self.CopyQueueItem)

            self.aCopyWorkers = []
            #dynamically adjust number of copy workers depending on the average size of files?
            self.CopyWorkerName = "[" + self.manager_name + "] Large_Copy Worker"
            self.dict_Jobs[self.ID].fileprogress[self.CopyWorkerName] = {}
            self.LargeCopyWorkerProcess = c_CopyWorker(self.dict_Jobs, self.dict_Data,self.CopyWorkerName,self.large_worker_queue,self.result_queue, ">")
            self.LargeCopyWorkerProcess.start()

            #copy workers that work on files less than 5mb
            for i in range(self.dict_Data["CopyWorkers_Per_Line"]):
                self.CopyWorkerName = "[" + self.manager_name + "] Copy Worker_" + str(i)
                self.dict_Jobs[self.ID].fileprogress[self.CopyWorkerName] = {}
                self.CopyWorkerProcess = c_CopyWorker(self.dict_Jobs, self.dict_Data,self.CopyWorkerName,self.worker_queue,self.result_queue, "<")
                self.CopyWorkerProcess.start()
                self.aCopyWorkers.append(self.CopyWorkerProcess)

            while True:
                self.CurrentQueueSize = self.result_queue.qsize()
                #print(self.CurrentQueueSize)
                if self.dict_Jobs[self.ID].active == False:
                    logging.debug("Pausing job:% at index: %s",self.ID,self.dict_Jobs[self.ID].PauseIndex)
                    self.dict_Jobs[self.ID].PauseIndex = self.CurrentQueueSize
                    while not self.worker_queue.empty():
                        self.worker_queue.get()
                    while not self.large_worker_queue.empty():
                        self.worker_queue.get()
                    while not self.result_queue.empty():
                        self.result_queue.get()
                    for p in self.aCopyWorkers:
                        p.join()
                    self.LargeCopyWorkerProcess.join()
                    self.task_queue.task_done()
                    break

                self.dict_Jobs[self.ID].progress = ((self.dict_Jobs[self.ID].PauseIndex+self.CurrentQueueSize)/len(self.files))*100
                if self.dict_Jobs[self.ID].progress >= 100.0:
                    self.dict_Jobs[self.ID].PauseIndex = 0
                    self.dict_Jobs[self.ID].progress = 100.0
                    self.dict_Jobs[self.ID].active = False

                    while not self.result_queue.empty():
                        self.result_queue.get()

                    break

            if self.dict_Jobs[self.ID].progress >= 100.0:
                self.dict_Jobs[self.ID].active = False
                self.dict_Jobs[self.ID].progress = 100.0

                while not self.result_queue.empty():
                    self.result_queue.get()

                for p in self.aCopyWorkers:
                    self.worker_queue.put(None)

                time.sleep(1)

                for p in self.aCopyWorkers:
                    p.join()
                self.task_queue.task_done()
                logging.debug("Finished job")
        logging.debug("Shutting down line manager")
    def get_filepaths(self,directory):
        """
        This function will generate the file names in a directory
        tree by walking the tree either top-down or bottom-up. For each
        directory in the tree rooted at directory top (including top itself),
        it yields a 3-tuple (dirpath, dirnames, filenames).
        """
        file_paths = []  # List which will store all of the full filepaths.

        # Walk the tree.
        for root, directories, files in os.walk(directory):
            for filename in files:
                # Join the two strings in order to form the full filepath.
                filepath = os.path.join(root, filename)
                file_paths.append(filepath)  # Add it to the list.

        return file_paths  # Self-explanatory.
class c_Task():
    def __init__(self,ID,Payload):
        self.TaskID = str(ID)
        self.Payload = Payload
        self.active = False
        self.progress = 0
        self.PauseIndex = 0
        self.fileprogress = {}




class TCPServer():
    def __init__(self,dict_WorkData):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('localhost',(dict_WorkData["serverport"])))
        self.socket.listen(dict_WorkData["Num_TCP_port"])
        self.task_queue = queue.Queue()
        self.dict_Jobs = {}
        self.dict_WorkData = dict_WorkData
        self.dict_fileprogress = {}
        self.aLineManagers = []
        for i in range(dict_WorkData["Line_Managers"]):
            self.LineManager = c_LineCopyManager(self.task_queue, self.dict_Jobs, self.dict_WorkData, "Line_manager_" + str(i))
            self.LineManager.start()
            self.aLineManagers.append(self.LineManager)
        self.buffersize = 1024*1024*5
        self.run()
    def run(self):
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
                logging.debug("Creating task : %s",str(self.ID))
                self.dict_Jobs[str(self.ID)] = c_Task(self.ID,self.Payload)
                self.Output["status"] = str(self.ID)
                self.client.send(bytes(json.dumps(self.Output) ,'utf-8'))
            elif self.Command == "start_task":
                if self.Payload in self.dict_Jobs:
                    if self.dict_Jobs[self.Payload].active == False:
                        self.dict_Jobs[self.Payload].active = True
                        self.dict_Jobs[self.Payload].progress = -1
                        self.dict_Jobs[self.Payload].fileprogress = {}
                        self.task_queue.put(self.dict_Jobs[self.Payload])
                    else:
                        self.Output["status"] = "Task already started"
                        self.client.send(bytes(json.dumps(self.Output),'utf-8'))
                else:
                    self.Output["status"] = "Task does not exist"
                    self.client.send(bytes(self.Output,'utf-8'))
            elif self.Command == "resume_job":
                if self.Payload in self.dict_Jobs:
                    if self.dict_Jobs[self.Payload].active == False:
                        logging.debug("Resuming Job : %s",str(self.Payload))
                        self.dict_Jobs[self.Payload].active = True
                        self.dict_Jobs[self.Payload]["fileprogress"] = {}
                        self.task_queue.put(self.dict_Jobs[self.Payload])
                    else:
                        self.Output["status"] = "Cannot resume as it has not been paused"
                        self.client.send(bytes(json.dumps(self.Output),'utf-8'))
                else:
                    self.Output["status"] = "Cannot resume as task does not exist"
                    self.client.send(bytes(json.dumps(self.Output),'utf-8'))
            elif self.Command == "status":
                if self.Payload in self.dict_Jobs:
                    if self.dict_Jobs[self.Payload].active:
                        if self.dict_Jobs[self.Payload].progress == -1:
                            self.Output["status"] = "Not Started"
                            self.Output["worker"] = []
                        else:
                            self.Output["status"] = self.dict_Jobs[self.Payload].progress
                            self.Output["worker"] = {}
                            for worker,file in self.dict_Jobs[self.Payload].fileprogress.items():
                                self.Output["worker"][worker] = [] #job
                                self.AddFile = {}
                                for file, progress in file.items():
                                    self.Output["worker"][worker].append({file:progress})
                                    if progress < 100.0:
                                        self.AddFile[file] = progress
                                self.dict_Jobs[self.Payload].fileprogress[worker] = self.AddFile
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
            elif self.Command == "get_tasks":
                self.aJobs = []
                for key,value in self.dict_Jobs.items():
                    self.aJobs.append(key)
                self.Output["job"] = self.aJobs
                self.client.send(bytes(json.dumps(self.Output),'utf-8'))
            elif self.Command == "get_active_tasks":
                self.aJobs = []
                self.JobIDString = ""
                for key,value in self.dict_Jobs.items():
                    if value.active == True:
                        self.aJobs.append(key)
                self.Output["job"] = self.aJobs
                self.client.send(bytes(json.dumps(self.Output),'utf-8'))
            elif self.Command == "pause_job":
                self.dict_Jobs[self.Payload].active = False
                self.Output["status"] = "Paused job " + self.Payload
                self.client.send(bytes(json.dumps(self.Output),'utf-8'))
            elif self.Command == "remove_completed_tasks":
                self.DeleteList = []
                for pl,job in self.dict_Jobs.items():
                    if self.dict_Jobs[pl].progress == 100.0:
                        self.DeleteList.append(pl)
                        logging.debug("Removing completed Job:%s",pl)
                    else:
                        logging.debug("Job is not complete yet:%s",pl)

                for ID in self.DeleteList:
                    del self.dict_Jobs[ID]
            elif self.Command == "remove_incomplete_tasks":
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

            elif self.Command == "modify_task":
                self.ID = self.Payload["ID"]
                self.Payload = self.Payload["Payload"]
                print(self.dict_Jobs[self.ID].active)
                if self.dict_Jobs[self.ID].active == False:
                    OldPayload = self.dict_Jobs[self.ID].Payload
                    self.dict_Jobs[self.ID].PauseIndex = 0
                    self.dict_Jobs[self.ID].Payload = self.Payload
                    print("Task [" + self.ID + "] has been modified:")
                    #send remove tree to a seperate thread
                    RemoveWorkerProcess = c_RemoveWorker(dict_WorkData,self.ID)
                    RemoveWorkerProcess.start()
                    RemoveWorkerProcess.join()


                    #some logic has to happen here to either remove the previous content, or do a smart diff to check
                    #if that are in the new self.Payload has been copied already and skip those. Other files that are not in the
                    #new payload must be deleted.
            elif self.Command == "shutdown_server":
                logging.debug("Shutting down server")
                for p in self.aLineManagers:
                    self.task_queue.put(None)
                time.sleep(1)
                for p in self.aLineManagers:
                    p.join()
                break



            self.client.close()
if __name__ == '__main__':
    dict_WorkData = {}
    dict_WorkData["Line_Managers"] = 2
    dict_WorkData["CopyWorkers_Per_Line"] = 10
    dict_WorkData["sTargetDir"] = "d:/destination/"
    dict_WorkData["serverport"] = 9090
    dict_WorkData["Num_TCP_port"] = 5
    dict_WorkData["large_file_threshold"] = 5
    TCPServer(dict_WorkData)
