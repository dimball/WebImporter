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
import stat
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

from xml.dom import minidom


#The copy worker is a class that runs constantly on a thread. If there is something in the task queue, then it will do it,
class c_file():
    def __init__(self,size):
        self.progress = 0
        self.copied = False
        self.delete = False
        self.uploaded = False
        self.size = size
class c_Task():
    def __init__(self,ID):
        self.TaskID = str(ID)
        self.state = "ready"
        self.active = False
        self.progress = 0
        self.workerlist = {}
        self.filelist = {}
    def GetCurrentProgress(self):
        self.CopiedFiles = 0
        for f in self.filelist:
            if self.filelist[f].copied == True:
                self.CopiedFiles += 1

        self.progress = (self.CopiedFiles/len(self.filelist))*100
        return self.progress
    def IsComplete(self):
        if self.GetCurrentProgress() == 100.0:
            return True
        else:
            return False
    def ResetFileStatus(self):
        for f in self.filelist:
            self.filelist[f].copied = False
            self.filelist[f].delete = False
            self.filelist[f].uploaded = False

        self.GetCurrentProgress()

class c_HelperFunctions():
    def remove_readonly(self,fn, path, excinfo):
        try:
            os.chmod(path, stat.S_IWRITE)
            fn(path)
        except Exception as exc:
            print("Skipped:", path, "because:\n", exc)
    def indent(self, elem, level=0):
      i = "\n" + level*"  "
      if len(elem):
        if not elem.text or not elem.text.strip():
          elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
          elem.tail = i
        for elem in elem:
          self.indent(elem, level+1)
        if not elem.tail or not elem.tail.strip():
          elem.tail = i
      else:
        if level and (not elem.tail or not elem.tail.strip()):
          elem.tail = i
    def WriteJob(self,dict_workdata, dict_Jobs, ID):
        TaskObject = dict_Jobs[ID]
        Task = ET.Element("Task")
        Task.set("ID",str(ID))
        Task.set("state",str(TaskObject.state))
        Task.set("active",str(TaskObject.active))
        FileList = ET.SubElement(Task,"FileList")
        for file in TaskObject.filelist:
            data = TaskObject.filelist[file]
            fileItem = ET.SubElement(FileList,"File")
            fileItem.set("file",file)
            fileItem.set("copied",str(data.copied))
            fileItem.set("delete",str(data.delete))
            fileItem.set("uploaded",str(data.uploaded))
            fileItem.set("size",str(data.size))
        self.indent(Task)
        Tree = ET.ElementTree(Task)
        Tree.write(dict_workdata["sTargetDir"] + str(ID) + "/" + str(ID) + ".xml", xml_declaration=True, encoding='utf-8', method="xml")
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
    def FileExpand(self,ID,Payload):
        self.ID = ID
        self.Payload = Payload
        self.FileList = {}
        for FileObj in self.Payload:
            if FileObj["type"] == "folder":
                self.aFilePaths = self.get_filepaths(FileObj["data"])
                for f in self.aFilePaths:
                    self.path = os.path.normpath(f)
                    self.FileList[self.path] = c_file(os.path.getsize(self.path))
            elif FileObj["type"] == "file":
                self.path = os.path.normpath(FileObj["data"])
                self.FileList[self.path] = c_file(os.path.getsize(self.path))

        return self.FileList
#threads
class c_FileExpander(threading.Thread,c_HelperFunctions):
    def __init__(self, dict_Jobs, ID, Payload):
        threading.Thread.__init__(self)
        self.dict_Jobs = dict_Jobs
        self.ID = str(ID)
        self.Payload = Payload
    def run(self):
        self.dict_Jobs[self.ID].state = "busy"
        self.dict_Jobs[self.ID].filelist = self.FileExpand(self.ID,self.Payload)
        self.dict_Jobs[self.ID].state = "ready"
class c_FileProgressMonitor(threading.Thread):
    def __init__(self,srcfile, tgtfile, dict_Jobs, ID,worker_name):
        threading.Thread.__init__(self)
        self.worker_name = worker_name
        self.ID = ID
        self.dict_Jobs = dict_Jobs
        self.srcfile = srcfile
        self.tgtfile = tgtfile
        self.size = os.path.getsize(self.srcfile)
        if self.dict_Jobs[self.ID].filelist[self.srcfile].size != self.size:
            logging.debug("Original size is different from current size. Updating entry")
            self.dict_Jobs[self.ID].filelist[self.srcfile].size = self.size

        self.name = "Progress monitor"

        #if self.worker_name in self.dict_Jobs[self.ID].workerlist == False:
        #    print("resetting worklist")
        self.dict_Jobs[self.ID].workerlist[self.worker_name] = {}

        self.dict_Jobs[self.ID].workerlist[self.worker_name][self.srcfile] = 0.0
    def run(self):
        while os.path.exists(self.tgtfile) == False:
            #logging.debug("waiting for file to exist at target destination")
            time.sleep(0.05)
#        logging.debug("File check started:%s", self.srcfile)

        self.currentSize = os.path.getsize(self.tgtfile)
        #self.dict_Jobs[self.ID].workerlist[self.worker_name]["progress"] = self.ProgressData["progress"]

        while self.currentSize < self.size:
            self.currentSize = os.path.getsize(self.tgtfile)
            self.dict_Jobs[self.ID].workerlist[self.worker_name][self.srcfile] = ((self.currentSize/self.size)*100.0)
            self.dict_Jobs[self.ID].filelist[self.srcfile].progress = ((self.currentSize/self.size)*100.0)
            print(self.dict_Jobs[self.ID].filelist[self.srcfile].progress)
            if self.dict_Jobs[self.ID].active == False:
                logging.debug("Aborted file check")
                break
            time.sleep(0.01)

        #if it has broken out of the loop then it should be 100.0

        self.dict_Jobs[self.ID].workerlist[self.worker_name][self.srcfile] = 100.0
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
                    if self.worker_name in self.dict_Jobs[self.ID].workerlist == False:
                        self.dict_Jobs[self.ID].workerlist[self.worker_name] = {}

                    self.pmon = c_FileProgressMonitor(self.srcfile,self.dstfile, self.dict_Jobs, self.ID, self.worker_name)
                    self.pmon.start()
                    self.copyFile(self.srcfile,self.dstfile)
                    self.dict_Jobs[self.ID].filelist[self.srcfile].copied = True

                    self.pmon.join()
                    self.worker_queue.task_done()
                else:
                    print("%s does not exist",self.srcfile)

                self.result_queue.put(self.next_task)
                if self.dict_Jobs[self.ID].active == False:
                    logging.debug("Aborting (setting task to inactive, breaking out of thread)")
                    break

        logging.debug("Shutting down copy worker")
        return
class c_PreTaskWorker(threading.Thread,c_HelperFunctions):
    def __init__(self,Data):
        threading.Thread.__init__(self)
        self.Data = Data
        self.dict_Data = self.Data["dict_WorkData"]
        self.dict_Jobs = self.Data["dict_Jobs"]
        self.ID = self.Data["ID"]
        self.command = self.Data["command"]

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
            if self.dict_Jobs[self.ID].active == False
                self.fullpath = self.dict_Data["sTargetDir"] + self.ID
                if os.path.isdir(self.fullpath):
                    shutil.rmtree(os.path.normpath(self.fullpath),onerror=self.remove_readonly)
                else:
                    logging.debug("%s is not a folder",self.fullpath)

                del self.dict_Jobs[self.ID]

    def m_modify_task(self):
        self.incomingFiles = self.FileExpand(self.ID,self.Payload)
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

# class c_RemoveWorker(threading.Thread, c_HelperFunctions):
#     def __init__(self,dict_Data,ID):
#         threading.Thread.__init__(self)
#         self.dict_Data = dict_Data
#         self.ID = ID
#     def run(self):

class c_LineCopyManager(threading.Thread,c_HelperFunctions):
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
            logging.debug("[%s] Processing task:[%s] ==> %s",self.manager_name,self.ID,self.dict_Data["sTargetDir"])
            self.files = []

            for file in self.next_task.filelist:
                print(file, self.next_task.filelist[file].copied)
                if self.next_task.filelist[file].copied == False:
                    self.files.append(file)
                    self.CopyQueueItem = {}
                    self.CopyQueueItem["file"] = file
                    self.head,self.tail = os.path.splitdrive(file)
                    self.dstfile = os.path.normpath(self.dict_Data["sTargetDir"] + self.ID + "/" + self.tail)
                    if not os.path.exists(os.path.dirname(self.dstfile)):
                        os.makedirs(os.path.dirname(self.dstfile))

                    #self.TotalSize += os.path.getsize(self.files[i])

                    self.CopyQueueItem["id"] = self.ID
                    self.worker_queue.put(self.CopyQueueItem)
                    self.large_worker_queue.put(self.CopyQueueItem)

            print(len(self.files))
            if len(self.files) > 0:

                self.numLargeFiles = 0
                self.numSmallFiles = 0
                for file in self.next_task.filelist:
                    if (self.next_task.filelist[file].size > self.dict_Data["large_file_threshold"]*1024*1024):
                        self.numLargeFiles += 1
                    else:
                        self.numSmallFiles += 1

                self.aCopyWorkers = []

                if self.numLargeFiles > 0:
                    #dynamically adjust number of copy workers depending on the average size of files?
                    self.CopyWorkerName = "[" + self.manager_name + "] Large_Copy Worker"
                    self.dict_Jobs[self.ID].workerlist[self.CopyWorkerName] = {}
                    self.LargeCopyWorkerProcess = c_CopyWorker(self.dict_Jobs, self.dict_Data,self.CopyWorkerName,self.large_worker_queue,self.result_queue, ">")
                    self.LargeCopyWorkerProcess.start()

                #copy workers that work on files less than 5mb
                if self.numSmallFiles > 0:
                    self.NumSmallCopyWorkers = self.dict_Data["CopyWorkers_Per_Line"]
                    if self.numSmallFiles < self.NumSmallCopyWorkers:
                        self.NumSmallCopyWorkers = self.numSmallFiles

                    for i in range(self.NumSmallCopyWorkers):
                        self.CopyWorkerName = "[" + self.manager_name + "] Copy Worker_" + str(i)
                        self.dict_Jobs[self.ID].workerlist[self.CopyWorkerName] = {}
                        self.CopyWorkerProcess = c_CopyWorker(self.dict_Jobs, self.dict_Data,self.CopyWorkerName,self.worker_queue,self.result_queue, "<")
                        self.CopyWorkerProcess.start()
                        self.aCopyWorkers.append(self.CopyWorkerProcess)





                self.OldQueueSize = 0
                while True:
                    self.CurrentQueueSize = self.result_queue.qsize()
                    #print(self.CurrentQueueSize)
                    if self.dict_Jobs[self.ID].active == False:
                        logging.debug("Pausing job:%s ",self.ID)
                        #self.dict_Jobs[self.ID].PauseIndex = self.CurrentQueueSize
                        while not self.worker_queue.empty():
                            self.worker_queue.get()
                        while not self.large_worker_queue.empty():
                            self.worker_queue.get()
                        while not self.result_queue.empty():
                            self.result_queue.get()
                        for p in self.aCopyWorkers:
                            p.join()

                        if self.numLargeFiles > 0:
                            self.LargeCopyWorkerProcess.join()

                        self.task_queue.task_done()

                        break

                    self.dict_Jobs[self.ID].progress = (self.CurrentQueueSize/len(self.files))*100
                    if self.CurrentQueueSize > self.OldQueueSize:
                        self.OldQueueSize = self.CurrentQueueSize
                        #write out data each time a file has been processed
                        self.WriteJob(self.dict_Data,self.dict_Jobs,self.ID)


                    if self.dict_Jobs[self.ID].progress >= 100.0:
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

                    self.large_worker_queue.put(None)
                    time.sleep(1)

                    for p in self.aCopyWorkers:
                        p.join()
                    if self.numLargeFiles > 0:
                        self.LargeCopyWorkerProcess.join()

                    self.task_queue.task_done()
                    #write job status xml
                    self.WriteJob(self.dict_Data,self.dict_Jobs,self.ID)
                    logging.debug("Finished job")
        logging.debug("Shutting down line manager")
class TCPServer(c_HelperFunctions):
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

        self.Data = {}
        self.Data["dict_WorkData"] = dict_WorkData
        self.Data["dict_Jobs"] = self.dict_Jobs

        for i in range(dict_WorkData["Line_Managers"]):
            self.LineManager = c_LineCopyManager(self.task_queue, self.dict_Jobs, self.dict_WorkData, "Line_manager_" + str(i))
            self.LineManager.start()
            self.aLineManagers.append(self.LineManager)
        self.buffersize = 1024*1024*5
        self.m_ReadJobList()
        self.run()
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
                tree = ET.ElementTree(file=f)
                self.task = tree.getroot()
                self.dict_Jobs[self.task.attrib["ID"]] = c_Task(self.task.attrib["ID"])
                self.dict_Jobs[self.task.attrib["ID"]].state = self.task.attrib["state"]
                self.dict_Jobs[self.task.attrib["ID"]].active = self.task.attrib["active"]

                self.bIsActive = True
                self.CopiedFiles = 0
                for file in self.task.find("FileList").findall("File"):
                    self.dict_Jobs[self.task.attrib["ID"]].filelist[file.attrib["file"]] = c_file(int(file.attrib["size"]))

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
        self.dict_Jobs[str(self.ID)] = c_Task(self.ID)
        time.sleep(0.1)
        self.Data["ID"] = str(self.ID)
        self.Data["command"]="expand_files"
        self.Data["payload"]=self.Payload

        self.FileExpander = c_PreTaskWorker(self.Data)
        self.FileExpander.start()
        # self.FileExpander = c_FileExpander(self.dict_Jobs,self.ID, self.Payload)
        # self.FileExpander.start()
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
        for pl,job in self.dict_Jobs.items():
            if self.dict_Jobs[pl].progress == 100.0:
                self.DeleteList.append(pl)
                self.Data["command"] = "restart_task"
                self.Data["ID"] = pl
                self.Data["task_queue"] = self.task_queue
                RemoveWorker = c_PreTaskWorker(self.Data)
                RemoveWorker.start()
                # RemoveWorkerProcess = c_RemoveWorker(dict_WorkData,self.ID)
                # RemoveWorkerProcess.start()
                logging.debug("Removing completed Job:%s",pl)
                RemoveWorker.join()

                # RemoveWorkerProcess.join()
            else:
                logging.debug("Job is not complete yet:%s",pl)

        for ID in self.DeleteList:
            del self.dict_Jobs[ID]
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
                for p in self.aLineManagers:
                    p.join()
                break
            self.client.close()
if __name__ == '__main__':
    dict_WorkData = {}
    dict_WorkData["Line_Managers"] = 1
    dict_WorkData["CopyWorkers_Per_Line"] = 10
    dict_WorkData["sTargetDir"] = "d:/destination/"
    dict_WorkData["serverport"] = 9090
    dict_WorkData["Num_TCP_port"] = 5
    dict_WorkData["large_file_threshold"] = 5
    TCPServer(dict_WorkData)
