import os
import shutil
import time
import helper as hfn
import fileprogressmonitor as FileMonitor
import threading
import queue
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
class c_PreTaskWorker(threading.Thread,hfn.c_HelperFunctions):
    def __init__(self,Data):
        threading.Thread.__init__(self)
        self.Data = Data
        self.dict_Data = self.Data.WorkData
        self.dict_Jobs = self.Data.Jobs
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

                    self.pmon = FileMonitor.c_FileProgressMonitor(self.srcfile,self.dstfile, self.dict_Jobs, self.ID, self.worker_name)
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
class c_LineCopyManager(threading.Thread,hfn.c_HelperFunctions):
    def __init__(self,Tasks, manager_name):
        threading.Thread.__init__(self)
        self.task_queue = Tasks.task_queue
        self.dict_Jobs = Tasks.Jobs
        self.dict_Data = Tasks.WorkData
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
            if self.next_task.active == True:
                self.ID = self.next_task.TaskID
                logging.debug("[%s] Processing task:[%s] ==> %s",self.manager_name,self.ID,self.dict_Data["sTargetDir"])
                self.files = []

                for file in self.next_task.filelist:
                    if self.next_task.filelist[file].copied == False:
                        self.files.append(file)
                        self.CopyQueueItem = {}
                        self.CopyQueueItem["file"] = file
                        self.head,self.tail = os.path.splitdrive(file)
                        self.dstfile = os.path.normpath(self.dict_Data["sTargetDir"] + self.ID + "/" + self.tail)
                        if not os.path.exists(os.path.dirname(self.dstfile)):
                            os.makedirs(os.path.dirname(self.dstfile))
                        self.CopyQueueItem["id"] = self.ID
                        self.worker_queue.put(self.CopyQueueItem)
                        self.large_worker_queue.put(self.CopyQueueItem)

                if len(self.files) > 0:

                    self.numLargeFiles = 0
                    self.numSmallFiles = 0
                    for file in self.next_task.filelist:
                        if (os.path.getsize(file) > self.dict_Data["large_file_threshold"]*1024*1024):
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