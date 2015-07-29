import os
import shutil
import time
import common as hfn
import threading
import queue
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )


from websocket import create_connection
import json
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
    def copyFile(self,sourcefile, destinationfile,filesize, buffer_size=10485760, perserveFileDate=True):
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
        self.buffer_size = min(buffer_size,filesize)
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

    def customCopyFile(self,sourcefile,destinationfile,filesize,dict_Jobs,ID,worker_name,buffer_size=16, perserveFileDate=True):
        buffer_size = 1024*1024*buffer_size
        try:
            fsrc = open(sourcefile, 'rb')
            fdst = open(destinationfile, 'wb')
            self.buffer_size = min(buffer_size,filesize)
            if(buffer_size == 0):
                buffer_size = 1024
            count = 0
            while True:
                # Read blocks of size 2**20 = 1048576
                # Depending on system may require smaller
                #  or could go larger...
                #  check your fs's max buffer size
                buf = fsrc.read(buffer_size)
                if not buf:
                    dict_Jobs[ID].workerlist[worker_name][sourcefile] = 100.0
                    break
                if dict_Jobs[ID].active == False:
                    logging.debug("Aborting file copy:%s", sourcefile)
                    break
                fdst.write(buf)
                count += len(buf)
                dict_Jobs[ID].workerlist[worker_name][sourcefile] = (count/filesize)*100
        finally:
            if fdst:
                fdst.close()
            if fsrc:
                fsrc.close()

            if perserveFileDate:
                shutil.copystat(sourcefile, destinationfile)
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
                if self.dict_Jobs[self.ID].filelist[self.srcfile].size > (1024*1024*self.dict_Data["large_file_threshold"]):
                    self.bContinue = True
            elif self.operand == "<":
                if self.dict_Jobs[self.ID].filelist[self.srcfile].size < (1024*1024*self.dict_Data["large_file_threshold"]):
                    self.bContinue = True

            if self.bContinue:
                self.head,self.tail = os.path.splitdrive(self.srcfile)
                self.dstfile = os.path.normpath(self.dict_Data["sTargetDir"] + self.ID + "/" + self.tail)


                if os.path.isfile(self.srcfile):
                    #logging.debug('%s ==> %s', self.srcfile, (self.dstfile))
                    if self.worker_name in self.dict_Jobs[self.ID].workerlist == False:
                        self.dict_Jobs[self.ID].workerlist[self.worker_name] = {}
                    #self.copyFile(self.srcfile,self.dstfile,self.dict_Jobs[self.ID].filelist[self.srcfile].size)
                    self.customCopyFile(self.srcfile,self.dstfile,self.dict_Jobs[self.ID].filelist[self.srcfile].size, self.dict_Jobs, self.ID, self.worker_name)
                    if self.dict_Jobs[self.ID].active == False:

                        self.worker_queue.task_done()
                        break

                    self.dict_Jobs[self.ID].filelist[self.srcfile].copied = True
                    #self.pmon.join()
                    self.worker_queue.task_done()
                else:
                    logging.debug("%s does not exist:%s",self.srcfile)

                self.result_queue.put(self.next_task)
                if not self.dict_Jobs[self.ID].active:
                    logging.debug("Aborting (setting task to inactive, breaking out of thread)")
                    break

        logging.debug("Shutting down copy worker")
        return
class c_LineCopyManager(threading.Thread,hfn.c_HelperFunctions):
    def __init__(self,Tasks, manager_name):

        threading.Thread.__init__(self)
        self.Tasks = Tasks
        self.task_queue = Tasks.task_queue
        self.dict_Jobs = Tasks.Jobs
        self.dict_Data = Tasks.WorkData
        self.manager_name = manager_name
        self.name = self.manager_name
        self.worker_queue = queue.Queue()
        self.large_worker_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.Threads = {}
    def Shutdown(self):
        self.aThreads = []
        for thread in self.Threads:
            self.aThreads.append(thread)

        for thread in self.aThreads:
            if thread in self.Threads:
                while self.Threads[thread].isAlive():
                    continue

                del self.Threads[thread]

    def m_SendToWebsocket(self, data):
        print("sending to websocket")
        self.websocket = yield from websockets.connect('ws://localhost:8765/')
        yield from self.websocket.send("test")
        #greeting = yield from websocket.recv()
        #print("< {}".format(greeting))
        yield from self.websocket.close()

    def run(self):
        logging.debug("Line manager started:")
        self.ws = create_connection('ws://localhost:8765/')
        while True:
            self.next_task = self.task_queue.get()
            if self.next_task is None:
                logging.debug("Shutting down line manager")
                break

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
                    self.Threads[self.LargeCopyWorkerProcess.name] = (self.LargeCopyWorkerProcess)

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
                        self.Threads[self.CopyWorkerProcess.name] = (self.CopyWorkerProcess)
                self.OldQueueSize = 0
                self.CompleteCounter = 0
                while True:
                    self.CurrentQueueSize = self.result_queue.qsize()

                    if self.dict_Jobs[self.ID].active == False:
                        logging.debug("Pausing job:%s ", self.ID)
                        while not self.worker_queue.empty():
                            self.worker_queue.get()
                        while not self.large_worker_queue.empty():
                            self.large_worker_queue.get()
                        while not self.result_queue.empty():
                            self.result_queue.get()

                        for p in self.aCopyWorkers:
                            p.join()

                        if self.numLargeFiles > 0:
                            self.LargeCopyWorkerProcess.join()
                        self.Shutdown()
                        self.task_queue.task_done()
                        self.WriteJob(self.Tasks, self.ID)
                        break

                    self.CurrentSize = len(self.next_task.filelist)-len(self.files)
                    self.dict_Jobs[self.ID].progress = (self.CurrentSize+self.CurrentQueueSize)/len(self.next_task.filelist)*100


                    self.Payload = {}
                    self.Payload["ID"] = self.ID
                    self.Payload["progress"] = self.dict_Jobs[self.ID].progress

                    if self.CurrentQueueSize > self.OldQueueSize:
                        self.OldQueueSize = self.CurrentQueueSize
                        #send progress to the sync server here?
                        #the tasks processed here are only the local ones.




                        if self.Tasks.syncserver_client.connected:

                            if self.CompleteCounter > 4:
                                self.ws.send(u"hello".encode('utf-8'))
                                #self.ws.recv()
                                #self.Tasks.syncserver_client.m_send(self.Tasks.syncserver_client.m_create_data("/syncserver/v1/global/queue/task/set_progress", self.Payload))
                                self.CompleteCounter = 0
                            else:
                                self.CompleteCounter += 1
                        #write out data each time a file has been processed



                    if self.dict_Jobs[self.ID].progress >= 100.0:
                        self.dict_Jobs[self.ID].progress = 100.0
                        self.dict_Jobs[self.ID].active = False
                        #self.Tasks.syncserver_client.m_send(self.Tasks.syncserver_client.m_create_data("/syncserver/v1/global/queue/task/set_progress", self.Payload))

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
                    time.sleep(0.2)

                    for p in self.aCopyWorkers:
                        p.join()

                    if self.numLargeFiles > 0:
                        self.LargeCopyWorkerProcess.join()

                    self.Shutdown()

                    self.task_queue.task_done()
                    #write job status xml
                    self.WriteJob(self.Tasks,self.ID)
                    logging.debug("Finished job")
            else:
                logging.debug("No files in task")
        return