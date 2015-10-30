import os
import shutil
import time
import common as hfn
import threading
import queue
import dataclasses
import logging
import socket
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )

import client as cl
from websocket import create_connection
import json

from vizone.client import init
from vizone.payload.asset import Item
from vizone.payload.series import Series
from vizone.resource.series import create_series
from vizone.payload.program import Program
from vizone.resource.program import create_program
from vizone.payload.collection.members import Members
from vizone.urilist import UriList
from vizone.resource.series import search_seriess

from vizone.resource.series import get_seriess

from vizone.payload.series import SeriesFeed
from vizone.payload.folder import Folder
from vizone.resource.folder import create_folder
from vizone.resource.aggregate_asset import aggregate_asset_ItemSet
from vizone.resource.aggregate_asset import create_aggregate_asset
from vizone.resource.aggregate_asset import get_aggregate_asset_by_atom_id

from vizone.payload.media import Incoming
from vizone.payload.transfer import TransferRequest
from vizone.resource.incoming import get_incomings
from vizone.resource.series import get_series_by_atom_id
from vizone.payload.media.incomingcollection import IncomingCollection
from vizone.payload.series import SeriesFeed
from vizone.net.message_queue import handle
from vizone.vdf import Model
from vizone.payload.metadata.metadataformcollection import MetadataFormCollection

##needs to override the incoming class as it is missing the atomid. Will be fixed in the next version of python One
from vizone.payload.media.incoming import Incoming as _Incoming
from vizone.descriptors import Value
class Incoming(_Incoming):
    atomid = Value('atom:id', str)

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
FTPTasks = None

class c_WebImporterFTPHandler(FTPHandler,hfn.c_HelperFunctions):
    def on_connect(self):
        pass
        # logging.debug("%s:%s connected", self.remote_ip, self.remote_port)

    def on_disconnect(self):
        # do something when client disconnects
        pass

    def on_login(self, username):
        # do something when user login
        pass

    def on_logout(self, username):
        # do something when user logs out
        pass

    def on_file_sent(self, file):
        #this can be used to notify clients/server etc.
        logging.debug("File: %s is sent", file)
        global FTPTasks
        self.ID = self.m_ExtractIDFromPath(file, FTPTasks)
        self.RealPath = self.m_FindPath(self.ID, file,FTPTasks)

        FTPTasks.Jobs[self.ID].filelist[self.RealPath].progress = 75.0
        logging.debug("Setting progress to 75 percent [%s] for file %s", self.ID, self.RealPath)

        #needs to notify of the status to the syncserver
        self.Payload = {}
        self.Payload["ID"] = self.ID
        self.Payload["progress"] = FTPTasks.Jobs[self.ID].filelist[self.RealPath].progress
        self.Payload["file"] = self.RealPath

        FTPTasks.syncserver_progress_client.m_send(self.m_create_data("/syncserver/v1/global/queue/task/file/set_progress", self.Payload))

        for cli in FTPTasks.ProgressClients:
            cli.write_message(self.m_create_data("/client/v1/local/queue/task/file/set_progress", self.Payload))

        self.Payload["progress"] = FTPTasks.Jobs[self.ID].GetCurrentTotalProgress()
        for cli in FTPTasks.ProgressClients:
            cli.write_message(self.m_create_data("/client/v1/local/queue/task/set_progress", self.Payload))
        # do something when a file has been sent
        pass

    def on_file_received(self, file):
        # do something when a file has been received
        pass

    def on_incomplete_file_sent(self, file):
        # do something when a file is partially sent
        pass

    def on_incomplete_file_received(self, file):
        # remove partially uploaded files
        import os
        os.remove(file)



class c_ftpServer(threading.Thread):
    def __init__(self, Tasks):
        threading.Thread.__init__(self)
        self.Tasks = Tasks
        self.start()

    def run(self):
        self.authorizer = DummyAuthorizer()
        self.authorizer.add_user(self.Tasks.WorkData["ftp_user"], self.Tasks.WorkData["ftp_pass"], self.Tasks.WorkData["ftp_root"], perm="elradfmw")
        self.authorizer.add_anonymous(self.Tasks.WorkData["ftp_root"])
        self.Tasks.FTPHandler = c_WebImporterFTPHandler

        self.Tasks.FTPHandler.authorizer = self.authorizer

        self.server = FTPServer(("0.0.0.0", 21), self.Tasks.FTPHandler)

        self.server.serve_forever()



class c_CopyWorker(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self,dict_Jobs, dict_Data, worker_name,worker_queue,result_queue, operand, Tasks):
        threading.Thread.__init__(self)
        self.operand = operand
        self.name = worker_name
        self.dict_Jobs = dict_Jobs
        self.dict_Data = dict_Data
        self.worker_name = worker_name
        self.worker_queue = worker_queue
        self.result_queue = result_queue
        self.Tasks = Tasks
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
    def customCopyFile(self,sourcefile,destinationfile,filesize,dict_Jobs,ID,worker_name, Tasks, buffer_size=16, perserveFileDate=True, ):
        buffer_size = 1024*1024*buffer_size
        try:
            fsrc = open(sourcefile, 'rb')
            fdst = open(destinationfile, 'wb')

            self.buffer_size = min(buffer_size,filesize)
            if(buffer_size == 0):
                buffer_size = 1024
            count = 0
            self.Payload = {}
            self.Payload["ID"] = self.ID


            while True:
                # Read blocks of size 2**20 = 1048576
                # Depending on system may require smaller
                #  or could go larger...
                #  check your fs's max buffer size
                buf = fsrc.read(buffer_size)
                if not buf:
                    #dict_Jobs[ID].workerlist[worker_name][sourcefile] = 100.0
                    break
                if dict_Jobs[ID].active == False:
                    logging.debug("Aborting file copy:%s", sourcefile)
                    break
                fdst.write(buf)
                count += len(buf)
                #dict_Jobs[ID].workerlist[worker_name][sourcefile] = (count/filesize)*100
                dict_Jobs[ID].filelist[sourcefile].progress = (count/filesize)*100/2
                self.Payload["progress"] = ((count/filesize)*100)/2

                if filesize > 1024*1024*self.Tasks.WorkData["large_file_threshold"]:

                    if Tasks.syncserver_client.connected:
                        self.Payload["file"] = sourcefile
                        #this sends it to the sync server, who will notify all connected clients about the progress
                        Tasks.syncserver_progress_client.m_send(self.m_create_data("/syncserver/v1/global/queue/task/file/set_progress", self.Payload))
                    for cli in Tasks.ProgressClients:
                        self.Payload["file"] = os.path.split(sourcefile)[1]
                        cli.write_message(self.m_create_data("/client/v1/local/queue/task/file/set_progress", self.Payload))


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
                    # if self.worker_name in self.dict_Jobs[self.ID].workerlist == False:
                    #     self.dict_Jobs[self.ID].workerlist[self.worker_name] = {}
                    #self.copyFile(self.srcfile,self.dstfile,self.dict_Jobs[self.ID].filelist[self.srcfile].size)
                    logging.debug(self.dstfile)
                    self.customCopyFile(self.srcfile,self.dstfile,self.dict_Jobs[self.ID].filelist[self.srcfile].size, self.dict_Jobs, self.ID, self.worker_name, self.Tasks)
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

    def run(self):
        logging.debug("Line manager started:")

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
                    #self.dict_Jobs[self.ID].workerlist[self.CopyWorkerName] = {}
                    self.LargeCopyWorkerProcess = c_CopyWorker(self.dict_Jobs, self.dict_Data,self.CopyWorkerName,self.large_worker_queue,self.result_queue, ">", self.Tasks)
                    self.LargeCopyWorkerProcess.start()
                    self.Threads[self.LargeCopyWorkerProcess.name] = (self.LargeCopyWorkerProcess)

                #copy workers that work on files less than 5mb
                if self.numSmallFiles > 0:
                    self.NumSmallCopyWorkers = self.dict_Data["CopyWorkers_Per_Line"]
                    if self.numSmallFiles < self.NumSmallCopyWorkers:
                        self.NumSmallCopyWorkers = self.numSmallFiles

                    for i in range(self.NumSmallCopyWorkers):
                        self.CopyWorkerName = "[" + self.manager_name + "] Copy Worker_" + str(i)
                        #self.dict_Jobs[self.ID].workerlist[self.CopyWorkerName] = {}
                        self.CopyWorkerProcess = c_CopyWorker(self.dict_Jobs, self.dict_Data,self.CopyWorkerName,self.worker_queue,self.result_queue, "<", self.Tasks)
                        self.CopyWorkerProcess.start()
                        self.aCopyWorkers.append(self.CopyWorkerProcess)
                        self.Threads[self.CopyWorkerProcess.name] = (self.CopyWorkerProcess)
                self.OldQueueSize = 0
                self.CompleteCounter = 0
                self.aCompletedFiles = {}
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
                    self.dict_Jobs[self.ID].progress = ((self.CurrentSize+self.CurrentQueueSize)/len(self.next_task.filelist)*100)/2


                    self.Payload = {}
                    self.Payload["ID"] = self.ID
                    self.Payload["progress"] = self.dict_Jobs[self.ID].progress

                    if self.CurrentQueueSize > self.OldQueueSize:
                        self.OldQueueSize = self.CurrentQueueSize
                        #send progress to the sync server here?
                        #the tasks processed here are only the local ones.
                        #sends progress data to the sync server so that all other clients will get notified of the progress too.
                        for cli in self.Tasks.ProgressClients:
                            cli.write_message(self.m_create_data("/client/v1/local/queue/task/set_progress", self.Payload))

                        for file in self.Tasks.Jobs[self.ID].filelist:
                            if file not in self.aCompletedFiles:
                                if self.Tasks.Jobs[self.ID].filelist[file].size < self.Tasks.WorkData["large_file_threshold"]*1024*1024:
                                    if self.Tasks.Jobs[self.ID].filelist[file].progress >= 50.0:
                                        self.FilePayload = {}
                                        self.FilePayload["ID"] = self.ID
                                        self.FilePayload["file"] = file
                                        self.FilePayload["progress"] = 50.0
                                        for cli in self.Tasks.ProgressClients:
                                            cli.write_message(self.m_create_data("/client/v1/local/queue/task/file/set_progress", self.FilePayload))
                                        if self.Tasks.syncserver_client.connected:
                                            #this sends it to the sync server, who will notify all connected clients about the progress
                                            self.Tasks.syncserver_progress_client.m_send(self.m_create_data("/syncserver/v1/global/queue/task/file/set_progress", self.FilePayload))

                                        self.aCompletedFiles[file] = True

                        if self.Tasks.syncserver_client.connected:
                            #this sends it to the sync server, who will notify all connected clients about the progress
                            self.Tasks.syncserver_progress_client.m_send(self.m_create_data("/syncserver/v1/global/queue/task/set_progress",self.Payload))
                        """
                            else need to update the connected clients to this server about the status update.
                            No need for polling (ajax style). The server will push the data to the clients
                        """
                        #write out data each time a file has been processed

                    if self.dict_Jobs[self.ID].progress >= 50.0:
                        self.dict_Jobs[self.ID].progress = 50.0
                        self.dict_Jobs[self.ID].active = False

                        #self.Tasks.syncserver_client.m_send(self.Tasks.syncserver_client.m_create_data("/syncserver/v1/global/queue/task/set_progress", self.Payload))

                        while not self.result_queue.empty():
                            self.result_queue.get()
                        break


                if self.dict_Jobs[self.ID].progress >= 50.0:
                    self.dict_Jobs[self.ID].active = False
                    self.dict_Jobs[self.ID].progress = 50.0



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

                    ### send to upload manager
                    self.m_UploadCompleteTasks(self.Tasks)

                    logging.debug("Finished caching job")
            else:
                logging.debug("No files in task")
                #self.m_UploadCompleteTasks(self.Tasks)
        return
class c_TranscodeMonitor(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self, Tasks):
        threading.Thread.__init__(self)
        self.TM = None
        self.Tasks = Tasks
        self.Started = False
        self.start()
        self.uploadtask = None

    def startMonitor(self, MonitorLink):
        if not self.Started:
            self.client = self.Tasks.VizOneClient
            self.TM = handle(MonitorLink.href, self.handler, self.Tasks.WorkData["vizone_user"], self.Tasks.WorkData["vizone_pass"])
            self.Started = True
    def stopMonitor(self):
        if self.Started:
            self.TM.close()
            self.Started = False
    def run(self):
        while True:
            #run indefinitely
            if self.Started:
                if len(self.Tasks.RequestLinks) == 0:
                    self.stopMonitor()

            continue
    def handler(self, response):
        self.identity = response.headers.get('identity')
        self.bContinue = False
        self.CurrentID = None
        for request in self.Tasks.RequestLinks:
            if self.identity == request.atomid:
                self.CurrentID = request
                self.bContinue = True
                break

        if self.bContinue == True:
            self.r = TransferRequest(response)
            #this status is for transcoding.
            if self.r.progress != None:
                self.head, self.tail = os.path.split(self.Tasks.RequestList[self.CurrentID.atomid]["path"])

                print("[" + self.tail + "] Status = ", self.r.progress.done, self.r.progress.total)


                self.uploadTask = self.Tasks.RequestList[self.CurrentID.atomid]["uploadTask"]
                #this is on a per file basis
                self.uploadTask.progress = ((self.r.progress.done/self.r.progress.total)*100.0)

                # needs to update the progress on file and task
                self.uploadTask.ParentTask.filelist[self.Tasks.RequestList[self.CurrentID.atomid]["uploadTask"].file].progress = 75+(self.uploadTask.progress/4)
                self.uploadTask.ParentTask.progress = self.uploadTask.ParentTask.GetCurrentTotalProgress()
                print("Current status:", self.uploadTask.ParentTask.progress)
                self.data = {}
                self.data["ID"] = self.uploadTask.ParentTask.TaskID
                self.data["progress"] = self.uploadTask.progress
                self.data["file"] = os.path.split(self.uploadTask.file)[1]
                for cli in self.Tasks.ProgressClients:
                    cli.write_message(self.m_create_data("/client/v1/local/queue/task/file/set_progress", self.data))

                #self.Tasks.syncserver_progress_client.m_send(self.m_create_data("/syncserver/v1/global/queue/task/file/transcode/set_progress", self.data))


                self.ProgressData = {}
                self.ProgressData["ID"] = self.uploadTask.ParentTask.TaskID
                self.ProgressData["progress"] = self.uploadTask.ParentTask.progress
                self.Tasks.syncserver_progress_client.m_send(self.m_create_data("/syncserver/v1/global/queue/task/set_progress", self.ProgressData))

                for cli in self.Tasks.ProgressClients:
                    cli.write_message(self.m_create_data("/client/v1/local/queue/task/set_progress", self.ProgressData))


                if self.r.progress.done == self.r.progress.total:
                    logging.debug("Transcode complete for:%s", os.path.split(self.uploadTask.file)[1])
                    self.uploadTask.FileTask.transcoded = True
                    self.data["ID"] = self.uploadTask.ParentTask.TaskID
                    self.data["file"] = self.uploadTask.file
                    #notify everyone of this event
                    self.data["transcoded"] = True
                    self.data["active"] = False
                    logging.debug("Transcoded = %s", self.Tasks.Jobs[self.uploadTask.ParentTask.TaskID].filelist[self.uploadTask.file].transcoded)
                    self.Tasks.syncserver_client.m_send(self.m_create_data("/syncserver/v1/global/upload/queue/task/file/transcoded",self.data))

                    self.activeData = {}
                    self.activeData["ID"] = self.uploadTask.ParentTask.TaskID
                    self.activeData["active"] = False

                    logging.debug("Notifying syncserver of new active task:%s", self.uploadTask.ParentTask.TaskID)
                    self.Tasks.syncserver_client.m_send(self.m_create_data("/syncserver/v1/global/queue/task/active", self.activeData))

                    self.WriteJob(self.Tasks, self.uploadTask.ParentTask.TaskID)
                    self.Tasks.RequestLinks.remove(self.CurrentID)
                    self.ChangeTranscodePriority(self.Tasks)




            else:
                self.Tasks.RequestLinks.remove(self.CurrentID)
class c_UploadWorker(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self,Tasks, upload_worker_name):
        threading.Thread.__init__(self)
        self.name = upload_worker_name
        self.Tasks = Tasks
        self.UploadWorkerQueue = Tasks.upload_worker_queue
        self.aItemAsset = []
        self.aUploadList = []
        self.method = "ftp"
    def run(self):
        logging.debug("Upload worker started")
        while True:
            self.next_task = self.UploadWorkerQueue.get()
            time.sleep(1)

            if self.next_task == None:
                if self.Tasks.TM != None:
                    self.Tasks.TM.close()
                logging.debug("breaking out")
                break

            if self.next_task.ParentTask.type == "global":
                ##wait until the global task has finished uploading until going onto the next task
                while not self.next_task.FileTask.uploaded:
                    logging.debug("waiting")
                    #break out if a flag is set
                    if not self.Tasks.Jobs[self.next_task.ParentTask.TaskID].active:
                        break
                    continue
            else:
                logging.debug("Notifying syncserver of new upload task:%s", self.next_task.file)
                self.Tasks.Jobs[self.next_task.ParentTask.TaskID].filelist[self.next_task.file].uploaded = True
                self.data = {}
                self.data["ID"] = self.next_task.ParentTask.TaskID
                self.data["file"] = self.next_task.file
                self.data["uploaded"] = True
                self.Tasks.syncserver_client.m_send(self.m_create_data("/syncserver/v1/global/upload/queue/task/file/uploaded", self.data))

                self.data = {}
                self.data["ID"] = self.next_task.ParentTask.TaskID
                self.data["file"] = self.next_task.file
                self.data["active"] = True

                self.Tasks.Jobs[self.next_task.ParentTask.TaskID].active = True
                logging.debug("Notifying syncserver of new active task:%s", self.next_task.ParentTask.TaskID)
                self.Tasks.syncserver_client.m_send(self.m_create_data("/syncserver/v1/global/queue/task/active", self.data))

                ###announce this to the syncserver which will broadcast this to other clients logged on to the syncserver
                #wait for the file to be imported before it start on the next file.


                ###upload here

                self.UploadFile = None

                self.NewFile = os.path.normpath(self.next_task.file)
                self.PathHead, self.FileTail = os.path.split(self.NewFile)
                self.FileHead, self.Extension = (os.path.splitext(self.FileTail))

                ## create an asset
                self.asset_entry_collection = self.Tasks.VizOneClient.servicedoc.get_collection_by_keyword('asset')
                self.asset_entry_endpoint = self.asset_entry_collection.endpoint
                self.placeholder = Item(title=self.FileHead)
                ##get the placeholder asset entry
                self.placeholder.parse(self.Tasks.VizOneClient.POST(self.asset_entry_endpoint, self.placeholder))

                logging.debug("Creating asset:%s", self.placeholder.atomid)
                #print("Placeholder atomid:", self.placeholder.atomid)
                self.aItemAsset.append(self.placeholder.atomid)
                self.ftpLink = None
                if self.method == "ftp":
                    self.drive, self.Path = os.path.splitdrive(self.PathHead)
                    self.Path = "/" + self.Path[1:]
                    self.UploadFile = self.Tasks.WorkData["sTargetDir"] + self.next_task.ParentTask.TaskID + self.Path + "/" + self.FileHead + self.Extension
                    self.drive, self.Path = os.path.splitdrive(self.UploadFile)
                    self.aPathTokens = self.Path.split("/")
                    self.NewPath = ""
                    for i in range(2, len(self.aPathTokens)):
                        if i < (len(self.aPathTokens)-1):
                            self.NewPath += self.aPathTokens[i] + "/"
                        else:
                            self.NewPath += self.aPathTokens[i]


                    self.ftpLink = "ftp://" + self.Tasks.WorkData["ftp_user"] + ":" + self.Tasks.WorkData["ftp_pass"] + "@" + socket.gethostbyname(socket.gethostname()) + "/" + self.NewPath
                    #print(self.ftpLink)
                    self.UploadFile = UriList([self.ftpLink])
                else:
                    self.drive, self.Path = os.path.splitdrive(self.PathHead)
                    self.Path = "/" + self.Path[1:]
                    self.UploadFile = self.Tasks.WorkData["sTargetDir"] + self.next_task.ParentTask.TaskID + self.Path + "/" + self.FileHead + self.Extension

                logging.debug("Starting upload:%s", self.UploadFile)




                #we want to announce this to other clients here, as we do not want to abort a file import.

                self.WriteJob(self.Tasks, self.next_task.ParentTask.TaskID)
                ###once done, then announce this to the other webimporter servers


                self.TransRequest = None
                if self.method == "ftp":


                    self.incoming = Incoming(self.Tasks.VizOneClient.POST(self.placeholder.import_unmanaged_link, self.UploadFile, check_status=False))

                    self.Counter = 0
                    while True:
                        #get all incoming media tasks
                        self.Collection = IncomingCollection(get_incomings())
                        self.bBreakOut = False
                        self.incomings = [Incoming(e) for e in self.Collection.entries]
                        for incoming in self.incomings:
                            #if transferlink is NONE then
                            if incoming.atomid == self.incoming.atomid:
                                self.Counter = 0
                                logging.debug("Import progress:%s", self.UploadFile)
                                if incoming.transfer_link != None:
                                    self.TransRequest = incoming.transfer_link
                                    self.TransRequest = TransferRequest(self.Tasks.VizOneClient.GET(self.TransRequest))
                                    self.bBreakOut = True
                        if self.bBreakOut == True:
                            break

                            ##wait 2 minutes
                        if self.Counter == 60:
                            logging.debug("import process timed out")
                            break

                        #wait until transfer request link is available
                        self.Counter += 1
                        time.sleep(0.5)
                else:
                    with open(self.UploadFile, 'rb') as f:
                        self.data = {}
                        self.data['Content-Type'] = 'application/octet-stream'
                        self.data['Expect'] = ''
                        #uploads the file
                        self.r = self.Tasks.VizOneClient.PUT(self.placeholder.edit_media_link, f, headers=self.data)
                        logging.debug("Upload complete")
                        #have to wait a little bit for the file to be finalised on the server
                        time.sleep(1)

                        #waits for the placeholder to be ready with the transfer request (transcoding request)
                        logging.debug("Waiting for upload completion response")
                        while True:
                            self.placeholder = Item(self.Tasks.VizOneClient.GET(self.placeholder.self_link))
                            try:
                                self.r = self.Tasks.VizOneClient.GET(self.placeholder.upload_task_link)
                                self.Incoming = Incoming(self.r)
                                if self.Incoming.transfer_link != None:
                                    self.TransRequest = TransferRequest(self.Tasks.VizOneClient.GET(self.Incoming.transfer_link))
                                    break
                            except:
                                time.sleep(1)

                #at this point the transfer request link is available. It should be possible to set the priority here now
                if self.TransRequest != None:
                    self.TransRequest.priority = self.next_task.priority
                    self.Tasks.VizOneClient.PUT(self.TransRequest.self_link, self.TransRequest, check_status=False)

                    self.Tasks.RequestLinks.append(self.TransRequest)
                    self.Tasks.RequestList[self.TransRequest.atomid] = {}


                    if self.method == "ftp":
                        self.Tasks.RequestList[self.TransRequest.atomid]["path"] = self.ftpLink
                    else:
                        self.Tasks.RequestList[self.TransRequest.atomid]["path"] = self.UploadFile


                    #logging.debug("dest file is:%s", self.UploadFile)
                    self.Tasks.RequestList[self.TransRequest.atomid]["asset"] = self.placeholder.self_link.href

                    #logging.debug("!!!!!!!!!!!!!!!" + self.TransRequest.atomid + "|" + self.next_task.ParentTask.TaskID + " !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    self.Tasks.RequestList[self.TransRequest.atomid]["uploadTask"] = self.next_task


                    #at this point there is a transferRequest monitor link. Monitoring can happen from here
                    if not self.Tasks.TM.Started:
                        self.Tasks.TM.startMonitor(self.TransRequest.monitor_link)


                    ## Add the files to the folder
                    self.next_task.PackageLink = get_aggregate_asset_by_atom_id(self.next_task.PackageLink.atomid)

                    if self.next_task.PackageLink.content == None:
                        logging.debug("setting new content in package")
                        self.list = []
                        self.list.append(self.placeholder.atomid)
                        self.next_task.PackageLink.content = UriList(self.list)
                    else:
                        logging.debug("Adding to package:%s", self.next_task.PackageLink)
                        logging.debug("Contents of packagelink:%s", self.next_task.PackageLink.content.uris)
                        self.list = self.next_task.PackageLink.content.uris
                        self.list.append(self.placeholder.atomid)
                        self.next_task.PackageLink.content = UriList(self.list)
                        logging.debug(self.next_task.PackageLink.content.uris)

                    self.Tasks.VizOneClient.PUT(self.next_task.PackageLink.edit_link, self.next_task.PackageLink, check_status=False)

                    self.next_task.FileTask.transferlink = self.TransRequest.self_link.href
                    self.next_task.FileTask.assetlink = self.placeholder.self_link.href
                    self.Tasks.Jobs[self.next_task.ParentTask.TaskID].filelist[self.next_task.file].transferlink = self.TransRequest.self_link.href
                    self.Tasks.Jobs[self.next_task.ParentTask.TaskID].filelist[self.next_task.file].assetlink = self.placeholder.self_link.href
                    #notify other clients about adding the atom links to the assets
                    self.data = {}
                    self.data["ID"] = self.next_task.ParentTask.TaskID
                    self.data["file"] = self.next_task.file
                    self.data["transferlink"] = self.TransRequest.self_link.href
                    self.data["assetlink"] = self.placeholder.self_link.href

                    self.Tasks.syncserver_client.m_send(self.m_create_data("/syncserver/v1/global/upload/queue/task/file/atomlink/update", self.data))


                    #write the data to disk
                    self.WriteJob(self.Tasks, self.next_task.ParentTask.TaskID)
class c_UploadManager(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self,Tasks, Upload_manager_name):
        threading.Thread.__init__(self)
        logging.debug("Starting upload manager")
        self.Tasks = Tasks
        global FTPTasks
        FTPTasks = Tasks


        self.upload_queue = Tasks.upload_queue
        self.name = Upload_manager_name
        self.Threads = []

        ##viz one connection
        self.Tasks.VizOneClient = init(self.Tasks.WorkData["vizone_address"], self.Tasks.WorkData["vizone_user"], self.Tasks.WorkData["vizone_pass"])
        self.worker_queue = queue.Queue()
        self.Tasks.TM = c_TranscodeMonitor(self.Tasks)
        #starting the upload workers
        for i in range(5):
            self.uploadWorker = c_UploadWorker(self.Tasks, ("upload_worker_" + str(i)))
            self.uploadWorker.start()
            self.Threads.append(self.uploadWorker)

    def Shutdown(self):
        self.aThreads = []
        #clear the upload_worker_queue
        while not self.Tasks.upload_worker_queue.empty():
            self.Tasks.upload_worker_queue.get()
        for thread in self.Threads:
            self.Tasks.upload_worker_queue.put(None)

        for thread in self.Threads:
            self.aThreads.append(thread)

        for thread in self.aThreads:
            if thread in self.Threads:
                while self.Threads[thread].isAlive():
                    continue
                del self.Threads[thread]
    def SetMetadata(self,Tasks, object, metadata, type):

        self.model_link = ("http://" + Tasks.WorkData["vizone_address"] +  "/api/metadata/form?qType=COLLECTION.Collection&qSubType=" + type)


        self.models = MetadataFormCollection(self.Tasks.VizOneClient.GET(self.model_link))
        self.Object_Desc = [model for model in self.models.entries if model.name == type][0]
        self.ObjectItem = Model(self.Tasks.VizOneClient.GET(self.Object_Desc.self_link))
        self.ObjectPayload = self.ObjectItem.to_payload()
        for item in metadata:
            for key, value in item.items():
                self.ObjectPayload.set(key, value)

        self.Tasks.VizOneClient.PUT(object.describedby_link, self.ObjectPayload)


    def run(self):
        while True:
            self.next_task = self.upload_queue.get()
            if not self.next_task.ParentTask:
                logging.debug("parent task is not present")
                continue
            else:
                #metadata has been added here.
                self.AllSeries = get_seriess()
                self.series = self.m_GetMetaData(self.next_task.ParentTask.metadata, "series")
                self.episode = self.m_GetMetaData(self.next_task.ParentTask.metadata, "episode")
                self.card = self.m_GetMetaData(self.next_task.ParentTask.metadata, "card")
                self.date = self.m_GetMetaData(self.next_task.ParentTask.metadata, "date")


                self.SeriesObject = None
                for SeriesEntry in self.AllSeries.entries:
                    if SeriesEntry.title == self.series:
                        logging.debug("Using existing series:%s", self.series)
                        self.SeriesObject = SeriesEntry
                        #logging.debug(self.SeriesObject)
                        break

                if not self.SeriesObject:
                    logging.debug("Creating series:%s", self.series)
                    #create the series
                    self.SeriesObject = Series(title=self.series)
                    self.SeriesObject = create_series(self.SeriesObject)
                    logging.debug("Models LINK !!!!!!! %s", self.SeriesObject.models_link.href)
                    #metadata can only be set if it does not have metadata already


                self.metadata = hfn.c_metadata()
                self.metadata.m_add("asset.title", self.series)
                self.metadata.m_add("asset.description", "Elliot is a hacker")
                self.SetMetadata(self.Tasks, self.SeriesObject, self.metadata.m_get(), 'series')
                    #logging.debug(self.SeriesObject.type)

                self.AllEpisodes = (Members(self.Tasks.VizOneClient.GET(self.SeriesObject.down_link)))
                self.EpisodeObject = None
                for EpisodeEntry in self.AllEpisodes.entries:
                    if EpisodeEntry.title == self.episode:
                        logging.debug("Using existing episode:%s", self.episode)
                        self.EpisodeObject = EpisodeEntry.program
                        break

                if not self.EpisodeObject:
                    logging.debug("Creating episode:%s", self.episode)
                    self.EpisodeObject = Program(title=self.episode)
                    self.EpisodeObject = create_program(self.EpisodeObject)
                    ##put the program in the series
                    self.EpisodeObject_urilist = UriList([self.EpisodeObject.atomid])
                    self.Tasks.VizOneClient.POST(self.SeriesObject.addmembers.add_last_link, self.EpisodeObject_urilist)

                # self.metadata = hfn.c_metadata()
                # self.metadata.m_add("asset.title", self.episode)
                # self.metadata.m_add("asset.description", "Daemon")
                # self.SetMetadata(self.Tasks, self.EpisodeObject, self.metadata.m_get(), 'program')


                self.ALLPackageInEpisodes = (Members(self.Tasks.VizOneClient.GET(self.EpisodeObject.down_link)))
                self.PackageObject = None

                ##use the metadata to create the folder name
                for PackageEntry in self.ALLPackageInEpisodes.entries:
                    if PackageEntry.title == self.card:
                        if PackageEntry.aggregateasset != None:
                            logging.debug("Using existing Package:%s", self.card)
                            self.PackageObject = PackageEntry.aggregateasset
                            self.PackageObject = get_aggregate_asset_by_atom_id(self.PackageObject.atomid)
                        ##if it is found, then it should already be under the right series
                            break

                if self.PackageObject == None:

                    self.PackageObjectSet = aggregate_asset_ItemSet(title=self.card)
                    self.PackageObjectID = create_aggregate_asset(self.PackageObjectSet)
                    self.PackageObject = get_aggregate_asset_by_atom_id(self.PackageObjectID.atomid)
                    self.PackageObject_urilist = UriList([self.PackageObject.atomid])
                    logging.debug("Creating new package:%s", self.PackageObject)
                    self.Tasks.VizOneClient.POST(self.EpisodeObject.addmembers.add_last_link, self.PackageObject_urilist)

                # self.metadata = hfn.c_metadata()
                # self.metadata.m_add("asset.title", self.card)
                # self.metadata.m_add("asset.description", self.date)
                # self.SetMetadata(self.Tasks, self.PackageObject, self.metadata.m_get(), 'folder')

                self.next_task.PackageLink = self.PackageObject
                logging.debug("putting task on worker queue:%s", self.next_task.file)


                self.Tasks.upload_worker_queue.put(self.next_task)
