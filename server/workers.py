import os
import shutil
import time
import common as hfn
import threading
import queue
import logging
import dataclasses
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
from vizone.payload.series import SeriesFeed
from vizone.payload.folder import Folder
from vizone.resource.folder import create_folder
from vizone.payload.media import Incoming
from vizone.payload.transfer import TransferRequest
from vizone.resource.incoming import get_incomings
from vizone.payload.media.incomingcollection import IncomingCollection
from vizone.net.message_queue import handle


##needs to override the incoming class as it is missing the atomid. Will be fixed in the next version of python One
from vizone.payload.media.incoming import Incoming as _Incoming
from vizone.descriptors import Value
class Incoming(_Incoming):
    atomid = Value('atom:id', str)


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
            self.Payload["file"] = sourcefile

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
                dict_Jobs[ID].filelist[sourcefile].progress = (count/filesize)*100
                self.Payload["progress"] = (count/filesize)*100
                if filesize > 1024*1024*self.Tasks.WorkData["large_file_threshold"]:
                    for cli in Tasks.ProgressClients:
                        cli.write_message(self.m_create_data("/client/v1/local/queue/task/file/set_progress", self.Payload))
                    if Tasks.syncserver_client.connected:
                        #this sends it to the sync server, who will notify all connected clients about the progress
                        Tasks.syncserver_progress_client.m_send(self.m_create_data("/syncserver/v1/global/queue/task/file/set_progress", self.Payload))



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
                    self.dict_Jobs[self.ID].progress = (self.CurrentSize+self.CurrentQueueSize)/len(self.next_task.filelist)*100


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
                                    if self.Tasks.Jobs[self.ID].filelist[file].progress == 100.0:
                                        self.FilePayload = {}
                                        self.FilePayload["ID"] = self.ID
                                        self.FilePayload["file"] = file
                                        self.FilePayload["progress"] = self.Tasks.Jobs[self.ID].filelist[file].progress
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

                    if self.dict_Jobs[self.ID].progress >= 100.0:
                        self.dict_Jobs[self.ID].progress = 100.0
                        self.dict_Jobs[self.ID].active = False
                        self.m_UploadCompleteTasks(self.Tasks)
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


                    ### send to upload manager
                    self.m_UploadCompleteTasks(self.Tasks)

                    logging.debug("Finished caching job")
            else:
                logging.debug("No files in task")
        return








class c_UploadManager(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self,Tasks, Upload_manager_name):
        threading.Thread.__init__(self)
        self.Tasks = Tasks
        self.upload_queue = Tasks.upload_queue
        self.dict_Jobs = Tasks.Jobs
        self.dict_Data = Tasks.WorkData
        self.Upload_manager_name = Upload_manager_name
        self.name = self.Upload_manager_name
        self.Threads = {}

        ##viz one connection
        self.client = init('192.168.110.144', 'admin', 'admin')
        self.TM = None

    def run(self):
        logging.debug("Upload manager started:")

        while True:
            self.next_task = self.upload_queue.get()
            if self.next_task == None:
                if self.TM != None:
                    self.TM.close()

                break

            if self.next_task.ParentTask.type == "global":
                ##wait until the global task has finished uploading until going onto the next task
                while not self.next_task.FileTask.uploaded:
                    continue

            else:
                ###upload here

                self.osd = search_seriess()


                ##use the metadata to create the series name
                self.searchUrl = self.osd.make_url({
                    'searchTerms': 'Mr Robot',
                    'count': 1,
                    'vizsort:sort': '-search.creationDate',
                })

                self.result = SeriesFeed(self.client.GET(self.searchUrl))
                self.series = None
                if (len(self.result.entries) == 0):
                    ##if series does not exist then create one
                    self.series = Series(title="Mr Robot")
                    self.series = create_series(self.series)
                else:
                    ##else just use the first one you find that matches it. Should not be duplicate series with the same name
                    self.series = self.result.entries[0]


                #figure out what programs is in the series. Means that we need the series first

                ##use the metadata to create the episode name
                self.SeriesPrograms = (Members(self.client.GET(self.series.down_link)))
                self.program = None
                for entry in self.SeriesPrograms.entries:
                    if entry.title == "Episode 1":
                        self.program = entry.program
                        ##if it is found, then it should already be under the right series
                        break

                if self.program == None:
                    self.program = Program(title="Episode 1")
                    self.program = create_program(self.program)
                    ##put the program in the series
                    self.Program_urilist = UriList([self.program.atomid])
                    Members(self.client.POST(self.series.addmembers.add_last_link, self.Program_urilist))

                ##add the asset item into the folder

                self.FoldersInPrograms = (Members(self.client.GET(self.program.down_link)))
                self.folder = None

                ##use the metadata to create the folder name
                for entry in self.FoldersInPrograms.entries:
                    if entry.title == "Card1":
                        self.folder = entry.folder
                        ##if it is found, then it should already be under the right series
                        break

                if self.folder == None:
                    self.folder = Folder(title="Card1")
                    self.folder = create_folder(self.folder)
                    ##add the folder item into the program
                    self.Folder_urilist = UriList([self.folder.atomid])
                    Members(self.client.POST(self.program.addmembers.add_last_link, self.Folder_urilist))

                ## create an asset
                self.asset_entry_collection = self.client.servicedoc.get_collection_by_keyword('asset')
                self.asset_entry_endpoint = self.asset_entry_collection.endpoint


                self.aItemAsset = []
                self.aUploadList = []
                self.RequestLinks = []
                self.RequestList = {}


                self.NewFile = os.path.normpath(self.next_task.file)
                self.PathHead, self.FileTail = os.path.split(self.NewFile)
                self.FileHead, self.Extension = (os.path.splitext(self.FileTail))

                self.placeholder = Item(title=self.FileHead)
                ##get the placeholder asset entry
                self.placeholder.parse(self.client.POST(self.asset_entry_endpoint, self.placeholder))
                #print("Placeholder atomid:", self.placeholder.atomid)
                self.aItemAsset.append(self.placeholder.atomid)

                self.drive, self.Path = os.path.splitdrive(self.PathHead)

                self.aPathTokens = self.Path.split("\\")
                self.NewPath = ""
                for i in range(2, len(self.aPathTokens)):
                    self.NewPath += self.aPathTokens[i] + "/"

                #the folder needs to have a root folder called ftp!!!

                self.ftpLink = ['ftp://ardome:aidem630@10.211.110.145/' + self.NewPath + self.FileHead + self.Extension]
                self.ftpLink = UriList(self.ftpLink)

                #Incoming media
                #this uploads the file (or rather, viz one imports the file from your computer)
                self.incoming = Incoming(self.client.POST(self.placeholder.import_unmanaged_link, self.ftpLink, check_status=False))



                #wait for the file to be imported before it start on the next file. This is ok for a single machine, but for
                #multiple then this will not work. It will need some sort of id mechanism as it checks all incoming media
                #tasks from the server and does no filtering.

                self.TransferRequestLink = None
                while True:
                    #get all incoming media tasks
                    self.Collection = IncomingCollection(get_incomings())
                    self.bBreakOut = False
                    self.incomings = [Incoming(e) for e in self.Collection.entries]
                    for incoming in self.incomings:
                        #if transferlink is NONE then
                        if incoming.atomid == self.incoming.atomid:
                            print("Import progress:", (self.NewPath + self.FileHead + self.Extension))
                            if incoming.transfer_link != None:
                                self.TransferRequestLink = incoming.transfer_link
                                self.bBreakOut = True

                    if self.bBreakOut == True:
                        break
                    #wait until transfer request link is available
                    time.sleep(0.5)

                    #at this point the transfer request link is available. It should be possible to set the priority here now

                self.TransRequest = TransferRequest(self.client.GET(self.TransferRequestLink))

                ##can be "low", "medium", "high"
                self.TransRequest.priority = self.next_task.priority
                self.client.PUT(self.TransRequest.self_link, self.TransRequest, check_status=False)

                self.RequestLinks.append(self.TransRequest)
                self.RequestList[self.TransRequest.atomid] = {}
                self.RequestList[self.TransRequest.atomid]["path"] = self.NewPath + self.FileHead + self.Extension
                self.RequestList[self.TransRequest.atomid]["asset"] = self.placeholder.atomid


                #at this point there is a transferRequest monitor link. Monitoring can happen from here
                if not self.TM:
                    self.TM = handle(self.RequestLinks[0].monitor_link.href, self.handler, 'admin', 'admin')


                ## Add the files to the folder
                self.ItemAsset_urilist = UriList(self.aItemAsset)
                Members(self.client.POST(self.folder.addmembers.add_last_link, self.ItemAsset_urilist))

                self.next_task.FileTask.uploaded = True

                self.WriteJob(self.Tasks, self.next_task.ParentTask.TaskID)
                ###once done, then announce this to the other webimporter servers
                self.data = {}
                self.data["ID"] = self.next_task.ParentTask.TaskID
                self.data["file"] = self.next_task.file
                self.data["uploaded"] = True
                self.Tasks.syncserver_client.m_send(self.m_create_data("/syncserver/v1/global/upload/queue/task/file/uploaded", self.data))
                ###announce this to the syncserver which will broadcast this to other clients logged on to the syncserver







