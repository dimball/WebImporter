import threading
import dataclasses
import uuid
import common as hfn
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
import os
import shutil

class c_createTask(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self, Payload, Tasks):
        threading.Thread.__init__(self)
        self.Payload = Payload
        self.Data = {}
        self.Output = {}
        self.Tasks = Tasks

    def run(self):
        #logging.debug("Create task type:%s", type(self.Payload))
        self.ID = str(uuid.uuid4())
        logging.debug("Creating task : %s", self.ID)
        self.Tasks.Jobs[self.ID] = dataclasses.c_Task(self.ID)

        self.Data["ID"] = self.ID
        self.Data["command"] = "expand_files"
        self.Data["payload"] = self.Payload
        self.Output["status"] = self.ID
        self.Tasks.Jobs[self.ID].state = "busy"

        self.Tasks.Order.append(self.ID)
        self.highestorderID = 0
        for JobID in self.Tasks.Jobs:
#            logging.debug("job id is:%s,%s", JobID, Tasks.Jobs[JobID].order)
            if self.Tasks.Jobs[JobID].order > self.highestorderID:
                self.highestorderID = int(self.Tasks.Jobs[JobID].order)

        logging.debug("Highest order ID:%s", self.highestorderID+1)
        self.Tasks.Jobs[self.ID].order = self.highestorderID+1
        self.Tasks.Jobs[self.ID].progress = -1

        #logging.debug("type: %s", type(self.Payload))
        self.FileListData = self.FileExpand(self.ID,self.Payload)

        self.Tasks.Jobs[self.ID].filelist = self.FileListData[0]
        self.Tasks.Jobs[self.ID].filelistOrder = self.FileListData[1]

        logging.debug("Number of files:%s", len(self.Tasks.Jobs[self.ID].filelist))
        self.Tasks.Jobs[self.ID].state = "ready"

        '''
        When creating a task, also send the data to the sync server. Send the ID here. When metadata is created, then send the ID with the metadata so
        that other clients can see this as well.
        '''
        if self.Tasks.syncserver_client.connected:
            self.Tasks.syncserver_client.m_send(self.m_create_data('/syncserver/v1/global/queue/task/put', self.m_SerialiseTaskList([self.Tasks.Jobs[self.ID]], self.Tasks)), self.Tasks)


        self.WriteJob(self.Tasks,self.ID)

class c_remove_completed_tasks(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self, Payload, Tasks):
        threading.Thread.__init__(self)
        self.Payload = Payload
        self.Data = {}
        self.Output = {}
        self.Tasks = Tasks
        self.aList = []
    def run(self):
        for ID, job in self.Tasks.Jobs.items():
            self.aList.append(ID)
        for ID in self.aList:
            if self.Tasks.Jobs[ID].IsComplete():
                self.fullpath = self.Tasks.WorkData["sTargetDir"] + ID
                if os.path.isdir(self.fullpath):
                    shutil.rmtree(os.path.normpath(self.fullpath), onerror=self.remove_readonly)
                    del self.Tasks.Jobs[ID]
                    logging.debug("Removing completed Job:%s", ID)
                else:
                    logging.debug("%s is not a folder",self.fullpath)
            else:
                logging.debug("Job is not complete yet:%s", ID)
class c_remove_incomplete_tasks(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self, Payload, Tasks):
        threading.Thread.__init__(self)
        self.Payload = Payload
        self.Data = {}
        self.Output = {}
        self.Tasks = Tasks
        self.aList = []
    def run(self):
        for ID, job in self.Tasks.Jobs.items():
            self.aList.append(ID)
        for ID in self.aList:
            if not self.Tasks.Jobs[ID].IsComplete() and not self.Tasks.Jobs[ID].active:
                self.fullpath = self.Tasks.WorkData["sTargetDir"] + ID
                if os.path.isdir(self.fullpath):
                    shutil.rmtree(os.path.normpath(self.fullpath),onerror=self.remove_readonly)
                    del self.Tasks.Jobs[ID]
                    logging.debug("Removing incomplete Job:%s", ID)
                else:
                    logging.debug("%s is not a folder",self.fullpath)
            else:
                logging.debug("Job is still active:" + ID)

class c_restart_task(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self, Payload, Tasks):
        threading.Thread.__init__(self)
        self.ID = Payload
        self.Data = {}
        self.Output = {}
        self.Tasks = Tasks
    def run(self):
        # self.Output["status"] = "test"
        # self.request.sendall(bytes(json.dumps(self.Output),'utf-8'))
        if ID in self.Tasks.Jobs:
            if self.Tasks.Jobs[ID].state == "ready":
                if self.Tasks.Jobs[ID].active == False:
                    self.Tasks.Jobs[ID].active = True
                    #self.Tasks.Jobs[ID].workerlist = {}
                    self.Tasks.Jobs[ID].ResetFileStatus()
                    self.Tasks.Jobs[ID].progress = -1
                    for folder in os.listdir(Tasks.WorkData["sTargetDir"] + ID):
                        self.fullpath = self.Tasks.WorkData["sTargetDir"] + ID + "/" + folder + "/"
                        if os.path.isdir(self.fullpath):
                            shutil.rmtree(os.path.normpath(self.fullpath),onerror=self.remove_readonly)
                        else:
                            logging.debug("%s is not a folder",self.fullpath)
                    self.Tasks.Jobs[ID].progress = self.Tasks.Jobs[ID].GetCurrentProgress()
                    logging.debug("Task is being restarted: %s. Remember to put the tasks on the queue again", ID)
                    self.WriteJob(self.Tasks,ID)
                    #Tasks.task_queue.put(Tasks.Jobs[ID])
                else:
                    logging.debug("Task is already active:%s", ID)
            else:
                self.Output["status"] = "Task is busy. Try again when it is ready"
                # Tasks.syncserver_client.m_reply(self.Output,self.request)
        else:
            self.Output["status"] = "Task does not exist"
            # Tasks.syncserver_client.m_reply(self.Output,self.request)

class c_modify_task(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self, ID, Payload, Tasks):
        threading.Thread.__init__(self)
        self.ID = ID
        self.Payload = Payload
        self.Data = {}
        self.Output = {}
        self.Tasks = Tasks
    def run(self):
        if not self.Tasks.Jobs[ID].active and self.Tasks.Jobs[ID].type == "local":
            logging.debug("Getting files and storing their sizes")
            self.incomingFiles = self.FileExpand(self.ID, self.Payload)
            self.Tasks.Jobs[self.ID].state = "busy"

            if len(self.Tasks.Jobs[self.ID].filelist) < len(self.incomingFiles):
                #print("there are LESS files in current list")
                #if there is less files in the current list then iterate over the current list against the incoming files. If file in current list
                #exist in incoming files, then keep file. If file in current list does not exist in incoming list, then mark for deletion.
                for file in Tasks.Jobs[self.ID].filelist:
                    self.Tasks.Jobs[self.ID].filelist[file].delete = True
                    if file in self.incomingFiles == True:
                        self.Tasks.Jobs[self.ID].filelist[file].delete = False


                    if self.Tasks.Jobs[self.ID].filelist[file].delete == True:
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

                self.Tasks.Jobs[self.ID].filelist = self.incomingFiles
            else:
                #print("there are MORE files in current list")
                #if there are more files in the current list than in the incoming list, then iterate over the incoming list. If the file in the incoming list
                #exists in the current list AND the file in the current list has been marked as copied, then keep the file. If it does not exists

                for file in Tasks.Jobs[self.ID].filelist:
                    self.Tasks.Jobs[self.ID].filelist[file].delete = True

                for file in self.incomingFiles:
                    #if incoming file exists in the current filelist
                    if file in Tasks.Jobs[self.ID].filelist:
                        #mark the file in the current list to NOT to be deleted
                        self.Tasks.Jobs[self.ID].filelist[file].delete = False

                for file in self.Tasks.Jobs[self.ID].filelist:
                    if self.Tasks.Jobs[self.ID].filelist[file].delete:
                        self.head,self.tail = os.path.splitdrive(file)
                        self.dstfile = os.path.normpath(self.Tasks.WorkData["sTargetDir"] + self.ID + "/" + self.tail)
                        if os.path.exists(self.dstfile):
                            logging.debug("deleting:%s",self.dstfile)
                            shutil.rmtree(self.dstfile,onerror=self.remove_readonly)
                            #also remove folder if the folder is empty.
                            #check if the directory that the file was in can be removed. If it can be removed then remove it.
                            if len(os.listdir(os.path.dirname(self.dstfile)))== 0:
                                logging.debug("Removing directory:%s",os.path.dirname(self.dstfile))
                                os.rmdir(os.path.dirname(self.dstfile))


                self.Tasks.Jobs[self.ID].filelist = self.incomingFiles

            self.Tasks.Jobs[self.ID].state = "ready"
            self.Tasks.Jobs[self.ID].active = False
            logging.debug("modified task: %s",self.ID)
            self.WriteJob(self.Tasks,self.ID)