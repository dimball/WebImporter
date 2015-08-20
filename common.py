import stat
import os
import dataclasses
import json
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

import urllib
class c_HelperFunctions():
    def m_UploadCompleteTasks(self, Tasks):
        while not Tasks.upload_queue.empty():
            Tasks.upload_queue.get()

        self.OrderCounter = 0
        for ID in Tasks.Order:
            if not Tasks.Jobs[ID].metadata and Tasks.Jobs[ID].progress == 100.0:
                for file in Tasks.Jobs[ID].filelist:
                    if not Tasks.Jobs[ID].filelist[file].uploaded:
                        self.priority = "medium"
                        if self.OrderCounter == 0:
                            self.priority = "high"
                        elif self.OrderCounter == 1:
                            self.priority = "medium"
                        elif self.OrderCounter > 1:
                            self.priority = "low"


                        self.uploadtask = dataclasses.c_uploadTask(file, Tasks.Jobs[ID], Tasks.Jobs[ID].filelist[file], self.priority)
                        Tasks.upload_queue.put(self.uploadtask)

                self.OrderCounter += 1




    def StringToBool(self,input):
        if input == "True":
            return True
        else:
            return False
    def m_show_tasks(self, Tasks):
        for ID in Tasks.Order:
            logging.debug("[%s] ID:%s Files:%s Progress:%s", Tasks.Jobs[ID].type, ID, len(Tasks.Jobs[ID].filelist), Tasks.Jobs[ID].progress )

    def m_StoreClient(self, origin, Tasks, sType, bUpdateTasks = True):
        parsed_origin = urllib.parse.urlparse(origin)
        logging.debug("Client logged in from: %s", parsed_origin.netloc + "/" + sType)
        if not parsed_origin.netloc in Tasks.clientlist:
            Tasks.clientlist[parsed_origin.netloc + "/" + sType] = self
        else:
            if bUpdateTasks:
                #if it is already in the dictionary then change all the existing tasks that has this command handler attached to it, to the new one.
                logging.debug(Tasks.Jobs)
                for ID in Tasks.Jobs:
                    logging.debug("ID is:%s", ID)
                    if Tasks.Jobs[ID].WSHandler == Tasks.clientlist[parsed_origin.netloc + "/" + sType]:
                        Tasks.Jobs[ID].WSHandler = self

    def m_NotifyClients(self,command, payload, Clients, Tasks=None):
        self.ClosedClients = []
        for client in Clients:
            try:
                if Tasks != None:
                    logging.debug("Sending tasks to:%s", Tasks.GetClientNameFromHandler(client))
                else:
                    logging.debug("Sending tasks to:%s", client)

                client.write_message(self.m_create_data(command, payload))
            except:
                self.ClosedClients.append(client)
                logging.debug("client closed")

        for client in self.ClosedClients:
            Clients.remove(client)

    def m_reply(self,payload,websock):
        #####payload comes in as a dictionary. NOT AS A STRING####

        self.payload = json.dumps(payload)
        #logging.debug("Replying with = %s",self.payload)
        websock.write_message(self.payload)
    def m_create_data(self, command, payload=0):
        self.data = {}
        self.data["command"] = command
        self.data["payload"] = payload
        return json.dumps(self.data)
    def m_SerialiseTaskList(self, aTaskJobs, Tasks, bReport=True):
        #this way we can add some extra information into the payload
        self.Data = {}
        self.Data["Order"] = Tasks.Order
        logging.debug("Serializing %s tasks", len(aTaskJobs))
        self.Data["TaskList"] = []
        for Task in aTaskJobs:
            self.TaskData = {}
            self.TaskData["ID"] = Task.TaskID
            self.TaskData["report"] = bReport

            self.TaskData["Data"] = {}

            if Task.type == "local":
                self.TaskData["Data"]["progress"] = Task.GetCurrentProgress()
            else:
                self.TaskData["Data"]["progress"] = Task.progress
            self.TaskData["Data"]["type"] = "global"
            self.TaskData["Data"]["metadata"] = Task.metadata
            self.TaskData["Data"]["filelist"] = {}
            self.TaskData["Data"]["filelistOrder"] = Task.filelistOrder
            for file in Task.filelistOrder:
                self.FileData = {}
                self.FileData["progress"] = Task.filelist[file].progress
                self.FileData["copied"] = Task.filelist[file].copied
                self.FileData["delete"] = Task.filelist[file].delete
                self.FileData["size"] = Task.filelist[file].size
                self.FileData["uploaded"] = Task.filelist[file].uploaded
                self.TaskData["Data"]["filelist"][file] = self.FileData

            self.Data["TaskList"].append(self.TaskData)
        return self.Data
    def m_deSerializeTaskList(self, Payload, Tasks, WSHandler=None):
        #converts the incoming data from json format into the internal data structure of classes
        Tasks.Order = Payload["Order"]
        logging.debug("deSerializing %s tasks", len(Payload["TaskList"]))
        for Task in Payload["TaskList"]:
            if not Task["ID"] in Tasks.Jobs:
                Tasks.Jobs[Task["ID"]] = dataclasses.c_Task(Task["ID"])
                Tasks.Jobs[Task["ID"]].type = Task["Data"]["type"]

            Tasks.Jobs[Task["ID"]].progress = Task["Data"]["progress"]
            Tasks.Jobs[Task["ID"]].metadata = Task["Data"]["metadata"]
            Tasks.Jobs[Task["ID"]].filelistOrder = Task["Data"]["filelistOrder"]


            for file in Tasks.Jobs[Task["ID"]].filelistOrder:
                Tasks.Jobs[Task["ID"]].filelist[file] = dataclasses.c_file(Task["Data"]["filelist"][file]["size"])
                Tasks.Jobs[Task["ID"]].filelist[file].progress = Task["Data"]["filelist"][file]["progress"]
                Tasks.Jobs[Task["ID"]].filelist[file].copied = self.StringToBool(Task["Data"]["filelist"][file]["progress"])
                Tasks.Jobs[Task["ID"]].filelist[file].delete = self.StringToBool(Task["Data"]["filelist"][file]["delete"])
                Tasks.Jobs[Task["ID"]].filelist[file].uploaded = self.StringToBool(Task["Data"]["filelist"][file]["uploaded"])

            if WSHandler != None:
                Tasks.Jobs[Task["ID"]].WSHandler = WSHandler

            logging.debug("deSerialized task:%s", Task["ID"])
    # def m_receive_all(self, sock):
    #     self.HeaderLength = 32
    #     self.PackageLength = 1024
    #     self.data = ""
    #
    #     self.data = sock.recv(self.HeaderLength).decode('utf8')
    #
    #     if self.data != "":
    #
    #         self.length = self.data.split("|")[0]
    #         self.SizeOfHeader = len(self.length)+1 # +1 is the "|" character
    #         self.data = self.data.split("|")[1]
    #         self.currentlength = self.HeaderLength-self.SizeOfHeader
    #         while True:
    #             #logging.debug(self.currentlength)
    #             if (int(self.length)-self.currentlength)<self.PackageLength:
    #                 #logging.debug("reading half:%s",int(self.length)-self.currentlength)
    #                 if int(self.length)-self.currentlength < 0:
    #                     self.line = sock.recv(int(self.length)).decode('utf8')
    #                 else:
    #                     self.line = sock.recv((int(self.length)-self.currentlength)).decode('utf8')
    #                 #logging.debug(self.line)
    #             else:
    #                 #logging.debug("reading full")
    #                 self.line = sock.recv(self.PackageLength).decode('utf8')
    #
    #             self.data += self.line
    #             self.currentlength += len(self.line)
    #
    #             if self.currentlength >= int(self.length):
    #                 break
    #
    #         #logging.debug(len(self.data))
    #     return self.data
    def m_Is_ID_In_List(self,list,ID):
        self.bIsFound = False
        for CheckID in list:
            if CheckID == ID:
                self.bIsFound = True
                return True

        return self.bIsFound

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
    def WriteJob(self,Tasks, ID):
        TaskObject = Tasks.Jobs[ID]
        Task = ET.Element("Task")
        Task.set("ID",str(ID))
        Task.set("state",str(TaskObject.state))
        Task.set("active",str(TaskObject.active))
        Task.set("order",str(TaskObject.order))
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
        self.dstfile = Tasks.WorkData["sTargetDir"] + str(ID) + "/" + str(ID) + ".xml"
        if not os.path.exists(os.path.dirname(self.dstfile)):
            os.makedirs(os.path.dirname(self.dstfile))
        Tree.write(self.dstfile, xml_declaration=True, encoding='utf-8', method="xml")
    def get_xmljobs(self,Tasks):
        self.xmljobs = []
        for ID in os.listdir(Tasks.WorkData["sTargetDir"]):
            self.xmljob = (Tasks.WorkData["sTargetDir"] + "/" +ID + "/" + ID + ".xml")
            if os.path.exists(self.xmljob):
                self.xmljobs.append(self.xmljob)
        return self.xmljobs

    def get_filepaths(self,directory,pattern="*.*"):
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
                self.bAdd = None
                if pattern != "*.*":
                    self.head,self.tail = os.path.split(filename)
                    self.head, self.tail = (os.path.splitext(self.tail))
                    if ("." + pattern.split(".")[1]) == self.tail:
                        self.bAdd = True
                    else:
                        self.bAdd = False
                else:
                    self.bAdd = True


                # Join the two strings in order to form the full filepath.
                filepath = os.path.join(root, filename)
                if self.bAdd == True:
                    file_paths.append(filepath)  # Add it to the list.

        return file_paths  # Self-explanatory.
    def FileExpand(self,ID,Payload):
        self.ID = ID
        self.Payload = Payload
        self.FileList = {}
        self.FileListOrder = []

        for FileObj in self.Payload:
            if FileObj["type"] == "folder":
                self.aFilePaths = self.get_filepaths(FileObj["data"])
                for f in self.aFilePaths:
                    self.path = os.path.normpath(f)

                    self.FileList[self.path] = dataclasses.c_file(os.path.getsize(f))

                    self.FileListOrder.append(self.path)
            elif FileObj["type"] == "file":
                self.path = os.path.normpath(FileObj["data"])
                self.FileList[self.path] = dataclasses.c_file(os.path.getsize(f))
                self.FileListOrder.append(self.path)
        return [self.FileList,self.FileListOrder]
