import stat
import os
import dataclasses
import socket
import select
import json
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

class c_HelperFunctions():
    def m_process_tasks_from_syncserver(self, Payload, Tasks):
        self.Payload = json.loads(Payload)

        if len(self.Payload)>0:
            Tasks.Order = []
            self.counter = 1

            for Data in self.Payload:

                Tasks.Order.append(Data["ID"])
                if not Data["ID"] in Tasks.Jobs:
                    Tasks.Jobs[Data["ID"]] = dataclasses.c_Task(Data["ID"])
                    print(Data["Data"]["type"])
                    Tasks.Jobs[Data["ID"]].type = Data["Data"]["type"]
                Tasks.Jobs[Data["ID"]].order = self.counter
                Tasks.Jobs[Data["ID"]].progress = Data["Data"]["progress"]
                Tasks.Jobs[Data["ID"]].metadata = Data["Data"]["metadata"]
                self.counter += 1

            # if Data["report"]:
            for ID in Tasks.Order:
                if Tasks.Jobs[ID].type == "local":
                    logging.debug("Loading LOCAL task:[%s] %s : %s percent complete", Tasks.Jobs[ID].order, ID, Tasks.Jobs[ID].progress)
                else:
                    logging.debug("Loading GLOBAL task from sync server:[%s] %s. %s percent complete", Tasks.Jobs[ID].order, ID, Tasks.Jobs[ID].progress)

    def m_reply(self,payload,sock):
        self.payload = json.dumps(payload)

        self.SizeOfData = len(self.payload)
        self.payload = str(self.SizeOfData) + "|" + self.payload
       #logging.debug("Replying with = %s",self.payload)
        sock.sendall(bytes(self.payload,encoding='utf8'))
    def m_SerializeTask(self,Task,bReport=True):
        self.output = []
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

        self.output.append(self.TaskData)
        return json.dumps(self.output)

    def m_SerialiseTaskList(self, TaskIDs, bReport=True):

        self.output = []
        for Task in TaskIDs:
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

            self.output.append(self.TaskData)
        return json.dumps(self.output)
    def m_SerialiseSyncTasks(self, Tasks, bReport=True):
        self.Tasks = Tasks
        self.output = []
        for ID in self.Tasks.Order:
            if ID in self.Tasks.Jobs:
                self.TaskData = {}
                self.TaskData["ID"] = ID
                self.TaskData["report"] = bReport

                self.TaskData["Data"] = {}

                if self.Tasks.Jobs[ID].type == "local":
                    self.TaskData["Data"]["progress"] = self.Tasks.Jobs[ID].GetCurrentProgress()
                else:
                    self.TaskData["Data"]["progress"] = self.Tasks.Jobs[ID].progress
                self.TaskData["Data"]["type"] = "global"
                self.TaskData["Data"]["metadata"] = self.Tasks.Jobs[ID].metadata
                self.TaskData["Data"]["filelist"] = {}
                self.TaskData["Data"]["filelistOrder"] = self.Tasks.Jobs[ID].filelistOrder
                for file in self.Tasks.Jobs[ID].filelistOrder:
                    self.FileData = {}
                    self.FileData["progress"] = self.Tasks.Jobs[ID].filelist[file].progress
                    self.FileData["copied"] = self.Tasks.Jobs[ID].filelist[file].copied
                    self.FileData["delete"] = self.Tasks.Jobs[ID].filelist[file].delete
                    self.FileData["size"] = self.Tasks.Jobs[ID].filelist[file].size
                    self.FileData["uploaded"] = self.Tasks.Jobs[ID].filelist[file].uploaded
                    self.TaskData["Data"]["filelist"][file] = self.FileData

                self.output.append(self.TaskData)
        return json.dumps(self.output)
    def m_receive_all(self, sock):
        self.HeaderLength = 32
        self.PackageLength = 1024
        self.data = ""
        self.data = sock.recv(self.HeaderLength).decode('utf8')

        if self.data != "":

            self.length = self.data.split("|")[0]
            self.SizeOfHeader = len(self.length)+1 # +1 is the "|" character
            self.data = self.data.split("|")[1]
            self.currentlength = self.HeaderLength-self.SizeOfHeader
            while True:
                #logging.debug(self.currentlength)
                if (int(self.length)-self.currentlength)<self.PackageLength:
                    #logging.debug("reading half:%s",int(self.length)-self.currentlength)
                    if int(self.length)-self.currentlength < 0:
                        self.line = sock.recv(int(self.length)).decode('utf8')
                    else:
                        self.line = sock.recv((int(self.length)-self.currentlength)).decode('utf8')
                    #logging.debug(self.line)
                else:
                    #logging.debug("reading full")
                    self.line = sock.recv(self.PackageLength).decode('utf8')

                self.data += self.line
                self.currentlength += len(self.line)

                if self.currentlength >= int(self.length):
                    break

            #logging.debug(len(self.data))
        return self.data
    def m_Is_ID_In_List(self,list,ID):
        self.bIsFound = False
        for CheckID in list:
            if CheckID == ID:
                self.bIsFound = True
                return True

        return self.bIsFound
    def StringToBool(self,input):
        if input == "True":
            return True
        else:
            return False
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
