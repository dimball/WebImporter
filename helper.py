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
class Client():
    def __init__(self, ip, port, Tasks):
        self.ip = ip
        self.port = port
        self.Tasks = Tasks
        self.m_send(self.m_create_data("/syncserver/v1/server/register",self.Tasks.WorkData["serverport"]))
    def m_SerialiseSyncTasks(self):
        self.output = []
        for ID in self.Tasks.Order:
            if ID in self.Tasks.Jobs:
                self.TaskData = {}
                self.TaskData["ID"] = ID
                self.TaskData["Data"] = {}
                self.TaskData["Data"]["progress"] = self.Tasks.Jobs[ID].progress
                self.TaskData["Data"]["metadata"] = self.Tasks.Jobs[ID].metadata
                self.output.append(self.TaskData)
        return json.dumps(self.output)

    def m_receive_all(self, sock):
        self.data = ""
        self.part = None
        while self.part != "":
            self.part = sock.recv(4096).decode('utf8')
            self.data += self.part
            if self.part == "":
                break
        return self.data

    def m_create_data(self, command, payload=0):
        self.data = {}
        self.data["command"] = command
        self.data["payload"] = payload
        return json.dumps(self.data)

    def m_send(self, payload):
        # SOCK_STREAM == a TCP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.setblocking(0)  # optional non-blocking
        self.sock.connect((self.ip, int(self.port)))
        logging.debug("sending data => %s", (payload))
        try:
            self.sock.send(bytes(payload, 'utf8'))
        except:
            print("")


        #sock.setblocking(0)
        self.ready = select.select([self.sock],[],[],2)
        if self.ready[0]:

            self.reply = self.m_receive_all(self.sock)
            if len(self.reply)>0:
                return self.reply
        else:
            print("request timed out")

        if self.sock != None:
            self.sock.close()

class c_HelperFunctions():
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
        for FileObj in self.Payload:
            if FileObj["type"] == "folder":
                self.aFilePaths = self.get_filepaths(FileObj["data"])
                for f in self.aFilePaths:
                    self.path = os.path.normpath(f)

                    self.FileList[self.path] = dataclasses.c_file(os.path.getsize(f))
            elif FileObj["type"] == "file":
                self.path = os.path.normpath(FileObj["data"])
                self.FileList[self.path] = dataclasses.c_file(os.path.getsize(f))

        return self.FileList
