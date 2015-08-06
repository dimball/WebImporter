import queue
class c_file():
    def __init__(self,size):
        self.progress = 0
        self.copied = False
        self.delete = False
        self.uploaded = False
        self.size = size


class c_Task():
    def __init__(self, ID):
        self.WSHandler = None
        self.order = 0
        self.globalorder = 0
        self.TaskID = str(ID)
        self.state = "ready"
        self.active = False
        self.progress = 0
        #self.workerlist = {}
        self.filelist = {}
        self.filelistOrder = []
        self.metadata = {}
        self.type = "local"


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
class c_basedata():
    def __init__(self):
        self.Jobs = {}
        self.Order = []
        self.WorkData = {}
        self.shutdown = False
        self.CommandClients = []
        self.ProgressClients = []
        self.MainLoop = None
        self.clientlist = {}

    def GetClientNameFromHandler(self, Handle):
        for host, handler in self.clientlist.items():
            if handler == Handle:
                return host
        return "None"
    def RemoveHandlerFromClientList(self, Handle):
        self.NewList = {}
        for host, handler in self.clientlist.items():
            if handler != Handle:
                self.NewList[host] = handler

        self.clientlist = self.NewList




class c_SyncServerData(c_basedata):
    def __init__(self):
        c_basedata.__init__(self)

class c_ServerData(c_basedata):
    def __init__(self):
        c_basedata.__init__(self)
        self.task_queue = queue.Queue()
        self.LineManagers = []
        self.syncserver_client = None
        self.syncserver_progress_client = None
        self.ready = True
