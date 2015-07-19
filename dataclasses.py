import queue
class c_file():
    def __init__(self):
        self.progress = 0
        self.copied = False
        self.delete = False
        self.uploaded = False
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

class c_data():
    def __init__(self):
        self.lock = False
        self.Jobs = {}
        self.task_queue = queue.Queue()
        self.WorkData = {}
        self.shutdown = False