import threading
import os
import time
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
class c_FileProgressMonitor(threading.Thread):
    def __init__(self,srcfile, tgtfile, dict_Jobs, ID,worker_name):
        threading.Thread.__init__(self)
        self.worker_name = worker_name
        self.ID = ID
        self.dict_Jobs = dict_Jobs
        self.srcfile = srcfile
        self.tgtfile = tgtfile
        self.size = os.path.getsize(self.srcfile)
        if self.dict_Jobs[self.ID].filelist[self.srcfile].size != self.size:
            logging.debug("Original size is different from current size. Updating entry")
            self.dict_Jobs[self.ID].filelist[self.srcfile].size = self.size

        self.name = "Progress monitor"

        #if self.worker_name in self.dict_Jobs[self.ID].workerlist == False:
        #    print("resetting worklist")
        self.dict_Jobs[self.ID].workerlist[self.worker_name] = {}

        self.dict_Jobs[self.ID].workerlist[self.worker_name][self.srcfile] = 0.0
    def run(self):
        while os.path.exists(self.tgtfile) == False:
            #logging.debug("waiting for file to exist at target destination")
            time.sleep(0.05)
#        logging.debug("File check started:%s", self.srcfile)

        self.currentSize = os.path.getsize(self.tgtfile)
        #self.dict_Jobs[self.ID].workerlist[self.worker_name]["progress"] = self.ProgressData["progress"]

        while self.currentSize < self.size:
            self.currentSize = os.path.getsize(self.tgtfile)
            self.dict_Jobs[self.ID].workerlist[self.worker_name][self.srcfile] = ((self.currentSize/self.size)*100.0)
            self.dict_Jobs[self.ID].filelist[self.srcfile].progress = ((self.currentSize/self.size)*100.0)
            print(self.dict_Jobs[self.ID].filelist[self.srcfile].progress)
            if self.dict_Jobs[self.ID].active == False:
                logging.debug("Aborted file check")
                break
            time.sleep(0.01)

        #if it has broken out of the loop then it should be 100.0

        self.dict_Jobs[self.ID].workerlist[self.worker_name][self.srcfile] = 100.0
