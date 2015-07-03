import multiprocessing as mp
import logging
import socket
#logger = mp.log_to_stderr(logging.DEBUG)
import os
import uuid
import shutil
import time
#The copy worker is a class that runs constantly on a thread. If there is something in the task queue, then it will do it,
#if not then it will lie idle until there is something there
def CopyWorker(task_queue, dict_WorkData, worker_name):
    print("Copy worker Started:" + worker_name)
    while True:
        next_task = task_queue.get()

        if next_task is None:
             break
        ID = next_task.split("|")[0]
        Path = next_task.split("|")[1]
        print("[" + worker_name + "] Processing task:[" + ID + "]" + Path + "==>" + dict_WorkData["sTargetDir"])
        # Run the above function and store its results in a variable.
        full_file_paths = get_filepaths(Path)  # List which will store all of the full filepaths.

        for f in range(len(full_file_paths)):
            if f > dict_WorkData["paused_" + ID]:
                #copy the actual file here
                srcfile = full_file_paths[f]
                head,tail = os.path.splitdrive(srcfile)
                dstfile = os.path.normpath("d:/destination/" + ID + "/" + tail)
                if not os.path.exists(os.path.dirname(dstfile)):
                    os.makedirs(os.path.dirname(dstfile))

                #shutil.copy2(srcfile, dstfile)
                print(srcfile + " ==> " + (dstfile))
                CurrentProgress = f/(len(full_file_paths)-1)*100
                dict_WorkData["progress_" + str(ID)] = str(CurrentProgress)
                if ("abort_" + ID) in dict_WorkData:
                    dict_WorkData["paused_" + ID] = f
                    print("aborting")
                    del dict_WorkData[("abort_" + ID)]
                    break


                time.sleep(0.1)

def get_filepaths(directory):
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
                # Join the two strings in order to form the full filepath.
                filepath = os.path.join(root, filename)
                file_paths.append(filepath)  # Add it to the list.

        return file_paths  # Self-explanatory.
class c_TCPServer():
    def __init__(self,socket,task_queue, dict_WorkData, dict_Jobs):
        print("TCP server started")
        self.task_queue = task_queue
        self.socket = socket
        self.dict_WorkData = dict_WorkData
        self.dict_Jobs = dict_Jobs
        self.run()
    def GenerateUUID(self):
        return uuid.uuid4()
    def run(self):
        while True:
            self.client, self.address = self.socket.accept()
            #logger.debug("{u} connected".format(u=self.address))
            self.data = self.client.recv(2048).decode('utf-8')
            self.data = self.data.split("|")
            self.ID = -1

            self.Command = self.data[0].lower()
            self.Payload = self.data[1]

            if self.Command == "copy":
                self.ID = self.GenerateUUID()
                print("Creating Job : " + str(self.ID))
                self.dict_WorkData["paused_" + str(self.ID)] = -1
                self.task_queue.put(str(self.ID) + "|" + self.Payload)
                self.dict_WorkData[str(self.ID)] = True
                self.dict_Jobs[str(self.ID)] = self.Payload
                self.client.send(bytes(str(self.ID) ,'utf-8'))
            if self.Command == "resume_job":
                print("Resuming Job : " + str(self.Payload))
                if ('paused_' + str(self.Payload)) in self.dict_WorkData:
                    if self.dict_WorkData["paused_" + str(self.Payload)] > 0:
                        self.task_queue.put(str(self.Payload) + "|"+ self.dict_Jobs[str(self.Payload)])
                        self.dict_WorkData[str(self.Payload)] = True
                        self.client.send(bytes("Resuming task:" + str(self.Payload),'utf-8'))
                    else:
                        self.client.send(bytes("Cannot resume as it has not been paused",'utf-8'))
                else:
                    print("in pause")
                    self.client.send(bytes("Cannot resume as it has not been paused",'utf-8'))

            elif self.Command == "status":
                if ("progress_" + str(self.Payload)) in self.dict_WorkData:
                    self.RecievedData = str(self.dict_WorkData["progress_" + str(self.Payload)])
                    if self.RecievedData == "100.0":
                        del self.dict_WorkData["progress_" + str(self.Payload)]
                        del self.dict_WorkData[self.Payload]
                        del self.dict_Jobs[self.Payload]
                        if "paused_" + str(self.Payload) in self.dict_WorkData:
                            del self.dict_WorkData["paused_" + str(self.Payload)]

                else:
                    self.RecievedData = "No Data"

                self.client.send(bytes(self.RecievedData,'utf-8'))
            elif self.Command == "getjobs":
                self.JobIDString = ""
                for job,path in self.dict_Jobs.items():
                    self.JobIDString += (job + "|")
                self.client.send(bytes(self.JobIDString,'utf-8'))
            elif self.Command == "abort_job":
                self.dict_WorkData["abort_" + self.Payload] = self.Payload
                del self.dict_WorkData[self.Payload]
                del self.dict_Jobs[self.Payload]
                self.client.send(bytes('Aborted job:' + self.Payload,'utf-8'))
            elif self.Command == "abort_queue":
                for job,path in self.dict_Jobs.items():
                    self.dict_WorkData["abort_" + job] = True


                for job,path in self.dict_Jobs.items():
                    del self.dict_WorkData[job]
                    del self.dict_Jobs[job]
            elif self.Command == "pause_job":
                self.dict_WorkData["abort_" + str(self.Payload)] = True
                self.client.send(bytes('Paused job:' + self.Payload,'utf-8'))


            self.client.close()

class c_Main():
    def __init__(self):
        self.serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.serversocket.bind(('',9090))
        self.serversocket.listen(5)
        self.Manager = mp.Manager()

        self.dict_WorkData = self.Manager.dict()
        self.dict_WorkData['abort'] = -1
        self.dict_WorkData["num_copyworkers"] = 5
        self.dict_WorkData["sTargetDir"] = "d:/destination"

        self.list_Jobs = self.Manager.dict()
        self.task_queue = self.Manager.Queue()

        self.CopyWorkerPool = mp.Pool(processes=self.dict_WorkData["num_copyworkers"])
        for i in range(self.dict_WorkData["num_copyworkers"]):
            self.CopyWorkerPool.apply_async(CopyWorker, (self.task_queue, self.dict_WorkData, "copy_Worker_" + str(i)))
        c_TCPServer(self.serversocket,self.task_queue, self.dict_WorkData, self.list_Jobs)
if __name__ == '__main__':
    MainApp = c_Main()

