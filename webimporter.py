import multiprocessing as mp
import logging
import socket
logger = mp.log_to_stderr(logging.DEBUG)
import os
import uuid
import shutil
import time
import json

#The copy worker is a class that runs constantly on a thread. If there is something in the task queue, then it will do it,
#if not then it will lie idle until there is something there
def CopyWorker(task_queue, dict_Jobs, dict_Data, worker_name):
    print("Copy worker Started:" + worker_name)
    while True:
        next_task = task_queue.get()
        if next_task is None:
             break

        ID = next_task["taskid"]
        Path = next_task["Payload"]

        print("[" + worker_name + "] Processing task:[" + ID + "]" + Path + "==>" + dict_Data["sTargetDir"])
        # Run the above function and store its results in a variable.
        full_file_paths = get_filepaths(Path)  # List which will store all of the full filepaths.
        for f in range(dict_Jobs[ID]["PauseIndex"], len(full_file_paths)):
            #copy the actual file here
            srcfile = full_file_paths[f]
            head,tail = os.path.splitdrive(srcfile)
            dstfile = os.path.normpath("d:/destination/" + ID + "/" + tail)
            if not os.path.exists(os.path.dirname(dstfile)):
                os.makedirs(os.path.dirname(dstfile))

            #shutil.copy2(srcfile, dstfile)
            print(srcfile + " ==> " + (dstfile))
            dProxy = dict_Jobs[ID]
            dProxy["progress"] = f/(len(full_file_paths)-1)*100
            dict_Jobs[ID] = dProxy

            if dict_Jobs[ID]["active"] == False:
                dProxy = dict_Jobs[ID]
                dProxy["PauseIndex"] = f
                dict_Jobs[ID] = dProxy
                print("Aborting (setting task to inactive) paused at:" + dict_Jobs[ID]["PauseIndex"])
                break
            if dict_Jobs[ID]["progress"] == 100.0:
                dProxy = dict_Jobs[ID]
                dProxy["active"] = False
                dict_Jobs[ID] = dProxy
                print("Finished job")
            time.sleep(0.01)
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
def CreateTask(ID,Payload):
    Task = {}
    Task["taskid"] = str(ID)
    Task["Payload"] = Payload
    Task["active"] = False
    Task["progress"] = 0
    Task["PauseIndex"] = -1
    return Task
def TCPServer(socket,task_queue, dict_Jobs):
    while True:
        client, address = socket.accept()
        #logger.debug("{u} connected".format(u=address))
        data = client.recv(16348).decode('utf-8')
        data = json.loads(data)
        ID = uuid.uuid4()
        Command = data["command"].lower()
        Payload = data["payload"]
        if Command == "create_copytask":
            for pl in Payload:
                print("Creating task : " + str(ID))
                dict_Jobs[str(ID)] = CreateTask(str(ID),pl)

        client.send(bytes(str(ID) ,'utf-8'))
        elif Command == "start_task":
            if Payload in dict_Jobs:
                if dict_Jobs[Payload]["active"] == False:
                    dProxy = dict_Jobs[Payload]
                    dProxy["active"] = True
                    dProxy["progress"] = 0
                    dict_Jobs[Payload] = dProxy
                    task_queue.put(dict_Jobs[Payload])
                else:
                    client.send(bytes("Task already started",'utf-8'))
            else:
                client.send(bytes("Task does not exist",'utf-8'))
        elif Command == "resume_job":
            if Payload in dict_Jobs:
                if dict_Jobs[Payload]["active"] == False:
                    print("Resuming Job : " + str(Payload))
                    dProxy = dict_Jobs[Payload]
                    dProxy["active"] = True
                    dProxy["started"] = True
                    dict_Jobs[Payload] = dProxy
                    print(dict_Jobs[Payload])
                    task_queue.put(dict_Jobs[Payload])
                else:
                    client.send(bytes("Cannot resume as it has not been paused",'utf-8'))
            else:
                client.send(bytes("Cannot resume as task does not exist",'utf-8'))
        elif Command == "status":
            if Payload in dict_Jobs:
                if dict_Jobs[Payload]["active"]:
                    client.send(bytes(str(dict_Jobs[Payload]["progress"]),'utf-8'))
                else:
                    if dict_Jobs[Payload]["progress"] == 100.0:
                        print("Job complete")
                        client.send(bytes("Job Complete",'utf-8'))
                    else:
                        if dict_Jobs[Payload]["progress"] < 100.0 and dict_Jobs[Payload]["progress"] > 0:
                            print("Job paused")
                            client.send(bytes("Job paused",'utf-8'))
                        else:
                            print("Not started")
                            client.send(bytes("Not Started",'utf-8'))
        elif Command == "get_tasks":
            JobIDString = ""
            for TaskID,JobObject in dict_Jobs.items():
                JobIDString += (TaskID + "|")
            client.send(bytes(JobIDString,'utf-8'))
        elif Command == "get_active_tasks":
            JobIDString = ""
            for TaskID,JobObject in dict_Jobs.items():
                if JobObject.active == True:
                    JobIDString += (TaskID + "|")
            client.send(bytes(JobIDString,'utf-8'))
        elif Command == "pause_job":
            dProxy = dict_Jobs[Payload]
            dProxy["active"] = False
            dict_Jobs[Payload] = dProxy
            time.sleep(0.5)
            client.send(bytes('Paused job: ' + Payload,'utf-8'))
        elif Command == "remove_completed_tasks":
            for pl,job in dict_Jobs.items():
                if dict_Jobs[pl]["progress"] == 100.0:
                    del dict_Jobs[pl]
                    print("Removing completed Job:" + pl)
                else:
                    print("Job is not complete yet:" + pl)
        elif Command == "remove_incomplete_tasks":
            for pl,job in dict_Jobs.items():
                if dict_Jobs[pl]["progress"] < 100.0 and dict_Jobs[pl]["active"] == False:
                    del dict_Jobs[pl]
                    print("Removing incomplete Job:" + pl)
                else:
                    if dict_Jobs[pl]["progress"] == 100.0:
                        print("Job is completed. Not removing:" + pl)
                    else:
                        print("Job is still active:" + pl)
        elif Command == "modify_task":
            ID = Payload.split("@")[0]
            Payload = Payload.split("@")[1]
            if dict_Jobs[Payload]["active"] == False:
                dProxy = dict_Jobs[Payload]

                OldPayload = dProxy[Payload]["Payload"]
                dProxy[Payload]["Payload"]["PauseIndex"] = 0
                dProxy[Payload]["Payload"] = Payload
                dict_Jobs[Payload] = dProxy

                print("Task [" + ID + "] has been modified:" + Payload)
                #some logic has to happen here to either remove the previous content, or do a smart diff to check
                #if that are in the new payload has been copied already and skip those. Other files that are not in the
                #new payload must be deleted.



        client.close()
if __name__ == '__main__':
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serversocket.bind(('',9090))
    serversocket.listen(5)
    Manager = mp.Manager()

    dict_WorkData = Manager.dict()
    dict_WorkData['abort'] = -1
    dict_WorkData["num_copyworkers"] = 5
    dict_WorkData["sTargetDir"] = "d:/destination"

    dict_Jobs = Manager.dict()
    task_queue = Manager.Queue()
    CopyWorkerPool = mp.Pool(processes=dict_WorkData["num_copyworkers"])
    for i in range(dict_WorkData["num_copyworkers"]):
        CopyWorkerPool.apply_async(CopyWorker, (task_queue, dict_Jobs, dict_WorkData, "copy_Worker_" + str(i)))
    TCPWorker = mp.Process(target=TCPServer, args=(serversocket,task_queue, dict_Jobs))
    TCPWorker.start()
    TCPWorker.join()
