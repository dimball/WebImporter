#!/usr/bin/python

import socket
import time
import select
import json
import os
import random
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

# def recvall(sock):
#     data = ""
#     part = None
#     while part != "":
#         part = sock.recv(1024).decode('utf8')
#         data += part
#         if part == "":
#             break
#     return data
def m_receive_all(sock):
    HeaderLength = 64
    PackageLength = 2048
    data = ""
    data = sock.recv(HeaderLength).decode('utf8')
    if data != "":
        #logging.debug(self.data)
        length = data.split("|")[0]
        SizeOfHeader = len(length)+1 # +1 is the "|" character
        data = data.split("|")[1]

        currentlength = HeaderLength-SizeOfHeader
        while True:
            #logging.debug(self.currentlength)
            if (int(length)-currentlength)<PackageLength:
                #logging.debug("reading half:%s",int(self.length)-self.currentlength)
                if (int(length)-currentlength) < 0:
                    line = sock.recv(int(length)).decode('utf8')
                else:
                    line = sock.recv((int(length)-currentlength)).decode('utf8')
                #logging.debug(self.line)
            else:
                #logging.debug("reading full")
                line = sock.recv(PackageLength).decode('utf8')

            data += line
            currentlength += len(line)

            if currentlength >= int(length):
                break
    return data
def client(string):
    HOST, PORT = 'localhost', 9090
    # SOCK_STREAM == a TCP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #sock.setblocking(0)  # optional non-blocking
    sock.connect((HOST, PORT))

    SizeOfData = len(string)
    Payload = str(SizeOfData) + "|" + string

    print("sending data => " + Payload)
    sock.sendall(bytes(Payload, 'utf8'))


    #sock.setblocking(0)
    #ready = select.select([sock],[],[],2)
    reply = m_receive_all(sock)
    if len(reply)>0:
        return reply

    if sock != None:
        sock.close()
    #return reply

def CreateData(Command,Payload=0):
    data = {}
    data["command"] = Command
    data["payload"] = Payload
    return json.dumps(data)
def create_copytask():
    tree = ET.ElementTree(file="./client_config.xml")
    config = tree.getroot()
    JobList = []
    for path in config.find("source").findall("path"):
        JobList.append(path.text)
    aPayload = []


    for pl in JobList:
        payload = {}
        head, tail = os.path.split(pl)
        if len(tail.split(".")) > 1:
            payload["type"] = "file"
        else:
            payload["type"] = "folder"

        payload["data"] = pl
        aPayload.append(payload)

    client(CreateData('/webimporter/v1/queue/task/create', aPayload))
def modify_task(slot):
    dJobs = client(CreateData('/webimporter/v1/queue/task/get_all'))
    JobList = ['c:/Data3']
    aPayload = []
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            if slot < len(aJobs):
                for pl in JobList:
                    payload = {}
                    head, tail = os.path.split(pl)
                    if len(tail.split(".")) > 1:
                        payload["type"] = "file"
                    else:
                        payload["type"] = "folder"
                    payload["data"] = pl
                    aPayload.append(payload)

                data = {}
                data["ID"] = aJobs[slot]
                data["Payload"] = aPayload
                response = client(CreateData('/webimporter/v1/queue/task/modify',data))
                print(response)
        else:
            print("no jobs on server")

def start_task(slot):
    dJobs = client(CreateData('/webimporter/v1/queue/task/get_all',0))

    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            if slot < len(aJobs):
                response = client(CreateData('/webimporter/v1/queue/task/start',aJobs[slot]))
                print(response)
        else:
            print("no jobs on server")

def CheckStatus():
    jobs_lookup = {}
    dJobs = client(CreateData('/webimporter/v1/queue/task/get_all',0))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        print("Number of active jobs:" + str(len(aJobs)-1))
        for job in aJobs:
            if job != "":
                jobs_lookup[job] = True

        while len(jobs_lookup)>0:
            for job in aJobs:
                if job in jobs_lookup:
                    response = client(CreateData('/webimporter/v1/queue/status',job))
                    #print(response)
                    if response != None:
                        response = json.loads(response)

                        if response["status"] == "Job Complete":
                             del jobs_lookup[job]
                             print("Job Complete")
                        else:
                            print("Task:" + str(response["status"]))
                            if "worker" in response:
                                if len(response["worker"]) > 0:

                                    for worker,progress in response["worker"].items():
                                        if len(progress) > 0:
                                            print("\t\t" + worker)
                                            for p in progress:
                                                for file,progress in p.items():
                                                    print("\t\t\t\t" + worker + " : " + file + " : " + str(progress))
            time.sleep(0.5)

        print("ended")
    else:
        print("No active jobs on server")
def pause(slot):
    dJobs = client(CreateData('/webimporter/v1/queue/task/get_all',0))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if slot < len(aJobs):
            client(CreateData('/webimporter/v1/queue/task/pause',aJobs[slot]))
def resume(slot):
    dJobs = client(CreateData('/webimporter/v1/queue/task/get_all',0))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            if slot < len(aJobs):
                response = client(CreateData('/webimporter/v1/queue/task/resume', aJobs[slot]))
        else:
            print("no jobs on server")

def pausequeue():
    dJobs = client(CreateData('/webimporter/v1/queue/task/get_all'))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        for j in aJobs:
            if j != "":
                client(CreateData('/webimporter/v1/queue/task/pause',j))
def resumequeue():
    dJobs = client(CreateData('/webimporter/v1/queue/task/get_all'))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            for j in aJobs:
                if j != "":
                    client(CreateData('/webimporter/v1/queue/task/resume', j))
def startqueue():
    dJobs = client(CreateData('/webimporter/v1/queue/task/get_all'))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            for j in aJobs:
                if j != "":
                    client(CreateData('/webimporter/v1/queue/task/start', j))
                    #time.sleep(1)
def removecompleted():
    aJobs = client(CreateData('/webimporter/v1/queue/task/get_all'))
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            client(CreateData('remove_completed_tasks'))
    else:
        print("No tasks on server")
def removeincompletetasks():
    dJobs = client(CreateData('/webimporter/v1/queue/task/get_all'))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        print(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            client(CreateData('/webimporter/v1/queue/task/remove_incomplete'))
    else:
        print("No tasks on server")
def restart_tasks():
    dJobs = client(CreateData('/webimporter/v1/queue/task/get_all'))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            for j in aJobs:
                if j != "":
                    client(CreateData('/webimporter/v1/queue/task/restart', j))
                    #time.sleep(1)
def modify(slot):
    dJobs = client(CreateData('/webimporter/v1/queue/task/get_all'))
    if dJobs != None:
        aJobs = json.loads(dJobs)
        aJobs = aJobs["job"]
        if len(aJobs)>0:
            if slot < len(aJobs):
                aPayload = []
                JobList = ['c:/Data1']
                for pl in JobList:
                    payload = {}
                    head, tail = os.path.split(pl)
                    if len(tail.split(".")) > 1:
                        payload["type"] = "file"
                    else:
                        payload["type"] = "folder"

                    payload["data"] = pl
                    aPayload.append(payload)


                data = {}
                data["ID"] = aJobs[slot]
                data["Payload"] = aPayload
                response = client(CreateData('/webimporter/v1/queue/task/modify', data))
        else:
            print("no jobs on server")
def setpriority(prioritylist):
    dJobs = client(CreateData('/webimporter/v1/queue/task/get_all'))
    if dJobs != None:
        aJobs = json.loads(dJobs)
        aJobs = aJobs["job"]
        if len(aJobs)>0:
            print("Current Jobs:")
            counter = 0
            for job in aJobs:
                print(str(counter) + ":" + job)
                counter += 1
            r = random.random()
            random.shuffle(aJobs, lambda: r)
            print("Sending jobs:")
            counter = 0
            for job in aJobs:
                print(str(counter) + ":" + job)
                counter += 1

    client(CreateData('/webimporter/v1/global/queue/set_priority', aJobs))
def shutdown():
    client(CreateData('/webimporter/v1/server/shutdown'))

def activate_queue():
    client(CreateData('/webimporter/v1/queue/activate'))
def deactivate_queue():
    client(CreateData('/webimporter/v1/queue/deactivate'))
def put_tasks_on_queue():
    client(CreateData('/webimporter/v1/queue/put_tasks'))
if __name__ == "__main__":
    #create_copytask()
    #create_copytask()
    create_copytask()
    startqueue()
    put_tasks_on_queue()
    activate_queue()
    #deactivate_queue()
    # time.sleep(5)
    #pausequeue()
    #
    #resumequeue()
    #
    # setpriority([])
    # time.sleep(2)
    #CheckStatus()

    #setpriority([])
    #deactivate_queue()
    #removeincompletetasks()
    #removecompleted()
#     time.sleep(0.1)
#
# #    removeincompletetasks()
#     create_copytask()


    #create_copytask()
    #create_copytask()
    #create_copytask()
    #time.sleep(2)
    #modify(0)
    #restart_tasks()

    #time.sleep(5)
    # # # # start_task(0)
    # # #start_task(0)
    # # #
    #pausequeue()
    # pause(0)
    # pause(1)
    # pause(2)
    # pause(3)
    # pause(4)
    # pause(5)
    #
    # #
    # time.sleep(3)
    # # # #resume(0)
    # # #resume(1)
    # #pausequeue()
    #

    # time.sleep(3)

    # #resume(1)
    # #aJobs = client(CreateData('get_tasks',0))
    # #print(aJobs)
    # #removeincompletetasks()
    #modify(3)




    #shutdown()
