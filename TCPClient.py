#!/usr/bin/python

import socket
import random
import time
import select
import json
import os
def recvall(sock):
    data = ""
    part = None
    while part != "":
        part = sock.recv(4096).decode('utf8')
        print(part)
        data += part
    return data

def client(string):
    HOST, PORT = 'localhost', 9090
    # SOCK_STREAM == a TCP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #sock.setblocking(0)  # optional non-blocking
    sock.connect((HOST, PORT))

    print("sending data => " + (string))
    try:
        sock.send(bytes(string, 'utf8'))
    except:
        print("")


    #sock.setblocking(0)
    ready = select.select([sock],[],[],2)
    if ready[0]:

        reply = recvall(sock)  # limit reply to 2mb
        if len(reply)>0:
            return reply
    else:
        print("request timed out")

    if sock != None:
        sock.close()
    #return reply

def CreateData(Command,Payload):
    data = {}
    data["command"] = Command
    data["payload"] = Payload
    return json.dumps(data)
def create_copytask():
    data = {}
    JobList = ['c:/Data3']
    data["command"] = 'create_copytask'
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

    client(CreateData('create_copytask',aPayload))
def modify_task(slot):
    dJobs = client(CreateData('get_tasks',0))
    JobList = ['c:/Data1']
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
                response = client(CreateData('modify_task',data))
                print(response)
        else:
            print("no jobs on server")

def start_task(slot):
    dJobs = client(CreateData('get_tasks',0))

    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            if slot < len(aJobs):
                response = client(CreateData('start_task',aJobs[slot]))
                print(response)
        else:
            print("no jobs on server")

def CheckStatus():
    jobs_lookup = {}
    dJobs = client(CreateData('get_tasks',0))
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
                    response = client(CreateData('status',job))
                    print(response)
                    response = json.loads(response)

                    if response["status"] == "Job Complete":
                         del jobs_lookup[job]
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
    dJobs = client(CreateData('get_tasks',0))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if slot < len(aJobs):
            client(CreateData('pause_job',aJobs[slot]))
def resume(slot):
    dJobs = client(CreateData('get_tasks',0))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            if slot < len(aJobs):
                response = client(CreateData('resume_job',aJobs[slot]))
        else:
            print("no jobs on server")

def pausequeue():
    dJobs = client(CreateData('get_tasks',0))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        for j in aJobs:
            if j != "":
                client(CreateData('pause_job',j))
def resumequeue():
    dJobs = client(CreateData('get_tasks',0))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            for j in aJobs:
                if j != "":
                    client(CreateData('resume_job',j))
def startqueue():
    dJobs = client(CreateData('get_tasks',0))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            for j in aJobs:
                if j != "":
                    client(CreateData('start_task',j))
                    #time.sleep(1)
def removecompleted():
    aJobs = client(CreateData('get_tasks',0))
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            client(CreateData('remove_completed_tasks',0))
    else:
        print("No tasks on server")
def removeincompletetasks():
    dJobs = client(CreateData('get_tasks',0))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        print(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            client(CreateData('remove_incomplete_tasks',0))
    else:
        print("No tasks on server")
def modify(slot):
    dJobs = client(CreateData('get_tasks',0))
    if dJobs != None:
        aJobs = json.loads(dJobs)
        aJobs = aJobs["job"]
        if len(aJobs)>0:
            if slot < len(aJobs):
                aPayload = []
                JobList = ['c:/Data3']
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
                response = client(CreateData('modify_task',data))
        else:
            print("no jobs on server")
if __name__ == "__main__":

    #removeincompletetasks()
    #removecompleted()
    #time.sleep(2)

#    removeincompletetasks()
    #create_copytask()
    modify(0)
    #create_copytask()
    # create_copytask()
    startqueue()
    # time.sleep(2)
    # # # start_task(0)
    # #start_task(0)
    # #start_task(1)
    # pausequeue()
    # # pause(0)
    # # pause(1)
    # # pause(2)
    #
    # time.sleep(2)
    # # #resume(0)
    # #resume(1)
    # #pausequeue()
    # resumequeue()
    #aJobs = client(CreateData('get_tasks',0))
    #print(aJobs)
    #removeincompletetasks()
    CheckStatus()