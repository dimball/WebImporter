#!/usr/bin/python

import socket
import random
import time
import select
import json
import os
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


    sock.setblocking(0)
    ready = select.select([sock],[],[],2)
    if ready[0]:
        reply = sock.recv(16348)  # limit reply to 16K
        if len(reply)>0:
            if (reply.decode('utf8')) != "Not started":
                print("reply => " + (reply.decode('utf8')))
            else:
                print("reply => Job not started")
            return reply.decode('utf8')
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
    JobList = ['c:/Data1']
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

def start_task(slot):
    dJobs = client(CreateData('get_tasks',0))

    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        print(aJobs)
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
                    if response == "Job Complete":
                        del jobs_lookup[job]
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
                print(response)
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
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            client(CreateData('remove_incomplete_tasks',0))
    else:
        print("No tasks on server")
def modify(slot):
    aJobs = client(CreateData('get_tasks',0))
    if aJobs != None:
        aJobs = json.loads(aJobs)
        if len(aJobs)>0:
            if slot < len(aJobs)-1:
                aPayload = []
                JobList = ['c:/Data1','c:/Data3']
                for pl in JobList:
                    payload = {}
                    head, tail = os.path.split(pl)
                    if len(tail.split(".")) > 1:
                        payload["type"] = "file"
                    else:
                        payload["type"] = "folder"

                    payload["data"] = pl
                    aPayload.append(payload)

                response = client(CreateData('modify_task',aPayload))
        else:
            print("no jobs on server")
if __name__ == "__main__":
    removecompleted()
    create_copytask()
    create_copytask()
    create_copytask()
    startqueue()
    time.sleep(2)
    # # start_task(0)
    # start_task(1)
    # start_task(2)
    pausequeue()
    # pause(0)
    # pause(1)
    # pause(2)

    time.sleep(2)
    #resume(0)
    #resume(1)
    #pausequeue()
    resumequeue()
    #aJobs = client(CreateData('get_tasks',0))
    #print(aJobs)
    #removeincompletetasks()
    CheckStatus()