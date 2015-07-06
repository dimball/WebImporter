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

def create_copytask():

    data = {}
    JobList = ['c:/Data2','c:/Data2','c:/Data3','temp.dat','somefile.exe','file.bat']
    data["command"] = 'create_copytask'
    data["payload"] = []


    for pl in JobList:
        payload = {}
        head, tail = os.path.split(pl)
        if len(tail.split(".")) > 1:
            payload["type"] = "file"
        else:
            payload["type"] = "folder"

        payload["data"] = pl
        data["payload"].append(payload)

    json_data = json.dumps(data)
    client(json_data)

def start_task(slot):
    aJobs = client('get_tasks|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            if slot < len(aJobs)-1:
                response = client("start_task|" + aJobs[slot])
                print(response)
        else:
            print("no jobs on server")

def CheckStatus():
    jobs_lookup = {}
    aJobs = client('get_tasks|0')
    if aJobs != None:
        jobs = aJobs.split("|")
        print("Number of active jobs:" + str(len(jobs)-1))
        for job in jobs:
            if job != "":
                jobs_lookup[job] = True

        while len(jobs_lookup)>0:
            for job in jobs:
                if job in jobs_lookup:
                    response = client("status|" + job)
                    if response == "Job Complete":
                        del jobs_lookup[job]
                    time.sleep(0.5)

        print("ended")
    else:
        print("No active jobs on server")
def pause(slot):
    aJobs = client('get_tasks|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if slot < len(aJobs)-1:
           client("pause_job|" + aJobs[slot])
def resume(slot):
    aJobs = client('get_tasks|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            if slot < len(aJobs)-1:
                response = client("resume_job|" + aJobs[slot])
                print(response)
        else:
            print("no jobs on server")

def pausequeue():
    aJobs = client('get_tasks|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            for j in aJobs:
                if j != "":
                    client("pause_job|" + j)
def resumequeue():
    aJobs = client('get_tasks|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            for j in aJobs:
                if j != "":
                    client("resume_job|" + j)

def removecompleted():
    aJobs = client('get_tasks|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            client("remove_completed_tasks|0")
    else:
        print("No tasks on server")
def removeincompletetasks():
    aJobs = client('get_tasks|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            client("remove_incomplete_tasks|0")
    else:
        print("No tasks on server")
def modify(slot,Payload):
    aJobs = client('get_tasks|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            if slot < len(aJobs)-1:
                response = client("modify_task|" + aJobs[slot] + "@" + Payload)
        else:
            print("no jobs on server")
if __name__ == "__main__":
    #®removecompleted()
    create_copytask()
    #start_task(0)
    #start_task(1)
    #start_task(2)

    #pause(0)
    #pause(1)git
    #resume(0)
    #resume(1)
    #pausequeue()
    #resumequeue()


    #removeincompletetasks()
    #CheckStatus()