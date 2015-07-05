#!/usr/bin/python

import socket
import random
import time
import select
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
            if (reply.decode('utf8')) != "No Data":
                print("reply => " + (reply.decode('utf8')))
            else:
                print("reply => Job not started")
            return reply.decode('utf8')
    else:
        print("request timed out")

    if sock != None:
        sock.close()
    #return reply

def main(method):
    jobs = []


    if method == "copy":
        JobList = ['copy|c:/Data1','copy|c:/Data1','copy|c:/Data2', 'copy|c:/Data1','copy|c:/Data3']
        #,'copy|c:/Data1','copy|c:/Data2', 'copy|c:/Data1','copy|c:/Data3'
        for j in JobList:
            id = client(j)
            #time.sleep(1)



def CheckStatus(method):
    jobs_lookup = {}
    aJobs = client('getjobs|0')
    if aJobs != None:
        jobs = aJobs.split("|")
        print("Number of active jobs:" + str(len(jobs)))
        for job in jobs:
            if job != "":
                jobs_lookup[job] = True

        while len(jobs_lookup)>0:
            for job in jobs:
                if job in jobs_lookup:
                    if method == "abort":
                        response = client("abort_job|" + job)
                        del jobs_lookup[job]

                    elif method == "copy":
                        response = client("status|" + job)
                        if response == "100.0":
                            del jobs_lookup[job]
                        time.sleep(0.5)

        print("ended")
    else:
        print("No active jobs on server")
def pause(slot):
    aJobs = client('getjobs|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if slot < len(aJobs)-1:
           client("pause_job|" + aJobs[slot])
def resume(slot):
    aJobs = client('getjobs|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            if slot < len(aJobs)-1:
                response = client("resume_job|" + aJobs[slot])
                print(response)
        else:
            print("no jobs on server")

def abortjob(slot):
    aJobs = client('getjobs|0')

    if aJobs != None:
        aJobs = aJobs.split("|")
        if slot < len(aJobs)-1:
           client("abort_job|" + aJobs[slot])

def abortqueue():
    aJobs = client('getjobs|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            client("abort_queue|0")

def pausequeue():
    aJobs = client('getjobs|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            for j in aJobs:
                if j != "":
                    client("pause_job|" + j)
def resumequeue():
    aJobs = client('getjobs|0')
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            for j in aJobs:
                if j != "":
                    client("resume_job|" + j)
if __name__ == "__main__":
    method = "copy"
    main(method)

    #pause(2)
    #resume(2)
    #abortjob(0)
    #abortqueue()
    #pausequeue()
    #resumequeue()

    CheckStatus(method)