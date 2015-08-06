#!/usr/bin/python

import socket
import time
import select
import json
import os
import random
import websocket

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

import threading

# def recvall(sock):
#     data = ""
#     part = None
#     while part != "":
#         part = sock.recv(1024).decode('utf8')
#         data += part
#         if part == "":
#             break
#     return data

import common as hfn
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )

class client_progress_handler():
    def on_message(self, ws, data):
        self.Data = json.loads(data)
        self.Command = self.Data["command"]
        self.Payload = self.Data["payload"]

        if self.Command == "/client/v1/local/queue/task/set_progress":
            logging.debug("Received TASK progress:%s:%s", self.Payload["ID"], self.Payload["progress"])
        elif self.Command == "/client/v1/local/queue/task/file/set_progress":
            logging.debug("Received FILE progress:%s:%s", self.Payload["file"], self.Payload["progress"])

class client_command_handler():
    ###this recieves messages from the web importer server.
    def on_message(self, ws, data):
        self.Data = json.loads(data)
        self.Command = self.Data["command"]
        self.Payload = self.Data["payload"]
        if self.Command == "/client/v1/local/queue/task/put":
            logging.debug("Received new TASK from webimporter:%s", self.Payload["TaskList"][0]["ID"])
        elif self.Command == "/client/v1/local/queue/task/get":
            logging.debug("Received Jobs from webimporter:%s", self.Payload["job"])



class threaded_Websocket_Client(threading.Thread, hfn.c_HelperFunctions):
    def __init__(self,ip, port, sType, handler):
        threading.Thread.__init__(self)
        self.connected = False
        self.type = sType
        self.ip = ip
        self.port = port
        self.connection = None
        self.name = sType
        self.on_message = handler.on_message

        # try:
        logging.debug("[" + self.type + "] Attempting to register on webimporter at %s:%s", self.ip, self.port)
        self.connection = websocket.create_connection("ws://" + self.ip + ":" + str(self.port) + "/" + self.type)
        self.connected = True
        # except:
        #     logging.debug("Web importer at %s:%s is not reachable. Disabling webimporter for this session",self.ip, self.port)
        #     self.connected = False

        if self.connected:
            logging.debug("Connected to sync server at: %s:%s",self.ip, self.port)

        self.start()
    def run(self):
        while True:
            if self.connection != None:
                self.on_message(self.connection, self.connection.recv())
                #logging.debug("received data from [%s} %s", self.name, self.connection.recv())

    def m_send(self, payload,bDebug=True):
        self.connection.send(payload)

def Response_Client(string):
    connection = websocket.create_connection("ws://localhost:9090/command")
    connection.send(string)
    try:
        result = connection.recv()
        if result != "None":
            return result
    except:
        return "None"

def CreateData(Command,Payload=0):
    data = {}
    data["command"] = Command
    data["payload"] = Payload
    return json.dumps(data)
def create_copytask(Client):
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

    Client.m_send(CreateData('/webimporter/v1/queue/task/create', aPayload))

def modify_task(slot):
    dJobs = Response_Client(CreateData('/webimporter/v1/queue/task/get_all'))
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
                response = Client.m_send(CreateData('/webimporter/v1/queue/task/modify',data))
                print(response)
        else:
            print("no jobs on server")
def start_task(Client,slot):
    dJobs = Response_Client(CreateData('/webimporter/v1/queue/task/get_all',0))

    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            if slot < len(aJobs):
                response = Client.m_send(CreateData('/webimporter/v1/queue/task/start', aJobs[slot]))
                print(response)
        else:
            print("no jobs on server")


def pause(Client,slot):
    dJobs = Response_Client(CreateData('/webimporter/v1/queue/task/get_all',0))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if slot < len(aJobs):
            Client.m_send(CreateData('/webimporter/v1/queue/task/pause',aJobs[slot]))
def resume(Client,slot):
    dJobs = Response_Client(CreateData('/webimporter/v1/queue/task/get_all',0))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            if slot < len(aJobs):
                response = Client.m_send(CreateData('/webimporter/v1/queue/task/resume', aJobs[slot]))
        else:
            print("no jobs on server")

def pausequeue(Client):
    dJobs = Response_Client(CreateData('/webimporter/v1/queue/task/get_all'))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        for j in aJobs:
            if j != "":
                Client.m_send(CreateData('/webimporter/v1/queue/task/pause',j))
def resumequeue(Client):
    dJobs = Response_Client(CreateData('/webimporter/v1/queue/task/get_all'))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            for j in aJobs:
                if j != "":
                    Client.m_send(CreateData('/webimporter/v1/queue/task/resume', j))
def startqueue(Client):
    dJobs = Response_Client(CreateData('/webimporter/v1/queue/task/get_all'))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            for j in aJobs:
                if j != "":
                    Client.m_send(CreateData('/webimporter/v1/queue/task/start', j))
                    #time.sleep(1)
def removecompleted(Client):
    aJobs = Response_Client(CreateData('/webimporter/v1/queue/task/get_all'))
    if aJobs != None:
        aJobs = aJobs.split("|")
        if len(aJobs)>0:
            Client.m_send(CreateData('remove_completed_tasks'))
    else:
        print("No tasks on server")
def removeincompletetasks(Client):
    dJobs = Response_Client(CreateData('/webimporter/v1/queue/task/get_all'))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        print(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            Client.m_send(CreateData('/webimporter/v1/queue/task/remove_incomplete'))
    else:
        print("No tasks on server")
def restart_tasks(Client):
    dJobs = Response_Client(CreateData('/webimporter/v1/queue/task/get_all'))
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
        if len(aJobs)>0:
            for j in aJobs:
                if j != "":
                    Client.m_send(CreateData('/webimporter/v1/queue/task/restart', j))
                    #time.sleep(1)
def modify(Client, slot):
    dJobs = Response_Client(CreateData('/webimporter/v1/queue/task/get_all'))
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
                response = Client.m_send(CreateData('/webimporter/v1/queue/task/modify', data))
        else:
            print("no jobs on server")


def setpriority(Client, prioritylist):

    dJobs = Response_Client(CreateData('/webimporter/v1/queue/task/get_all'))
    logging.debug("recieving:%s",dJobs)
    if dJobs != None:
        dJobs = json.loads(dJobs)
        aJobs = dJobs["job"]
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

            Client.m_send(CreateData('/webimporter/v1/global/queue/set_priority', aJobs))
def shutdown(Client):
    Client.m_send(CreateData('/webimporter/v1/server/shutdown'))

def activate_queue(Client):
    Client.m_send(CreateData('/webimporter/v1/queue/activate'))
def deactivate_queue(Client):
    Client.m_send(CreateData('/webimporter/v1/queue/deactivate'))
def put_tasks_on_queue(Client):
    Client.m_send(CreateData('/webimporter/v1/queue/put_tasks'))
if __name__ == "__main__":
    threaded_Websocket_Client("localhost", 9090, "progress", client_progress_handler())
    CommandHandler = threaded_Websocket_Client("localhost", 9090, "command", client_command_handler())
    #setpriority([])
    # create_copytask()
    # #create_copytask()
    create_copytask(CommandHandler)
    # time.sleep(4)
    # startqueue(CommandHandler)
    # put_tasks_on_queue(CommandHandler)
    # activate_queue(CommandHandler)
    #deactivate_queue()
    # time.sleep(5)
    #pausequeue()
    #
    #resumequeue()
    #
    #
    # time.sleep(2)
   #CheckStatus()

    #setpriority(CommandHandler, [])
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

