from vizone.client import init
from vizone.payload.asset import Item
from vizone.payload.series import Series
from vizone.resource.series import create_series
from vizone.payload.program import Program
from vizone.resource.program import create_program
from vizone.payload.collection.members import Members
from vizone.urilist import UriList
from vizone.resource.series import search_seriess
from vizone.payload.series import SeriesFeed
from vizone.payload.folder import Folder
from vizone.resource.folder import create_folder
from vizone.payload.media import Incoming
from vizone.payload.transfer import TransferRequest
import common as hfn
import os
import time
from vizone.resource.incoming import get_incomings
from vizone.payload.media.incomingcollection import IncomingCollection
from vizone.net.message_queue import handle
from vizone.client import HTTPClientError

##needs to override the incoming class as it is missing the atomid. Will be fixed in the next version of python One
from vizone.payload.media.incoming import Incoming as _Incoming
from vizone.descriptors import Value
class Incoming(_Incoming):
    atomid = Value('atom:id', str)

class viz_one_test(hfn.c_HelperFunctions):
    def __init__(self):
        self.ExitStomp = False
        self.client = init('192.168.110.144', 'admin', 'admin')
        self.TransferMonitors = []
        self.osd = search_seriess()
        self.searchUrl = self.osd.make_url({
            'searchTerms': 'Mr Robot',
            'count': 1,
            'vizsort:sort': '-search.creationDate',
        })

        self.result = SeriesFeed(self.client.GET(self.searchUrl))
        self.series = None
        if (len(self.result.entries) == 0):
            ##if series does not exist then create one
            self.series = Series(title="Mr Robot")
            self.series = create_series(self.series)
        else:
            ##else just use the first one you find that matches it. Should not be duplicate series with the same name
            self.series = self.result.entries[0]


        #figure out what programs is in the series. Means that we need the series first
        self.SeriesPrograms = (Members(self.client.GET(self.series.down_link)))
        self.program = None
        for entry in self.SeriesPrograms.entries:
            if entry.title == "Episode 1":
                self.program = entry.program
                ##if it is found, then it should already be under the right series
                break

        if self.program == None:
            self.program = Program(title="Episode 1")
            self.program = create_program(self.program)
            ##put the program in the series
            self.Program_urilist = UriList([self.program.atomid])
            Members(self.client.POST(self.series.addmembers.add_last_link, self.Program_urilist))

        ##add the asset item into the folder

        self.FoldersInPrograms = (Members(self.client.GET(self.program.down_link)))
        self.folder = None

        for entry in self.FoldersInPrograms.entries:
            if entry.title == "Card1":
                self.folder = entry.folder
                ##if it is found, then it should already be under the right series
                break

        if self.folder == None:
            self.folder = Folder(title="Card1")
            self.folder = create_folder(self.folder)
            ##add the folder item into the program
            self.Folder_urilist = UriList([self.folder.atomid])
            Members(self.client.POST(self.program.addmembers.add_last_link, self.Folder_urilist))

        ## create an asset
        self.asset_entry_collection = self.client.servicedoc.get_collection_by_keyword('asset')
        self.asset_entry_endpoint = self.asset_entry_collection.endpoint


        self.aFiles = self.get_filepaths("d:/ftp/","*.mxf")
        self.aItemAsset = []
        self.aUploadList = []
        self.RequestLinks = []
        self.RequestList = {}

        for filename in self.aFiles:
            self.NewFile = os.path.normpath(filename)
            self.PathHead, self.FileTail = os.path.split(self.NewFile)
            self.FileHead, self.Extension = (os.path.splitext(self.FileTail))
            #
            self.placeholder = Item(title=self.FileHead)
            ##get the placeholder asset entry
            self.placeholder.parse(self.client.POST(self.asset_entry_endpoint, self.placeholder))
            print("Placeholder atomid:", self.placeholder.atomid)
            self.aItemAsset.append(self.placeholder.atomid)
            #
            self.drive, self.Path = os.path.splitdrive(self.PathHead)
            #
            self.aPathTokens = self.Path.split("\\")
            self.NewPath = ""
            for i in range(2, len(self.aPathTokens)):
                self.NewPath += self.aPathTokens[i] + "/"

            #wait for the file to be imported before it start on the next file. This is ok for a single machine, but for
            #multiple then this will not work. It will need some sort of id mechanism as it checks all incoming media
            #tasks from the server and does no filtering.
            self.TransferRequest = None
            with open(self.NewFile, 'rb') as f:
                self.data = {}
                self.data['Content-Type'] = 'application/octet-stream'
                self.data['Expect'] = ''

                #uploads the file
                self.r = self.client.PUT(self.placeholder.edit_media_link, f, headers=self.data)

                #have to wait a little bit for the file to be finalised on the server
                time.sleep(1)

                #waits for the placeholder to be ready with the transfer request (transcoding request)
                while True:
                    self.placeholder = Item(self.client.GET(self.placeholder.self_link))
                    try:
                        self.r = self.client.GET(self.placeholder.upload_task_link)
                        self.Incoming = Incoming(self.r)
                        if self.Incoming.transfer_link != None:
                            self.TransferRequest = TransferRequest(self.client.GET(self.Incoming.transfer_link))
                            break
                    except:
                        time.sleep(1)
            #at this point the transfer request link is available. It should be possible to set the priority here now

            self.TransferRequest.priority = "low" ## can be "low", "medium", "high"
            self.client.PUT(self.TransferRequest.self_link, self.TransferRequest, check_status=False)

            self.RequestLinks.append(self.TransferRequest)
            self.RequestList[self.TransferRequest.atomid] = {}
            self.RequestList[self.TransferRequest.atomid]["path"] = self.NewPath + self.FileHead + self.Extension
            self.RequestList[self.TransferRequest.atomid]["asset"] = self.placeholder.atomid



        self.TM = handle(self.RequestLinks[0].monitor_link.href, self.handler, 'admin', 'admin')


        ## Add the files to the folder
        self.ItemAsset_urilist = UriList(self.aItemAsset)
        Members(self.client.POST(self.folder.addmembers.add_last_link, self.ItemAsset_urilist))

        print("Blocking until complete")
        while len(self.RequestLinks)>0:
            time.sleep(1)
            continue

        self.TM.close()
        #self.RequestLinks[0].monitor_link.href


    def handler(self, response):
        identity = response.headers.get('identity')


        self.bContinue = False
        self.CurrentID = None
        for request in self.RequestLinks:
            if identity == request.atomid:
                self.CurrentID = request
                self.bContinue = True
                break

        if self.bContinue == True:
            self.r = TransferRequest(response)
            #this status is for transcoding.
            if self.r.progress != None:
                print("[" + self.RequestList[self.CurrentID.atomid]["path"] + "]Status = ", self.r.progress.done, self.r.progress.total)
                if self.r.progress.done == self.r.progress.total:
                    print("Complete")
                    self.RequestLinks.remove(self.CurrentID)
            else:
                self.RequestLinks.remove(self.CurrentID)




test = viz_one_test()