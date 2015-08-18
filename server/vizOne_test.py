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

client = init('192.168.110.144', 'admin', 'admin')

osd = search_seriess()
searchUrl = osd.make_url({
    'searchTerms': 'Mr Robot',
    'count': 1,
    'vizsort:sort': '-search.creationDate',
})

result = SeriesFeed(client.GET(searchUrl))
series = None
if (len(result.entries) == 0):
    ##if series does not exist then create one
    series = Series(title="Mr Robot")
    series = create_series(series)
else:
    ##else just use the first one you find that matches it. Should not be duplicate series with the same name
    series = result.entries[0]


#figure out what programs is in the series. Means that we need the series first
SeriesPrograms = (Members(client.GET(series.down_link)))
program = None
for entry in SeriesPrograms.entries:
    if entry.title == "Episode 1":
        program = entry.program
        ##if it is found, then it should already be under the right series
        break

if program == None:
    program = Program(title="Episode 1")
    program = create_program(program)
    ##put the program in the series
    Program_urilist = UriList([program.atomid])
    Members(client.POST(series.addmembers.add_last_link, Program_urilist))

##add the asset item into the folder

FoldersInPrograms = (Members(client.GET(program.down_link)))
folder = None

for entry in FoldersInPrograms.entries:
    if entry.title == "Card1":
        folder = entry.folder
        ##if it is found, then it should already be under the right series
        break

if folder == None:
    folder = Folder(title="Card1")
    folder = create_folder(folder)
    ##add the folder item into the program
    Folder_urilist = UriList([folder.atomid])
    Members(client.POST(program.addmembers.add_last_link, Folder_urilist))

## create an asset
asset_entry_collection = client.servicedoc.get_collection_by_keyword('asset')
asset_entry_endpoint = asset_entry_collection.endpoint
placeholder = Item(title="whiterose")

##get the placeholder asset entry
placeholder.parse(client.POST(asset_entry_endpoint, placeholder))

ItemAsset_urilist = UriList([placeholder.atomid])
Members(client.POST(folder.addmembers.add_last_link, ItemAsset_urilist))

## create a transfer request for media on the placeholder

UploadList = UriList(['ftp://ardome:aidem630@10.211.110.145/upload/id1234/testUpload.mxf', ])


#r = client.POST(placeholder.import_unmanaged_link, UploadList, check_status=False)
incoming = Incoming(client.POST(placeholder.import_unmanaged_link, UploadList, check_status=False))
print(incoming.state)
