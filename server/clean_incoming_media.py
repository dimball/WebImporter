__author__ = 'hng'
from vizone.client import init
from vizone.resource.incoming import get_incomings
from vizone.payload.media.incomingcollection import IncomingCollection
client = init('192.168.110.144', 'admin', 'admin')
Collection = IncomingCollection(get_incomings())
print(len(Collection.entries))
for incoming in Collection.entries:
    client.DELETE(incoming.edit_link)
print(len(Collection.entries))