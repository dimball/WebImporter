__author__ = 'hng'
from vizone.client import init
from vizone.payload.asset import Item
from vizone.payload.aggregate_asset.itemsetcollection import ItemSetCollection
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
import socket

from vizone.resource.asset import get_assets
from vizone.payload.asset.itemcollection import ItemCollection
class c_DeleteFiles():
    def __init__(self):
        self.client = init('192.168.110.144', 'admin', 'admin')
        self.Assets = ItemCollection(get_assets())

        for a in self.Assets.entries:
            self.client.DELETE(a.edit_link, check_status=False)

c_DeleteFiles()



