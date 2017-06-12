from twisted.internet import threads

import pubnub
import logging
import sys
import time

from pubnub.enums import PNStatusCategory
from pubnub.pubnub_twisted import PubNubTwisted as PubNub
from pubnub.pnconfiguration import PNConfiguration
from pubnub.callbacks import SubscribeCallback

pubnub.set_stream_logger('pubnub', logging.INFO, None, sys.stdout)

pnconf = PNConfiguration()
pnconf.subscribe_key = 'sub-c-45499300-2417-11e7-bb8a-0619f8945a4f'
pnconf.publish_key = 'pub-c-04e11ea3-cd21-4662-ad94-293fe28fa843'

pubnub = PubNub(pnconf)

class MySubscribeCallback(SubscribeCallback):
    def presence(self, pubnub, presence):
        print("Reveived presence event: " + str(presence.event) + " " + str(presence.uuid))

    def status(self, pubnub, status):
        if status.category == PNStatusCategory.PNConnectedCategory:
            print('Connected!')

    def message(self, pubnub, message):
        print("Received message: " + str(message.message))

        if message.message.has_key('subscribe'):
            pubnub.subscribe().channels(message.message['subscribe']).execute()

        if message.message.has_key('leave'):
            pubnub.unsubscribe().channels(message.message['leave']).execute()

        if message.message.has_key('sleep'):
            time.sleep(message.message['sleep'])

pubnub.add_listener(MySubscribeCallback())
pubnub.subscribe().channels(['start', 'start-pnpres']).execute()

pubnub.start()
