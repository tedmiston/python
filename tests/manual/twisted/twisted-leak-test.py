from urlparse import SplitResult

from twisted.internet.task import LoopingCall

import pubnub
import sys
import logging
import os
import gc
# gc.set_debug(gc.DEBUG_LEAK|gc.DEBUG_STATS)

from pympler import summary, muppy, tracker
from pubnub.enums import PNStatusCategory
from pubnub.pubnub_twisted import PubNubTwisted as PubNub
from pubnub.pnconfiguration import PNConfiguration
from pubnub.callbacks import SubscribeCallback

print("PID:" + str(os.getpid()))

pubnub.set_stream_logger('pubnub', logging.INFO, None, sys.stdout)

pnconf = PNConfiguration()
pnconf.subscribe_key = 'sub-c-45499300-2417-11e7-bb8a-0619f8945a4f'
pnconf.publish_key = 'pub-c-04e11ea3-cd21-4662-ad94-293fe28fa843'

pubnub = PubNub(pnconf)


def my_publish_callback(result, status):
    # Check whether request successfully completed or not
    if not status.is_error():
        pass  # Message successfully published to specified channel.


class MySubscribeCallback(SubscribeCallback):
    def presence(self, pubnub, presence):
        pass

    def status(self, pubnub, status):
        if status.category == PNStatusCategory.PNConnectedCategory:
            # pubnub.publish().channel("bot").message("Hello World!").async(my_publish_callback)
            # pubnub.stop(True)
            print('Connected!')

    def message(self, pubnub, message):
        print(message)

pubnub.add_listener(MySubscribeCallback())
pubnub.subscribe().channels(['bot']).execute()


# def track(tr):
#    gc.collect()
#    tr.print_diff()
#
# tr = tracker.SummaryTracker()
# tracker = LoopingCall(track, tr)
# tracker.start(10)

#
# def stop_pubnub():
#     pubnub.stop()
#
# pubnub.reactor.callLater(60, stop_pubnub)


pubnub.start()
