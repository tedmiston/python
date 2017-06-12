
from twisted.internet.task import LoopingCall

import pubnub
import sys
import logging
import os
import gc
# gc.set_debug(gc.DEBUG_LEAK|gc.DEBUG_STATS)


import objgraph
import time

from pympler import summary, muppy, tracker, classtracker

from pubnub.endpoints.pubsub.publish import Publish
from pubnub.enums import PNStatusCategory
from pubnub.pubnub_twisted import PubNubTwisted as PubNub
from pubnub.pnconfiguration import PNConfiguration
from pubnub.callbacks import SubscribeCallback
from pympler.web import start_profiler, start_in_background

print("PID:" + str(os.getpid()))

pubnub.set_stream_logger('pubnub', logging.INFO, None, sys.stdout)

pnconf = PNConfiguration()
pnconf.subscribe_key = 'sub-c-45499300-2417-11e7-bb8a-0619f8945a4f'
pnconf.publish_key = 'pub-c-04e11ea3-cd21-4662-ad94-293fe28fa843'
pnconf.enable_subscribe = False

pubnub = PubNub(pnconf)


# def track(tr):
#     gc.collect()
#     tr.print_diff()
#
# tr = tracker.SummaryTracker()
# tracker = LoopingCall(track, tr)
# tracker.start(10)
# time.sleep(5)

# ct = classtracker.ClassTracker()
# ct.track_class(Publish)
# ct.track_object(pubnub)
# ct.start_periodic_snapshots(10)

# # def my_publish_callback(result, status):
# def my_publish_callback(result):
#     # Check whether request successfully completed or not
#     print(result)

def stop_pubnub(_):
    # pass
    # gc.collect()
    print('Published!')
    # time.sleep(1)
    # print('Stopping')
    # pubnub.stop()
#
# pubnub.reactor.callLater(3, stop_pubnub)

# all_objects = muppy.get_objects()


def keep_publishing():
    pubnub.publish().channel("bot").message("Hello World!").deferred().addCallback(stop_pubnub)
#
LoopingCall(keep_publishing).start(0.2)

# start_in_background(tracker=ct)

pubnub.start()
#
# gc.collect()
#
# current_objects = muppy.get_objects()
#
# interesting_ones = muppy.filter(current_objects, Type=dict)
#
# objgraph.show_refs(interesting_ones, filename='dict-graph.jpg')
