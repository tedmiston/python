import twisted

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial import unittest
from twisted.web.client import HTTPConnectionPool

from pubnub.models.consumer.presence import PNHereNowResult

from pubnub.pubnub_twisted import PubNubTwisted, TwistedEnvelope

from tests.helper import pnconf
from tests.integrational.vcr_helper import pn_vcr

twisted.internet.base.DelayedCall.debug = True

channel = 'twisted-test'
channels = 'twisted-test-1', 'twisted-test-1'


class HereNowTest(unittest.TestCase):
    def setUp(self):
        self.pool = HTTPConnectionPool(reactor, persistent=False)
        self.pubnub = PubNubTwisted(pnconf, reactor=reactor, pool=self.pool)

    def tearDown(self):
        return self.pool.closeCachedConnections()

    def assert_valid_here_now_envelope(self, envelope, expected_result):
        self.assertIsInstance(envelope, TwistedEnvelope)
        self.assertIsInstance(envelope.result, PNHereNowResult)
        self.assertEqual(envelope.raw_result, expected_result)

    @inlineCallbacks
    @pn_vcr.use_cassette(
        'tests/integrational/fixtures/twisted/here_now/global.yaml',
        filter_query_parameters=['uuid', 'pnsdk'])
    def test_global_here_now(self):
        envelope = yield self.pubnub.here_now() \
            .include_uuids(True) \
            .deferred()

        self.assert_valid_here_now_envelope(
            envelope,
            {u'status': 200, u'message': u'OK', u'payload': {
                u'channels': {u'twisted-test-1': {u'uuids': [u'00de2586-7ad8-4955-b5f6-87cae3215d02'], u'occupancy': 1},
                              u'twisted-test': {u'uuids': [u'00de2586-7ad8-4955-b5f6-87cae3215d02'], u'occupancy': 1}},
                u'total_channels': 2, u'total_occupancy': 2}, u'service': u'Presence'})
        returnValue(envelope)

    @inlineCallbacks
    @pn_vcr.use_cassette(
        'tests/integrational/fixtures/twisted/here_now/single.yaml',
        filter_query_parameters=['uuid', 'pnsdk'])
    def test_here_now_single_channel(self):
        envelope = yield self.pubnub.here_now() \
            .channels(channel) \
            .include_uuids(True) \
            .deferred()

        self.assert_valid_here_now_envelope(
            envelope,
            {u'status': 200, u'message': u'OK', u'occupancy': 1, u'uuids': [u'00de2586-7ad8-4955-b5f6-87cae3215d02'],
             u'service': u'Presence'}
        )
        returnValue(envelope)

    @inlineCallbacks
    @pn_vcr.use_cassette(
        'tests/integrational/fixtures/twisted/here_now/multiple.yaml',
        filter_query_parameters=['uuid', 'pnsdk'])
    def test_here_now_multiple_channels(self):
        envelope = yield self.pubnub.here_now() \
            .channels(channels) \
            .include_uuids(True) \
            .deferred()

        self.assert_valid_here_now_envelope(
            envelope,
            {u'status': 200, u'message': u'OK', u'payload': {u'channels': {
                u'twisted-test-1': {u'uuids': [u'00de2586-7ad8-4955-b5f6-87cae3215d02'], u'occupancy': 1}},
                                                             u'total_channels': 1, u'total_occupancy': 1},
             u'service': u'Presence'}
        )
        returnValue(envelope)
