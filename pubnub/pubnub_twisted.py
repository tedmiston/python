import logging

import re
import six
import treq
import sys
import time

# from StringIO import StringIO
from twisted.internet.defer import DeferredQueue, Deferred, CancelledError
from twisted.internet.error import ConnectingCancelledError
from twisted.internet.task import LoopingCall
from twisted.web._newclient import ResponseNeverReceived
from twisted.web.client import Agent, HTTPConnectionPool, FileBodyProducer

from .endpoints.presence.leave import Leave
from .workers import SubscribeMessageWorker

# if sys.version_info.major >= 3:
#     from urllib.parse import urlparse, parse_qs
# else:
#     from urlparse import urlparse, parse_qs

from .endpoints.pubsub.subscribe import Subscribe
from .endpoints.presence.heartbeat import Heartbeat
from .exceptions import PubNubException
from .managers import PublishSequenceManager, SubscriptionManager
from .structures import ResponseInfo
from .errors import PNERR_SERVER_ERROR, PNERR_CLIENT_ERROR
from . import utils
from .enums import PNStatusCategory, PNOperationType, PNHeartbeatNotificationOptions
from .pubnub_core import PubNubCore
from twisted.internet import reactor as _reactor, threads
from twisted.python import log

logger = logging.getLogger('pubnub')


class PubNubTwisted(PubNubCore):
    # TODO: custom connection pool

    def sdk_platform(self):
        return "-Twisted"

    def __init__(self, config, pool=None, reactor=None, clock=None):
        super(PubNubTwisted, self).__init__(config)

        self._publish_sequence_manager = PublishSequenceManager(PubNubCore.MAX_SEQUENCE)
        self._message_queue = DeferredQueue()

        self.clock = clock
        if self.config.enable_subscribe:
            self._subscription_manager = TwistedSubscriptionManager(self)

        if reactor is None:
            self.reactor = _reactor
        else:
            self.reactor = reactor

        non_sub_pool = HTTPConnectionPool(persistent=True, reactor=self.reactor)
        non_sub_pool.maxPersistentPerHost = 1
        non_sub_pool.cachedConnectionTimeout = 300
        self.non_sub_agent = Agent(
            reactor=self.reactor,
            connectTimeout=10,
            pool=non_sub_pool
        )

        if self.config.enable_subscribe:
            sub_pool = HTTPConnectionPool(persistent=True, reactor=self.reactor)
            sub_pool.maxPersistentPerHost = 1
            sub_pool.cachedConnectionTimeout = 300
            self.sub_agent = Agent(
                reactor=self.reactor,
                connectTimeout=300,
                pool=non_sub_pool
            )

        self.headers = {
            'User-Agent': [self.sdk_name]
        }

    def start(self, skip_reactor=False):
        logger.debug('Starting PubNubTwisted')
        if self.config.enable_subscribe:
            self._subscription_manager.start()
        if not skip_reactor:
            self.reactor.run()

    def stop(self, skip_reactor=False):
        logger.debug('Stopping PubNubTwisted')
        if self.config.enable_subscribe:
            self._subscription_manager.stop()
        if not skip_reactor:
            self.reactor.stop()

    def request_async(self, endpoint_name, endpoint_call_options, callback, cancellation_event):
        def async_request(endpoint_call_options, cancellation_event, callback):
            def manage_failures(failure):
                # Cancelled
                if failure.type == CancelledError:
                    return failure
                elif failure.type == PubNubTwistedException:
                    raise failure.value
                else:
                    raise failure

            def options_func():
                return endpoint_call_options

            request = self.request_deferred(options_func, cancellation_event)
            request.addCallbacks(callback, manage_failures)

        self.reactor.callLater(0, async_request, endpoint_call_options, cancellation_event, callback)

    def request_deferred(self, options_func, cancellation_event):
        def handle_response(response, url, options, request):
            logger.debug('Handling response')

            def status_category(status):
                if status == 200:
                    return PNStatusCategory.PNAcknowledgmentCategory
                elif status == 400:
                    return PNStatusCategory.PNBadRequestCategory
                elif status == 403:
                    return PNStatusCategory.PNAccessDeniedCategory
                elif status == 500:
                    return PNERR_SERVER_ERROR

            def error_value(status):
                if status == 200:
                    return None
                elif status >= 500:
                    return PNERR_SERVER_ERROR
                else:
                    return PNERR_CLIENT_ERROR

            def response_info(response, url, request):
                def uuid(query):
                    if 'uuid' in query and len(query['uuid']) > 0:
                        return query['uuid'][0]
                    else:
                        return None

                def auth_key(query):
                    if 'auth_key' in query and len(query['auth_key']) > 0:
                        return query['auth_key'][0]
                    else:
                        return None

                def parsed_url(url):
                    return six.moves.urllib.parse.urlparse(url)

                def query(parsed_url):
                    return six.moves.urllib.parse.parse_qs(parsed_url.query)

                return ResponseInfo(
                    status_code=response.code,
                    tls_enabled='https' == parsed_url(url).scheme,
                    origin=parsed_url(url).netloc,
                    uuid=uuid(query(parsed_url(url))),
                    auth_key=auth_key(query(parsed_url(url))),
                    client_request=request
                )

            def handle_error(error, status, response, url, request):
                if error.check(ResponseNeverReceived):
                    return

                logger.error(error)
                raise PubNubTwistedException(
                    result=error,
                    status=options.create_status(
                        category=status_category(response.code),
                        response=response,
                        response_info=response_info(response, url, request),
                        exception=PubNubException(
                            errormsg=error,
                            pn_error=error_value(status),
                            status_code=status
                        )
                    ),
                    raw_result=response
                )

            def format_envelope(json, url, options, request, response):
                logger.debug('Generating Envelope')
                logger.debug('Status: ' + str(response.code))

                if response.code != 200:
                    raise PubNubTwistedException(
                        result=json,
                        status=options.create_status(
                            category=status_category(response.code),
                            response=response,
                            response_info=response_info(response, url, request),
                            exception=PubNubException(
                                errormsg=str(json),
                                pn_error=error_value(response.code),
                                status_code=response.code
                            )
                        ),
                        raw_result=json
                    )

                else:
                    return TwistedEnvelope(
                        result=options.create_response(json),
                        status=options.create_status(
                            category=status_category(response.code),
                            response=response,
                            response_info=response_info(response, url, request),
                            exception=None
                        ),
                        raw_result=json
                    )

            return treq.json_content(response) \
                .addErrback(handle_error, response.code, response, url, request) \
                .addCallback(format_envelope, url, options, request, response)

        def handle_error(error):
            if error.check(ResponseNeverReceived):
                return
            logger.error(error)
            raise error

        def request_options(method, url, reactor, headers, persistent, body):
            return {
                "method": method,
                "url": url,
                "reactor": reactor,
                "headers": headers,
                "persistent": persistent,
                "body": body
            }

        def url(scheme, origin, path, qs):
            logger.debug("QS: %s", qs)
            return utils.build_url(scheme, origin, path, qs)

        def determine_agent(path):
            if re.compile('/v2/subscribe/.*/.*/0').match(path):
                logger.debug('Using subscribe agent')
                return self.sub_agent
            else:
                logger.debug('Using non subscribe agent')
                return self.non_sub_agent

        options = options_func()
        logger.debug('Options: %s', options)

        if options.operation_type == PNOperationType.PNPublishOperation:
            options.merge_params_in(
                {"seqn": self._publish_sequence_manager.get_next_sequence()}
            )
        else:
            options.merge_params_in({})

        logger.debug("url: %s", str(url(self.config.scheme(), self.base_origin, options.path, options.query_string)))
        logger.debug(options.method_string)
        logger.debug(options.data)
        logger.debug(determine_agent(options.path))
        logger.debug("query_string: %s", options.query_string)
        return treq.request(options.method_string,
                            url(self.config.scheme(), self.base_origin, options.path, options.query_string),
                            reactor=self.reactor,
                            headers=self.headers,
                            persistent=True,
                            body=options.data,
                            agent=determine_agent(options.path)) \
            .addCallback(handle_response,
                         url(self.config.scheme(), self.base_origin, options.path, options.query_string),
                         options,
                         request_options(
                             options.method_string,
                             url(self.config.scheme(), self.base_origin, options.path, options.query_string),
                             self.reactor,
                             self.headers,
                             True,
                             options.data
                         )) \
            .addErrback(handle_error)


class TwistedSubscriptionManager(SubscriptionManager):
    def __init__(self, pubnub_instance):
        self.clock = pubnub_instance.clock
        self._continue_subscription = True
        self._message_queue = DeferredQueue()
        self._subscribe_deferred = None

        super(TwistedSubscriptionManager, self).__init__(pubnub_instance)

        self._subscribe_message_worker = TwistedSubscribeMessageWorker(
            self._pubnub,
            self._listener_manager,
            self._message_queue,
            None
        )

    def _subscribe_call(self):
        def manage_success(envelope):
            logger.debug('Handling successful subscribe call')
            try:
                logger.debug(envelope.status.client_request)
                logger.debug(envelope.raw_result)
                self._handle_endpoint_call(envelope.raw_result, envelope.status)
                self._subscribe_deferred = self._subscribe_call()
            except Exception as exception:
                raise exception

        def manage_failure(failure):
            logger.debug('Handling failed subscribe call')
            if failure.type == PubNubTwistedException:
                self._announce_status(failure.value.status)
                if failure.value.status.category in (PNStatusCategory.PNDisconnectedCategory,
                                                     PNStatusCategory.PNUnexpectedDisconnectCategory,
                                                     PNStatusCategory.PNCancelledCategory,
                                                     PNStatusCategory.PNBadRequestCategory,
                                                     PNStatusCategory.PNMalformedFilterExpressionCategory):
                    time.sleep(30)
            elif failure.check(CancelledError):
                return failure
            else:
                raise failure

        logger.debug('Running subscription')

        combined_channels = self._subscription_state.prepare_channel_list(True)
        combined_groups = self._subscription_state.prepare_channel_group_list(True)

        try:
            logger.debug("Subscribing with tt: %s, region: %s", self._timetoken, self._region)
            return Subscribe(self._pubnub) \
                .channels(combined_channels) \
                .channel_groups(combined_groups) \
                .timetoken(self._timetoken) \
                .region(self._region) \
                .filter_expression(self._pubnub.config.filter_expression) \
                .deferred() \
                .addCallbacks(
                    callback=manage_success,
                    errback=manage_failure
                )

        except Exception as exception:
            raise exception

    def _announce_status(self, status):
        self._listener_manager.announce_status(status)

    def _start_worker(self):
        logger.debug('Starting worker')
        self._subscribe_message_worker.run()

    def _set_consumer_event(self):
        raise NotImplementedError

    def _message_queue_put(self, message):
        logger.debug('Putting to message queue')
        self._message_queue.put(message)

    def _stop_heartbeat_timer(self):
        if self._heartbeat_loop is not None:
            self._heartbeat_loop.stop()

    def _register_heartbeat_timer(self):
        super(TwistedSubscriptionManager, self)._register_heartbeat_timer()
        self._heartbeat_loop = LoopingCall(self._perform_heartbeat_loop)
        interval = self._pubnub.config.heartbeat_interval / 2 - 1
        self._heartbeat_loop.start(interval, True)

    def _perform_heartbeat_loop(self):
        def heartbeat_callback(_, status):
            heartbeat_verbosity = self._pubnub.config.heartbeat_notification_options
            if heartbeat_verbosity == PNHeartbeatNotificationOptions.ALL or (
                            status.is_error() is True and heartbeat_verbosity == PNHeartbeatNotificationOptions.FAILURES):
                self._listener_manager.announce_status(status)

        state_payload = self._subscription_state.state_payload()
        channels = self._subscription_state.prepare_channel_list(False)
        channel_groups = self._subscription_state.prepare_channel_group_list(False)

        self._heartbeat_call = Heartbeat(self._pubnub) \
            .channels(channels) \
            .channel_groups(channel_groups) \
            .state(state_payload) \
            .async(heartbeat_callback)

    def _send_leave(self, unsubscribe_operation):
        def announce_leave_status(response, status):
            self._listener_manager.announce_status(status)

        Leave(self._pubnub) \
            .channels(unsubscribe_operation.channels) \
            .channel_groups(unsubscribe_operation.channel_groups) \
            .async(announce_leave_status)

    def stop(self):
        self._stop_subscribe_loop()

    def start(self):
        self._start_worker()
        self._start_subscribe_loop()

    def reconnect(self):
        logger.info('Reconnecting!')
        self._stop_subscribe_loop()
        self._start_subscribe_loop()

    def _start_subscribe_loop(self):
        logger.debug('Starting subscribe')
        if self._subscribe_deferred is None or self._subscribe_deferred.called:
            self._subscribe_deferred = self._subscribe_call()
            logger.debug('Subscribe started')

    def _stop_subscribe_loop(self):
        logger.debug('Stopping subscribe')
        if self._subscribe_deferred is not None:
            logger.debug(self._subscribe_deferred)
            self._subscribe_deferred.cancel()
            logger.debug('Subscribe stopped')


class TwistedSubscribeMessageWorker(SubscribeMessageWorker):
    def run(self):
        self._take_message()

    def _take_message(self):
        self._queue.get().addCallback(self.send_message_to_processing)

    def send_message_to_processing(self, message):
        self._take_message()
        logger.debug('Sending message to process')
        logger.debug(message)
        if message is not None:
            logger.debug('Taking message to process')
            # self._process_incoming_payload(message)
            threads.deferToThread(self._process_incoming_payload, message)


class TwistedEnvelope(object):
    def __init__(self, result, status, raw_result=None):
        self.result = result
        self.status = status
        self.raw_result = raw_result


class PubNubTwistedException(Exception):
    def __init__(self, result, status, raw_result=None):
        self.result = result
        self.status = status
        self.raw_result = raw_result

    def __str__(self):
        return str(self.status.error_data.exception)
