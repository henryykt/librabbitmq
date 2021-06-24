# coding: utf-8
from __future__ import absolute_import

import socket
import unittest

from librabbitmq import Message, Connection, ConnectionError, ChannelError
TEST_QUEUE = 'pyrabbit.testq'


class test_Channel(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(host='localhost:5672', userid='guest',
                                     password='guest', virtual_host='/')
        self.channel = self.connection.channel()
        self.channel.queue_delete(TEST_QUEUE)
        self._queue_declare()

    def test_send_message(self):
        message = Message(
            channel=self.channel,
            body='the quick brown fox jumps over the lazy dog',
            properties=dict(content_type='application/json',
                            content_encoding='utf-8'))
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        self.assertGreater(self.channel.queue_purge(TEST_QUEUE), 2)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)

    def test_nonascii_headers(self):
        message = Message(
            channel=self.channel,
            body='the quick brown fox jumps over the lazy dog',
            properties=dict(content_type='application/json',
                            content_encoding='utf-8',
                            headers={'key': r'¯\_(ツ)_/¯'}))
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)

    def _queue_declare(self):
        self.channel.exchange_declare(TEST_QUEUE, 'direct')
        x = self.channel.queue_declare(TEST_QUEUE)
        self.assertEqual(x.message_count, x[1])
        self.assertEqual(x.consumer_count, x[2])
        self.assertEqual(x.queue, TEST_QUEUE)
        self.channel.queue_bind(TEST_QUEUE, TEST_QUEUE, TEST_QUEUE)

    def test_basic_get_ack(self):
        message = Message(
            channel=self.channel,
            body='the quick brown fox jumps over the lazy dog',
            properties=dict(content_type='application/json',
                            content_encoding='utf-8'))
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        while True:
            x = self.channel.basic_get(TEST_QUEUE)
            if x:
                break
        self.assertIs(self.channel, x.channel)
        self.assertIn('message_count', x.delivery_info)
        self.assertIn('redelivered', x.delivery_info)
        self.assertEqual(x.delivery_info['routing_key'], TEST_QUEUE)
        self.assertEqual(x.delivery_info['exchange'], TEST_QUEUE)
        self.assertTrue(x.delivery_info['delivery_tag'])
        self.assertTrue(x.properties['content_type'])
        self.assertTrue(x.body)
        x.ack()

    def test_timeout_burst(self):
        """Check that if we have a large burst of messages in our queue
        that we can fetch them with a timeout without needing to receive
        any more messages."""

        message = Message(
            channel=self.channel,
            body='the quick brown fox jumps over the lazy dog',
            properties=dict(content_type='application/json',
                            content_encoding='utf-8'))

        for i in range(100):
            self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)

        messages = []

        def cb(x):
            messages.append(x)
            x.ack()

        self.channel.basic_consume(TEST_QUEUE, callback=cb)
        for i in range(100):
            self.connection.drain_events(timeout=0.2)

        self.assertEqual(len(messages), 100)

    def test_timeout(self):
        """Check that our ``drain_events`` call actually times out if
        there are no messages."""
        message = Message(
            channel=self.channel,
            body='the quick brown fox jumps over the lazy dog',
            properties=dict(content_type='application/json',
                            content_encoding='utf-8'))

        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)

        messages = []

        def cb(x):
            messages.append(x)
            x.ack()

        self.channel.basic_consume(TEST_QUEUE, callback=cb)
        self.connection.drain_events(timeout=0.1)

        with self.assertRaises(socket.timeout):
            self.connection.drain_events(timeout=0.1)
        self.assertEqual(len(messages), 1)

    def test_properies(self):
        """Check that nested properties are sent / received. """
        body = 'the quick brown fox jumps over the lazy dog'
        props = {
            'content_type': 'application/json',
            'content_encoding': 'utf-8',
            'headers': {
                'none': None,
                'list': [-1, 0, 1, 2, 3, 4, 0.1, -0.1, -1.0],
                'list2': [
                    pow(2, 63) - 1, -1 * pow(2, 63), 1.54334E-34, -1.54334E-34
                ],
                'list3': [True, False, None],
                'list4': ["a", "b", "c"],
                'tuple': ("a", "c", True, False),
                'true': True,
                'false': False,
                'zero': 0,
                'minus_one': -1,
                'min_int': -1 * pow(2, 31),
                'max_int': pow(2, 31) - 1,
                'long': pow(2, 32),
                'long2': -1 * pow(2, 32),
                'long3': pow(2, 63) - 1,
                'long4': -1 * pow(2, 63),
                'float': 3.14159265359,
                'float2': 1.54334E-34,
                'float3': -1.54334E-34,
                'unicode': '¯\_(ツ)_/¯',
                'unicode_array': [r'♠♥', r'♣♦', r'¯\_(ツ)_/¯']
            }
        }
        message = Message(
            channel=self.channel,
            body=body,
            properties=props)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        while True:
            x = self.channel.basic_get(TEST_QUEUE)
            if x:
                break

        # tuple is converted to list
        props['headers']['tuple'] = list(props['headers']['tuple'])
        self.maxDiff = None

        self.assertIs(self.channel, x.channel)
        self.assertIn('message_count', x.delivery_info)
        self.assertIn('redelivered', x.delivery_info)
        self.assertEqual(x.delivery_info['routing_key'], TEST_QUEUE)
        self.assertEqual(x.delivery_info['exchange'], TEST_QUEUE)
        self.assertTrue(x.delivery_info['delivery_tag'])
        self.assertTrue(x.properties['content_type'])
        self.assertEqual(x.body.decode(), body)
        self.assertEqual(x.properties, props)
        x.ack()

    def test_resend_message(self):
        """Check that it's possible to 'forward' a message"""
        body = b'the quick brown fox jumps over the lazy dog'
        props = {
            'content_type': 'application/text',
            'content_encoding': 'utf-8',
        }

        messages = []
        msg = Message(channel=self.channel, body=body, properties=props)

        # resend message got from basic_get
        self.channel.basic_publish(msg, TEST_QUEUE, TEST_QUEUE)
        while True:
            x = self.channel.basic_get(TEST_QUEUE)
            if x:
                x.ack()
                messages.append(x)
                break

        self.assertEqual(len(messages), 1)

        self.channel.basic_publish(messages[-1], TEST_QUEUE, TEST_QUEUE)

        # resend message got from basic_consume (message body is memoryview)
        def cb(x):
            x.ack()
            messages.append(x)

        self.channel.basic_consume(TEST_QUEUE, callback=cb)
        self.connection.drain_events(timeout=0.2)

        self.assertEqual(len(messages), 2)

        self.channel.basic_publish(messages[-1], TEST_QUEUE, TEST_QUEUE)
        self.connection.drain_events(timeout=0.2)

        self.assertEqual(len(messages), 3)
        self.assertEqual(messages[-1].body.tobytes(), body)

    def test_message_body(self):
        """Check that message body has the same type for short and long messages"""
        body1 = b'the quick brown fox jumps over the lazy dog'
        body2 = body1 * 1000000 # long body
        props = {
            'content_type': 'application/text',
            'content_encoding': 'utf-8',
        }

        msg1 = Message(channel=self.channel, body=body1, properties=props)
        msg2 = Message(channel=self.channel, body=body2, properties=props)

        bodies = [body1, body2, body1, body2]
        messages = []

        # basic_get
        self.channel.basic_publish(msg1, TEST_QUEUE, TEST_QUEUE)
        self.channel.basic_publish(msg2, TEST_QUEUE, TEST_QUEUE)
        while len(messages) < 2:
            x = self.channel.basic_get(TEST_QUEUE)
            if x:
                messages.append(x)
                x.ack()

        # basic_consume
        self.channel.basic_publish(msg1, TEST_QUEUE, TEST_QUEUE)
        self.channel.basic_publish(msg2, TEST_QUEUE, TEST_QUEUE)

        def cb(x):
            messages.append(x)
            x.ack()

        self.channel.basic_consume(TEST_QUEUE, callback=cb)
        for i in range(2):
            self.connection.drain_events(timeout=0.2)

        self.assertEqual(len(messages), 4)

        for i, msg in enumerate(messages):
            body = msg.body
            if isinstance(body, memoryview):
                body = body.tobytes()

            self.assertEqual(body, bodies[i], msg="check body (i={})".format(i))



    def tearDown(self):
        if self.channel and self.connection.connected:
            self.channel.queue_purge(TEST_QUEUE)
            self.channel.close()
        if self.connection:
            try:
                self.connection.close()
            except ConnectionError:
                pass


class test_Delete(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(host='localhost:5672', userid='guest',
                                     password='guest', virtual_host='/')
        self.channel = self.connection.channel()
        self.TEST_QUEUE = 'pyrabbitmq.testq2'
        self.channel.queue_delete(self.TEST_QUEUE)

    def test_delete(self):
        """Test that we can declare a channel delete it, and then declare with
        different properties"""

        self.channel.exchange_declare(self.TEST_QUEUE, 'direct')
        self.channel.queue_declare(self.TEST_QUEUE)
        self.channel.queue_bind(
            self.TEST_QUEUE, self.TEST_QUEUE, self.TEST_QUEUE,
        )

        # Delete the queue
        self.channel.queue_delete(self.TEST_QUEUE)

        # Declare it again
        x = self.channel.queue_declare(self.TEST_QUEUE, durable=True)
        self.assertEqual(x.queue, self.TEST_QUEUE)

        self.channel.queue_delete(self.TEST_QUEUE)

    def test_delete_empty(self):
        """Test that the queue doesn't get deleted if it is not empty"""
        self.channel.exchange_declare(self.TEST_QUEUE, 'direct')
        self.channel.queue_declare(self.TEST_QUEUE)
        self.channel.queue_bind(self.TEST_QUEUE, self.TEST_QUEUE,
                                self.TEST_QUEUE)

        message = Message(
            channel=self.channel,
            body='the quick brown fox jumps over the lazy dog',
            properties=dict(content_type='application/json',
                            content_encoding='utf-8'))

        self.channel.basic_publish(message, self.TEST_QUEUE, self.TEST_QUEUE)

        with self.assertRaises(ChannelError):
            self.channel.queue_delete(self.TEST_QUEUE, if_empty=True)

        # We need to make a new channel after a ChannelError
        self.channel = self.connection.channel()

        x = self.channel.basic_get(self.TEST_QUEUE)
        self.assertTrue(x.body)

        self.channel.queue_delete(self.TEST_QUEUE, if_empty=True)

    def tearDown(self):
        if self.channel and self.connection.connected:
            self.channel.queue_purge(TEST_QUEUE)
            self.channel.close()
        if self.connection:
            try:
                self.connection.close()
            except ConnectionError:
                pass
