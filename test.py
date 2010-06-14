import unittest2 as unittest

from pylibrabbitmq import Message, Connection


class TestChannel(unittest.TestCase):

    def setUp(self):
        self.connection = Connection()
        self.connection.connect()
        self.channel = self.connection.channel()

    def test_send_message(self):
        message = Message("the quick brown fox jumps over the lazy dog",
                properties=dict(content_type="application/json",
                                content_encoding="utf-8"))
        self.channel.basic_publish(message, "celery", "celery")
        self.channel.basic_publish(message, "celery", "celery")
        self.channel.basic_publish(message, "celery", "celery")
        self.assertGreater(self.channel.queue_purge("celery"), 2)
        self.channel.basic_publish(message, "celery", "celery")
        self.channel.basic_publish(message, "celery", "celery")
        self.channel.basic_publish(message, "celery", "celery")

    def test_exchange_declare(self):
        self.channel.exchange_declare("tesxxx", "direct")

    def test_queue_declare(self):
        x = self.channel.queue_declare("tesxxx")
        self.assertIn("message_count", x)
        self.assertIn("consumer_count", x)
        self.assertEqual(x["queue"], "tesxxx")
        self.channel.queue_bind("tesxxx", "tesxxx", "rkey")

    def test_basic_get_ack(self):
        x = self.channel.basic_get("celery")
        raise Exception("BODY:%s INFO:%s PROPERTIES:%s" % (
            x.body, x.delivery_info, x.properties))
        self.assertIn("message_count", x)
        self.assertIn("redelivered", x)
        self.assertEqual(x["routing_key"], "celery")
        self.assertEqual(x["exchange"], "celery")
        self.assertTrue(x["delivery_tag"])
        self.assertTrue(x["content_type"])
        self.assertTrue(x["body"])
        self.channel.basic_ack(x["delivery_tag"])

    def tearDown(self):
        self.channel.close()
        self.connection.close()