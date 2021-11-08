import pika
import time
import os
import json
import logging
import ast
from extract_prosodic.main import extract
import threading
import functools
from files_ms_client import download, upload

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

FILES_SERVER = os.environ.get("FILES_SERVER", "localhost:3001") 
QUEUE_SERVER_HOST, QUEUE_SERVER_PORT = os.environ.get("QUEUE_SERVER", "localhost:5672").split(":")
Q_IN = os.environ.get("INPUT_QUEUE_NAME", "low_level_in")
Q_OUT = os.environ.get("OUTPUT_QUEUE_NAME", "low_level_out")

def callback(channel, method, properties, body, args):

    (connection, threads) = args
    delivery_tag = method.delivery_tag
    t = threading.Thread(target=do_work, args=(
        connection, channel, delivery_tag, body))
    t.start()
    threads.append(t)


def do_work(connection, channel, delivery_tag, body):
    try:
        print(" [x] Received %r" % body, flush=True)
        args = json.loads(body)
        file = download(args['file']['name'], url="http://" + FILES_SERVER, buffer=True)
        result = ast.literal_eval(file.decode('utf-8'))

        count = 0
        dict_result = {}
        previous_duration = 0
        for key, value in result.items():
            dict_result[count] = {}
            if count == 0:
                dict_result[count]['pause'] = float(value['timestamp'])
            else:
                dict_result[count]['pause'] = float(
                    value['timestamp']) - previous_duration

            dict_result[count]['init_time'] = float(value['timestamp'])
            previous_duration = float(
                value['timestamp']) + float(value['duration'])
            dict_result[count]['pitch'], dict_result[count]['volume'] = extract(
                value['bytes'])
            count += 1

        payload = bytes(str(dict_result), encoding='utf-8')

        uploaded = upload(payload, url="http://" + FILES_SERVER, buffer=True, mime='text/plain')

        message = {
                **args,
                'low-level-output': uploaded
                }

        connection_out = pika.BlockingConnection(
            pika.ConnectionParameters(host=QUEUE_SERVER_HOST, port=QUEUE_SERVER_PORT))
        channel2 = connection_out.channel()

        channel2.queue_declare(queue=Q_OUT, durable=True)
        channel2.basic_publish(
            exchange='', routing_key=Q_OUT, body=json.dumps(message))

    except Exception as e:
        print('Connection Error %s' % e, flush=True)

    print(" [x] Done", flush=True)
    cb = functools.partial(ack_message, channel, delivery_tag)
    connection.add_callback_threadsafe(cb)


def ack_message(channel, delivery_tag):
    """Note that `channel` must be the same pika channel instance via which
    the message being ACKed was retrieved (AMQP protocol constraint).
    """
    if channel.is_open:
        channel.basic_ack(delivery_tag)
    else:
        # Channel is already closed, so we can't ACK this message;
        # log and/or do something that makes sense for your app in this case.
        pass


def consume():
    logging.info('[x] start consuming')
    success = False
    while not success:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=QUEUE_SERVER_HOST, port=QUEUE_SERVER_PORT, heartbeat=5))
            channel = connection.channel()
            success = True
        except:
            time.sleep(30)
            pass

    channel.queue_declare(queue=Q_IN, durable=True)
    channel.queue_declare(queue=Q_OUT, durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C', flush=True)
    channel.basic_qos(prefetch_count=1)

    threads = []
    on_message_callback = functools.partial(
        callback, args=(connection, threads))
    channel.basic_consume(queue=Q_IN,
                          on_message_callback=on_message_callback)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()

    # Wait for all to complete
    for thread in threads:
        thread.join()

    connection.close()

consume()