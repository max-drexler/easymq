import argparse
import logging
import random
import sys
import time
from typing import List

import quickmq as mq
from quickmq import AmqpSession
from quickmq.exceptions import NotAuthenticatedError

import pika

#log = logging.getLogger('quickmq')
#log.addHandler(logging.StreamHandler())
#log.setLevel(logging.DEBUG)

PUBLISH_THRESHOLD = 0.1
EPILOG_MESSAGES = [
    f"""If no arguments provided,
test will run with increasing messages until average publish time is greater than {PUBLISH_THRESHOLD} seconds""",
    "Note*: A RabbitMQ server needs to be running on localhost with default credentials guest/guest!"
]


def run_test(publisher, total_messages: int, message_size: int, confirm_delivery=False) -> float:
    msg_list = (random.randbytes(message_size) for _ in range(total_messages))
    start_time = time.time()
    for new_msg in msg_list:
        publisher.publish(new_msg, confirm_delivery=confirm_delivery, exchange='amq.fanout')
    total_time = time.time() - start_time
    second_per_msg = total_time / total_messages
    verify_test(msg_list)
    return second_per_msg


def verify_test(expected_msgs: List[bytes]) -> None:
    msg_index = 0

    def on_msg(chan, __, ___, msg: bytes) -> None:
        nonlocal msg_index
        ex_msg = expected_msgs[msg_index]
        if ex_msg != msg:
            if len(ex_msg) <= 20:
                raise TypeError(f"Got message {msg} instead of expected message {ex_msg}!")
            raise TypeError(f"Got message {msg} instead of expected message {ex_msg[:20]}...")
        msg_index += 1
        if msg_index >= len(expected_msgs):
            chan.basic_cancel(consume_tag)
    con = pika.BlockingConnection()
    chan = con.channel()
    consume_tag = chan.basic_consume('performance_test', on_message_callback=on_msg, auto_ack=True)


def test_setup(cmdln_args: List[str]) -> None:
    parser = argparse.ArgumentParser(
        prog='QuickMQ Performance Test',
        description='Test the performance of the quickmq package',
        epilog='\n'.join(EPILOG_MESSAGES)
    )

    parser.add_argument('--num_msgs', type=int, help='Number of messages to publish')
    parser.add_argument('--threshold', default=PUBLISH_THRESHOLD, help='Time considered to be a quick enough publish')
    parser.add_argument('--confirm', action='store_true', help='Publish with confirm_delivery on')
    parser.add_argument('--session', action='store_true', help='Publish with a session instead of api')
    parser.add_argument('--msg_size', default=20, type=int, help='Size in bytes of published messages')

    args = parser.parse_args(cmdln_args)

    con = pika.BlockingConnection()
    chan = con.channel()
    chan.queue_declare('performance_test')
    chan.queue_purge('performance_test')
    chan.queue_bind('performance_test', 'amq.fanout')
    con.close()

    try:
        if args.session:
            publisher = AmqpSession()
            publisher.connect('localhost')
        else:
            mq.connect('localhost')
            publisher = mq
    except (ConnectionError, NotAuthenticatedError):
        print('Need a RabitMQ running on localhost with default user/password')
        return

    if args.num_msgs is not None:
        avg_publish = run_test(publisher, args.num_msgs, args.msg_size, args.confirm)
        print_performance(args.msg_size, args.num_msgs, avg_publish)
        if avg_publish >= args.threshold:
            raise RuntimeError(f'Average time to publish is greater than the threshold {args.threshold}')
        return

    messages = 2
    avg_publish = run_test(publisher, messages, args.msg_size, args.confirm)
    try:
        while avg_publish < float(args.threshold):
            print_performance(args.msg_size, messages, avg_publish)
            messages *= 2
            avg_publish = run_test(publisher, messages, messages, args.confirm)
        print(f'Could not publish {messages} messages with an average publish time less than {args.threshold}')
    except KeyboardInterrupt:
        pass


def print_performance(msg_size: int, num_msgs: int, average_publish_time: float) -> None:
    print(f"""published {num_msgs} {msg_size}b messages in {num_msgs * average_publish_time} seconds
(average of {average_publish_time} seconds per message)""")
    print(f'{1 / average_publish_time} messages per second\n')


if __name__ == '__main__':
    sys.exit(test_setup(sys.argv[1:]))
