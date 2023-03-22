import argparse
import random
import sys
import time
from typing import List

import quickmq as mq
from quickmq import AmqpSession, NotAuthenticatedError

import pika


PUBLISH_THRESHOLD = 0.1
EPILOG_MESSAGES = [
    f"""If no arguments provided,
test will run with increasing messages until average publish time is greater than {PUBLISH_THRESHOLD} seconds""",
    "Note*: A RabbitMQ server needs to be running on localhost with default credentials guest/guest!"
]


def run_test(publisher, total_messages: int, message_size: int, confirm_delivery=False) -> float:
    start_time = time.time()
    msg_list = []
    for i in range(total_messages):
        new_msg = random.randbytes(message_size)
        publisher.publish(new_msg, confirm_delivery=confirm_delivery, exchange='amq.fanout')
        msg_list.append(new_msg)
    total_time = time.time() - start_time
    second_per_msg = total_time / total_messages
    verify_test(msg_list)
    return second_per_msg


def verify_test(expected_msgs: List[bytes]) -> None:
    con = pika.BlockingConnection()
    chan = con.channel()

    for ex_msg in expected_msgs:
        _, _, msg = chan.basic_get('performance_test', auto_ack=True)
        if bytes(msg) != ex_msg:
            raise TypeError(f"Did not get expected message {ex_msg} but message {msg}")


def test_setup(cmdln_args: List[str]) -> None:
    parser = argparse.ArgumentParser(
        prog='QuickMQ Performance Test',
        description='Test the performance of the quickmq package',
        epilog='\n'.join(EPILOG_MESSAGES)
    )

    parser.add_argument('-m', '--messages', type=int, help='Number of messages to publish')
    parser.add_argument('--threshold', default=PUBLISH_THRESHOLD, help='Time considered to be a quick enough publish')
    parser.add_argument('--confirm', action='store_true', help='Publish with confirm_delivery on')
    parser.add_argument('--session', action='store_true', help='Publish with a session instead of api')
    parser.add_argument('--msg_size', default=20, type=int, help='Size in bytes of published messages')

    args = parser.parse_args(cmdln_args)

    con = pika.BlockingConnection()
    chan = con.channel()
    chan.queue_declare('performance_test')
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

    if args.messages is not None:
        avg_publish = run_test(publisher, args.messages, args.msg_size, args.confirm)
        print(f'published {args.messages} messages in {args.messages * avg_publish} seconds (average of {avg_publish} messages per second)')
        if avg_publish >= args.threshold:
            raise RuntimeError(f'Average time to publish is greater than the threshold {args.threshold}')
        return

    messages = 2
    avg_publish = run_test(publisher, messages, args.msg_size, args.confirm)
    try:
        while avg_publish < args.threshold:
            print(f'published {messages} messages in {messages * avg_publish} seconds (average of {avg_publish} messages per second)')
            messages *= 2
            avg_publish = run_test(publisher, messages, args.msg_size, args.confirm)
        print(f'Could not publish {messages} messages with an average publish time less than {args.threshold}')
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    sys.exit(test_setup(sys.argv[1:]))
