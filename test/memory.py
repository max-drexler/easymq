import sys
import time

import quickmq as mq


def main(argv) -> None:
    offset = 0
    total_msgs = 1000

    mq.connect('localhost')
    for i in range(total_msgs):
        mq.publish(f'testing{i}!')
        if i % 15 == 0:
            offset += .01
        print(f"Waiting {offset:0.3f} seconds, published {i} messages")
    time.sleep(offset)
    mq.disconnect()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
