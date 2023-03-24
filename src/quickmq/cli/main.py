"""
quickmq.cli.main
~~~~~~~~~~~~~~~~~~~~

Command line interface for QuickMQ package.
"""

import argparse
import sys
from typing import List, Optional
import logging

from quickmq import __version__
from quickmq.session import AmqpSession

log = logging.getLogger('quickmq')

vInfoStr = f"QuickMQ {__version__}"


def cmdln_publish(
    servers: List[str],
    exchange: str,
    username: str,
    password: str,
    route: str,
    messages: Optional[List[str]],
) -> None:
    with AmqpSession() as session:
        print(*servers)
        session.connect(*servers, auth=(username, password))
        if messages is not None:
            session.publish_all([(route, msg) for msg in messages], exchange=exchange, confirm_delivery=True)
            return

        try:
            for msg in iter(sys.stdin.readline, b''):
                session.publish(msg.strip(), exchange=exchange, confirm_delivery=True, key=route)
        except KeyboardInterrupt:
            return


def main(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(
        prog="quickmq", description="Use QuickMQ from the command line"
    )
    parser.add_argument(
        "-V", "--version", action="store_true", help="print the version of QuickMQ package"
    )
    parser.add_argument(
        "-v", "--verbose", default=0, action="count", help="specify verbosity of script"
    )

    subparsers = parser.add_subparsers(dest="cmd")
    publish_parser = subparsers.add_parser(
        "publish",
        description="Publish messages to rabbitmq server(s) from the command line",
        help="publish messages",
    )

    publish_parser.add_argument(
        "-s",
        "--servers",
        nargs="+",
        help="servers to publish message(s) to.",
        required=True,
    )
    publish_parser.add_argument(
        "-e",
        "--exchange",
        default='',
        help="exchange to publish message(s) to, default is '%(default)s'.",
    )
    publish_parser.add_argument(
        "-m", "--messages", nargs="+",
        help="The message(s) to publish. If not specified, quickmq will read from stdin.",
    )
    publish_parser.add_argument(
        "-u",
        "--username",
        help="username to connect to server with.",
        default='guest',
    )
    publish_parser.add_argument(
        "-p",
        "--password",
        help="password to connect to server with.",
        default='guest',
    )
    publish_parser.add_argument(
        '-k',
        '--key',
        default='',
        help="Routing key to publish message(s) with, default is '%(default)s'."
    )

    args = parser.parse_args(args=argv)

    levels = [logging.ERROR, logging.CRITICAL, logging.WARNING, logging.INFO, logging.DEBUG]
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('[%(levelname)s] - %(message)s'))
    log.addHandler(ch)
    log.setLevel(levels[min(4, args.verbose)])

    sub_command = getattr(args, "cmd")
    if sub_command == "publish":
        cmdln_publish(
            messages=args.messages,
            exchange=args.exchange,
            username=args.username,
            password=args.password,
            servers=args.servers,
            route=args.route
            )
    elif sub_command == "consume":
        raise NotImplementedError("command line consumption behavior is not yet implemented!")
    else:
        if getattr(args, "version"):
            print(vInfoStr)
        else:
            parser.print_usage()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
