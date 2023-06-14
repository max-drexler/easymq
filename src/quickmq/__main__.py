"""
quickmq.__main__
~~~~~~~~~~~~~~~~~~~~

Command line interface for QuickMQ package.
"""

import argparse
import sys
from typing import List, Optional, Tuple
import logging

from quickmq import __version__
from quickmq.config import CFG_FILE_PATH
from quickmq.client import AmqpClient

log = logging.getLogger("quickmq")

version_str = f"QuickMQ {__version__}"
title_str = r"""
_______        _      __   __  _______
__  __ \__  __(_)____/ /__/  |/  / __ \
_  / / / / / / / ___/ //_/ /|_/ / / / /
/ /_/ / /_/ / / /__/ ,< / /  / / /_/ /
\___\_\__,_/_/\___/_/|_/_/  /_/\___\_\
"""


def cmdln_publish(
    servers: List[str],
    exchange: str,
    username: str,
    password: str,
    route: str,
    messages: Optional[List[str]],
) -> None:
    with AmqpClient() as client:
        client.connect(*servers, auth=(username, password))
        if messages is not None:
            for msg in messages:
                client.publish(msg, key=route, exchange=exchange, confirm_delivery=True)
            return

        try:
            for msg in iter(sys.stdin.readline, b""):
                client.publish(
                    msg.strip(), exchange=exchange, confirm_delivery=True, key=route
                )
        except (KeyboardInterrupt, BrokenPipeError):
            return


def print_quickmq_info() -> None:
    print(title_str)
    print()
    print(f"version : {__version__}")
    print(f"user config file : {CFG_FILE_PATH}")


def parse_arguments(
    argv: Optional[List[str]] = None,
) -> Tuple[argparse.Namespace, argparse.ArgumentParser]:
    """Parses

    Args:
        argv (Optional[List[str]], optional): specific arguments to parse. Defaults to None.

    Returns:
        Tuple[argparse.Namespace, argparse.ArgumentParser]: the argparse namespace and parser
    """

    parser = argparse.ArgumentParser(
        prog="quickmq", description="Use QuickMQ from the command line"
    )
    parser.add_argument(
        "-V",
        "--version",
        action="store_true",
        help="print the version of QuickMQ package",
    )
    parser.add_argument(
        "-v", "--verbose", default=0, action="count", help="specify verbosity of script"
    )

    subparsers = parser.add_subparsers(dest="command")
    publish_parser = subparsers.add_parser(
        "publish",
        description="Publish messages to rabbitmq server(s) from the command line",
        help="Publish AMQP messages",
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
        default="",
        help="exchange to publish message(s) to, default is '%(default)s'.",
    )
    publish_parser.add_argument(
        "-m",
        "--messages",
        nargs="+",
        help="The message(s) to publish. If not specified, quickmq will read from stdin.",
    )
    publish_parser.add_argument(
        "-u",
        "--username",
        help="username to connect to server with.",
        default="guest",
    )
    publish_parser.add_argument(
        "-p",
        "--password",
        help="password to connect to server with.",
        default="guest",
    )
    publish_parser.add_argument(
        "-k",
        "--key",
        default="",
        help="Routing key to publish message(s) with, default is '%(default)s'.",
    )

    subparsers.add_parser(
        "info",
        description="Show more information about QuickMQ",
        help="Show QuickMQ information",
    )

    return parser.parse_args(argv), parser


def main(argv: Optional[List[str]] = None):
    args, parser = parse_arguments(argv)

    levels = [
        logging.ERROR,
        logging.CRITICAL,
        logging.WARNING,
        logging.INFO,
        logging.DEBUG,
    ]
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("[%(levelname)s] - %(message)s"))
    log.addHandler(ch)
    log.setLevel(levels[min(4, args.verbose)])

    sub_command = getattr(args, "command")
    if sub_command == "publish":
        cmdln_publish(
            messages=args.messages,
            exchange=args.exchange,
            username=args.username,
            password=args.password,
            servers=args.servers,
            route=args.key,
        )
    elif sub_command == "consume":
        raise NotImplementedError(
            "command line consumption behavior is not yet implemented!"
        )
    elif sub_command == "info":
        print_quickmq_info()
    else:
        if getattr(args, "version"):
            print(version_str)
        else:
            parser.print_usage()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
