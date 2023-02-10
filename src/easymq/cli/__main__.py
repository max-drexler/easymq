import argparse
import sys
from typing import List, Optional
import warnings

import easymq.config as cfg
from easymq.config import configure, CURRENT_CONFIG
from easymq import __version__
from ..session import get_current_session

vInfoStr = f"EasyMQ {__version__}"


def list_cfg_vars(print_values: bool) -> None:
    print("EasyMQ configuration variables:\n")
    for cfg_var in CURRENT_CONFIG:
        print(cfg_var, end="")
        if print_values:
            print(f"={getattr(cfg, cfg_var)}", end="")
        print("")
    print()


def main(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(
        prog="easymq", description="Use EasyMQ from the command line"
    )
    parser.add_argument(
        "--version", action="store_true", help="print the version of EasyMQ package"
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
    # consume_parser = subparsers.add_parser(
    #    "consume",
    #    description="not currently implemented...",
    #    help="listen for messages",
    # )
    vConfig_parser = subparsers.add_parser(
        "set",
        description="set EasyMQ configuration variables in the user configuration file",
        help="sets a configuration variable. Note this variable is set permanently for all future runs of EasyMQ.",
    )
    vLister_parser = subparsers.add_parser(
        "list",
        description="list all configuration variables",
        help="list all configuration valiables",
    )

    publish_parser.add_argument(
        "-s",
        "--servers",
        default=cfg.DEFAULT_SERVER,
        nargs="+",
        help="servers to publish message(s) to, default is '%(default)s'",
    )
    publish_parser.add_argument(
        "-e",
        "--exchange",
        default=cfg.DEFAULT_EXCHANGE,
        help="exchange to publish message(s) to, default is '%(default)s'",
    )
    publish_parser.add_argument(
        "-m", "--messages", nargs="+", help="The message(s) to publish", required=True
    )
    publish_parser.add_argument(
        "-u",
        "--username",
        help="username to connect to server with",
        default=cfg.DEFAULT_USER,
    )
    publish_parser.add_argument(
        "-p",
        "--password",
        help="password to connect to server with",
        default=cfg.DEFAULT_PASS,
    )

    vConfig_parser.add_argument(
        "variable", help="name of configuration variable to set"
    )
    vConfig_parser.add_argument(
        "value",
        nargs="?",
        help="the value to set the variable to, leave blank to reset to deafult",
    )

    vLister_parser.add_argument(
        "--values",
        action="store_true",
        help="list current values of configuration variables as well",
    )

    args = parser.parse_args(args=argv)

    sub_command = getattr(args, "cmd")
    if sub_command == "publish":
        get_current_session().connect(
            *getattr(args, "servers"),
            auth=(getattr(args, "username"), getattr(args, "password")),
        )
        get_current_session().publish_all(
            getattr(args, "messages"), exchange=getattr(args, "exchange")
        )
    elif sub_command == "consume":
        print("command line consumption is not yet implemented!")
    elif sub_command == "set":
        warnings.filterwarnings("error")
        var = getattr(args, "variable")
        val = getattr(args, "value")
        try:
            configure(var, val, durable=True)
            print(
                f"variable '{var}' set to '{val}' in config file at '{CURRENT_CONFIG.path}'"
            )
        except Exception:
            raise
    elif sub_command == "list":
        list_cfg_vars(getattr(args, "values"))
    else:
        if getattr(args, "version"):
            print(vInfoStr)
        else:
            parser.print_usage()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
