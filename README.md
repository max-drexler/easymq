# QuickMQ

An easy-to-use RabbitMQ client.

[![pipeline status](https://gitlab.ssec.wisc.edu/mdrexler/easymq/badges/main/pipeline.svg)](https://gitlab.ssec.wisc.edu/mdrexler/easymq/-/commits/main)
[![coverage report](https://gitlab.ssec.wisc.edu/mdrexler/easymq/badges/main/coverage.svg)](https://gitlab.ssec.wisc.edu/mdrexler/easymq/-/commits/main)
[![Latest Release](https://gitlab.ssec.wisc.edu/mdrexler/easymq/-/badges/release.svg)](https://gitlab.ssec.wisc.edu/mdrexler/easymq/-/releases)


## Table of Contents

* [Description](#description)
* [Installation](#installation)
* [Features](#features)
* [Docs](#documentation)
* [Contributing](#contributing)
* [Authors](#authors)


## Description

QuickMQ is a purely python implementation of a RabbitMQ client. QuickMQ abstracts quite a few RabbitMQ concepts like channels, publisher confirms, and message encoding to make interacting with RabbitMQ as easy as possible.  

These abstractions make the package a good fit for anyone looking to quickly and easily interact with RabbitMQ servers.

### What QuickMQ ***Cannot*** Do

* Work queues
* Transactions
* Consumer confirmations

QuickMQ is simple on purpose, so if you require the above features or the abstractions don't work well for you, clients like [pika](https://github.com/pika/pika) or [aio-pika](https://github.com/mosquito/aio-pika) might be a better fit.

## Installation

QuickMQ is currently still being tested, but it can be installed from the PyPI index.

```
pip install quickmq
```

To install the most recent version from GitLab.

```
pip install git+https://gitlab.ssec.wisc.edu/mdrexler/easymq.git
```

### Requirements

Python >= 3.6

## Features

QuickMQ comes with some useful features, all which are easy to use.

### Multi-Server Actions

With QuickMQ you're able to connect to and publish/listen for messages from multiple servers at the same time.

```
import quickmq as mq

mq.connect('server1', 'server2', auth=('user', 'pass'))

mq.publish('Hello World!') # publishes to both servers
```

### Automatic Reconnection

Server connections can reconnect automatically in case of sporatic drops.

```
import quickmq as mq

mq.configure('RECONNECT_TRIES', 3)
mq.configure('RECONNECT_DELAY', 60)
# Connections will try to reconnect 3 times,
# waiting a minute between each attempt
```

### Command Line Interface

QuickMQ also installs with a CLI for use outside of python scripts.

```
$ quickmq publish -e exchange -s server(s) -u username -p password
```

Use `quickmq --help` for more information.

## Documentation

See the GitLab [wiki]() for more documentation.

## Contributing

Contributions welcome!  

Docker is required to run tests.  
To run tests simply use the Makefile:

```
make test
```

## Authors

Created/Maintained by [Max Drexler](mailto:mndrexler@wisc.edu)

## License

MIT License. See LICENSE for more information.

