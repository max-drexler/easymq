# QuickMQ

An easy-to-use RabbitMQ client.

[![pipeline status](https://gitlab.ssec.wisc.edu/mdrexler/easymq/badges/main/pipeline.svg)](https://gitlab.ssec.wisc.edu/mdrexler/easymq/-/commits/main)

[![coverage report](https://gitlab.ssec.wisc.edu/mdrexler/easymq/badges/main/coverage.svg)](https://gitlab.ssec.wisc.edu/mdrexler/easymq/-/commits/main)

[![Latest Release](https://gitlab.ssec.wisc.edu/mdrexler/easymq/-/badges/release.svg)](https://gitlab.ssec.wisc.edu/mdrexler/easymq/-/releases)


## Table of Contents

* [Description](#description)
* [Installation](#installation)
* [Usage](#usage)
    * [Default Behavior](#changing-defaults)
* [Contributing](#contributing)
* [Authors](#authors)


## Description

QuickMQ is a purely python implementation of a RabbitMQ client. QuickMQ abstracts concepts like connection drops, authentication, and message encoding to make interacting with RabbitMQ as easy as possible.  

### When is QuickMQ a Good Fit
* When you need to publish AMQP messages
* When setup is not a priority
* When multiple server connections are required

### What QuickMQ ***Cannot*** Do

* Work Queues
* Transactions
* Confirm that a consumer received a message

QuickMQ is meant to be fairly simple in order to reduce the complexity for a user so these concepts are left out. Clients like [pika](https://github.com/pika/pika) or [aio-pika](https://github.com/mosquito/aio-pika) might be a better fit if you are looking for these features.

## Installation

QuickMQ is currently still being tested, but it can be installed from the PyPI index.

```
pip install quickmq
```

### Requirements

Python >= 3.7

## Usage

QuickMQ uses 'configuration variables' to make interacting with RabbitMQ trivial. For example, publishing to your default server and exchange with default credentials can be done in two lines of code.

```
import quickmq as mq

mq.publish('Hello World!')
```

Configuration variables are not required. The previous code can be generalized to connect to any server(s) with any credentials as follows.

```
import quickmq as mq

mq.connect('server1', 'server2', auth=('username', 'password'))

mq.publish('Hello World!', exchange='amq.topic', key='intro.test')
```

This will publish 'Hello World!' to the 'amq.topic' exchange on both server1 and server2.

### Changing Defaults

The following is a list of all configuration varaibles, what they do, their default value, and acceptable value.

| Variable Name    | Acceptable Values | Default Value | What it does |
|:----------------:|:-----------------:|:------------:|:------------:|
| RECONNECT_TRIES  | int  | 5   | How many times quickmq will try to reconnect to a server, negative for infinite.
| RECONNECT_DELAY  | float >= 0  | 5.0 | Delay between reconnect attempts.
| DEFAULT_SERVER   |     str     | "localhost" |Default server to connect to.
| DEFAULT_EXCHANGE |     str     | ""  | Default exchange to publish to.
| DEFAULT_USER     |     str     | "guest" | Default username to connect with.
| DEFAULT_PASS     |     str     | "guest" | Default password to connect with.
|DEFAULT_ROUTE_KEY |     str     |   ""    | Default routing key to publish with.
| RABBITMQ_PORT    |     int     |  5672   | Port to connect to rabbitmq server with.

To change a configuration variable use the following command:

```
quickmq.configure('default_server', 'new_server', durable=True)
```

This will permenently change the default server that QuickMQ will connect to. Set durable to false (the default) to make the change for the current runtime only. Using None as the second argument will reset the configuration variable back to its default value. 

```
quickmq.configure('default_server', None, durable=True)
```

## Contributing

Contributions welcome!  

Currently need to implement consumer behaviour and topology editor.  
Docker is required to run tests.  
To run tests simply use the Makefile:

```
make test
```

## Authors

Created/Maintained by [Max Drexler](mailto:mndrexler@wisc.edu)

## License

MIT License. See LICENSE for more information.

