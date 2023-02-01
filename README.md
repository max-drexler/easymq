# EasyMQ

An easy-to-use AMQP client.

## Table of Contents

* [Description](#description)
* [Installation](#installation)
* [Usage](#usage)
    * [Basic Publishing](#basic-publishing)
    * [Multi-Server Publishing](#publishing-to-multiple-servers)
    * [Editing Topology](#editing-rabbitmq-server-topology)
    * [Change Default Behavior](#change-default-behavior)
* [Contributing](#contributing)
* [Authors](#authors)


## Description

EasyMQ is a purely pythonic implementation of an amqp client that can publish, and eventually listen for, amqp messages on one or more RabbitMQ servers. EasyMQ handles connection drops, authentication, and message encoding to make interacting with RabbitMQ as easy as possible.

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.


## Installation

Install EasyMQ from PyPI.

```
pip install easymq
```

### Requirements

EasyMQ uses the python library [pika](https://github.com/pika/pika).  
Note that it will be automatically installed using pip.

## Usage

### Basic Publishing

```
from easymq import Publisher

# create a publisher
# connects to localhost by default
pub = Publisher()

# publish a message
# publishes to rabbitmq default exchange by default
pub.publish("Hello World!")

# switch to a different exchange if it exists
if pub.exchange_exists("amq.topic"):
    pub.exchange = "amq.topic"

# publish a message using a routing key
pub.publish("Hello!", route_key="topic.string")

# connect to a server with specific credentials
from easymq import MQCredentials
creds = MQCredentials('username', 'password')
pub.connect("new.server.com", credentials=creds)

# publish multiple messages at once
messages = ["message1", ("routing.key", "message2"), ("other.key", "message3"), "message4"]
pub.publish_all(messages)

# close connection to server
pub.close()

```

### Publishing to multiple servers

```
from easymq import PublisherPool, MQCredentials

# authenticate client for all servers
creds = MQCredentials("username", "password")
pool = PublisherPool(credentials=creds)

# connect to any number of servers
pool.connect(['server.one', 'server.two'])

# publish a message to all servers
pool.publish("Hello World!")

# change the exchnage on all servers 
# note exchange won't change on servers where the new exchange doesn't exist
pool.exchange = 'new_exchange'

# change exchange of one server
pool['server.one'].exchange = 'different_exchange'

# publish multiple messages at once
messages = ["message1", ("routing.key", "message2"), ("other.key", "message3"), "message4"]
pub.publish_all(messages)

# close connections
pool.close()
```

### Editing RabbitMQ Server Topology

```
from easymq import Publisher

with Publisher().connect('rabbitmq.server') as pub:
    pub('declare_exchange', 'new_exchange', auto_delete=True)
    pub('declare_queue', 'new_queue', durable=True)
    pub('bind_queue', 'new_queue', 'new_exchange')
```

### Change Default Behavior

To edit the default behavior of easymq, import the package and directly edit the values of the configuration variables.

```
import easymq

easymq.DEFAULT_USER = "my_default_username"
pub = easymq.Publisher() # <- will automatically use my_default_username
```

The following is a list of all configuration varaibles, what they do, their default value, and acceptable value.

| Variable Name    | Acceptable Values | Default Value | What it does |
|:----------------:|:-----------------:|:------------:|:------------:|
| RECONNECT_TRIES  | int or None | 5   | The number of times easymq will try to reconnect to a server after a disconnect, None for infinite.
| RECONNECT_DELAY  | float >= 0  | 5.0 | Delay between reconnect attempts.
| DEFAULT_SERVER   |     str     | "localhost" |Default server to connect to.
| DEFAULT_EXCHANGE |     str     | ""  | Default exchange to connect to.
| DEFAULT_USER     |     str     | "guest" | Default username to connect with.
| DEFAULT_PASS     |     str     | "guest" | Default password to connect with.
|DEFAULT_ROUTE_KEY |     str     |   ""    | Routing key to use when not specified in method calls
| RABBITMQ_PORT    |     int     |  5672   | Port to connect to rabbitmq server with

Permently setting configuration variables in development.

## Roadmap

Will eventually create a Consumer class to listen to messages from RabbitMQ servers.

## Contributing
Open to contributions, currently looking to create a consumer class.

For people who want to make changes to your project, it's helpful to have some documentation on how to get started. Perhaps there is a script that they should run or some environment variables that they need to set. Make these steps explicit. These instructions could also be useful to your future self.

You can also document commands to lint the code or run tests. These steps help to ensure high code quality and reduce the likelihood that the changes inadvertently break something. Having instructions for running tests is especially helpful if it requires external setup, such as starting a Selenium server for testing in a browser.

## Authors

Created/Maintained by [Max Drexler](mailto:mndrexler@wisc.edu)

## License

MIT License. See LICENSE for more information.

