# EasyMQ

A simple AMQP message publisher.

## Integrate with your tools

- [ ] [Set up project integrations](https://gitlab.ssec.wisc.edu/mdrexler/easymq/-/settings/integrations)


## Test and Deploy

Use the built-in continuous integration in GitLab.

- [ ] [Get started with GitLab CI/CD](https://docs.gitlab.com/ee/ci/quick_start/index.html)
- [ ] [Analyze your code for known vulnerabilities with Static Application Security Testing(SAST)](https://docs.gitlab.com/ee/user/application_security/sast/)

***

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

# create a publisher on localhost with default credentials
pub = Publisher()

# publish to the default exchange
pub.publish("Hello World!")

# switch to a different exchange if it exists
if pub.exchange_exists("amq.topic"):
    pub.exchange = "amq.topic"

# publish using a routing key
pub.publish("Hello!", route_key="topic.string")

# connect to a remote server with specific credentials
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
from easymq import MQCredentials
from easymq.adapter import PikaClient

# create client credentials for server 
creds = MQCredentials('username', 'password')

# create a client on desired server
client = PikaClient(credentials=creds, server='localhost')

# create/delete/bind exchanges and queues
client.declare_exchange('test_exchange', durable=True)
client.declare_queue('new_queue', auto_delete=True)
client.bind_queue('new_queue', 'test_exchange')
```


## Roadmap

Will eventually create a Consumer class to listen to messages from RabbitMQ servers.

## Contributing
State if you are open to contributions and what your requirements are for accepting them.

For people who want to make changes to your project, it's helpful to have some documentation on how to get started. Perhaps there is a script that they should run or some environment variables that they need to set. Make these steps explicit. These instructions could also be useful to your future self.

You can also document commands to lint the code or run tests. These steps help to ensure high code quality and reduce the likelihood that the changes inadvertently break something. Having instructions for running tests is especially helpful if it requires external setup, such as starting a Selenium server for testing in a browser.

## Authors

Developed by [Max Drexler](mailto:mndrexler@wisc.edu)

## License

MIT License. See LICENSE for more information.

