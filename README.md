# EasyMQ

An easy-to-use AMQP client.

## Table of Contents

* [Description](#description)
* [Installation](#installation)
* [Usage](#usage)
    * [Change Default Behavior](#change-default-behavior)
* [Contributing](#contributing)
* [Authors](#authors)


## Description

EasyMQ is a purely pythonic implementation of an amqp client that can publish, and eventually listen for, amqp messages on one or more RabbitMQ servers. EasyMQ handles connection drops, authentication, and message encoding to make interacting with RabbitMQ as easy as possible.  

EasyMQ is a synchronous adapter 

### What EasyMQ Can Do For You

* Publish messsages to one or multiple RabbitMQ servers
* Edit RabbitMQ broker topology (create queues/exchanges, etc)
* Confirm that messages have been delivered to the broker
* Publish messages to exchanges with or without routing keys and publish directly to queues


### What EasyMQ ***Cannot*** Do For You

* Consume messages, but expect this in the future
* Work Queues
* Transactions
* Confirm that a consumer received a message

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.


## Installation

Install EasyMQ from PyPI.

```
pip install easymq
```

### Requirements

Python >= 3.7

## Usage

The most basic way to publish messages to an AMQP server.

```
import easymq

easymq.publish('Hello World!', auth=('guest', 'password'))
```

Please note that this method of publishing is not efficient for long running publishers. Check documentation for a better suited publishing interface.


### Change Default Behavior

EasyMQ uses a set of variables, called configuration variables in the code, to define default behavior and how easymq operates.

The following is a list of all configuration varaibles, what they do, their default value, and acceptable value.

| Variable Name    | Acceptable Values | Default Value | What it does |
|:----------------:|:-----------------:|:------------:|:------------:|
| RECONNECT_TRIES  | int  | 5   | How many times easymq will try to reconnect to a server, negative for infinite.
| RECONNECT_DELAY  | float >= 0  | 5.0 | Delay between reconnect attempts.
| DEFAULT_SERVER   |     str     | "localhost" |Default server to connect to.
| DEFAULT_EXCHANGE |     str     | ""  | Default exchange to connect to.
| DEFAULT_USER     |     str     | "guest" | Default username to connect with.
| DEFAULT_PASS     |     str     | "guest" | Default password to connect with.
|DEFAULT_ROUTE_KEY |     str     |   ""    | Routing key to use when not specified in method calls
| RABBITMQ_PORT    |     int     |  5672   | Port to connect to rabbitmq server with

There are multiple ways to change the default value of configuration variables.

```
import easymq

# this will set RECONNECT_DELAY to 10 seconds
easymq.configure('reconnect_delay', 10)

# this will tell easymq to try to reconnect forever
easymq.configure('RECONNECT_TRIES', -1)
```

If you don't want to call configure 

## Contributing

Contributions welcome! Currently need to implement consumption behavior.  
Docker and pip are required to run tests.  
To run tests simply use the Makefile:

```
make test
```

## Authors

Created/Maintained by [Max Drexler](mailto:mndrexler@wisc.edu)

## License

MIT License. See LICENSE for more information.

