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

EasyMQ is a 

Let people know what your project can do specifically. Provide context and add a link to any reference visitors might be unfamiliar with. A list of Features or a Background subsection can also be added here. If there are alternatives to your project, this is a good place to list differentiating factors.

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.


## Installation

Install EasyMQ from PyPI.

```
pip install easymq
```


## Usage

### Publishing to localhost

```
from easymq import Publisher

# publish a message to the default exchange on localhost
pub = Publisher()
pub.publish("message")

# change exchange
pub.exchange = 'amq.topic'

# publish a list of messages
messages = [('route.key', {"message": "hello"}), ('route.key', {"message": "world!"})]

pub.publish_all(messages)

pub.close()
```

### Publishing to a server with credentials

```
from easymq import Publisher, MQCredentials

creds = MQCredentials("username", "password")
pub = Publisher(server="remote.server", credentials=creds)

pub.publish("Hello World!")

pub.close()
```

### Publishing to multiple servers

```
from easymq import PublisherPool, MQCredentials

creds = MQCredentials("username", "password")
pool = PublisherPool(credentials=creds)
pool.connect(['server.one', 'server.two'])

pool.publish("Hello World!")

pool.close()
```

### Create/Delete/Bind/Unbind exchanges/queues

```
from easymq import MQCredentials
from easymq.adapter import PikaClient

creds = MQCredentials('username', 'password')
client = PikaClient(credentials=creds)

client.declare_exchange('test_exchange', auto_delete=True)
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

