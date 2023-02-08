.PHONY: stop_rabbitmq check_rabbitmq test tox

RABBIT_MQ_CONTAINER_NAME:=rabbitmq
RABBIT_MQ_IMAGE:=rabbitmq:3-management

test: start_rabbitmq run_tests stop_rabbitmq

run_tests:
	tox

start_rabbitmq:
	@echo "Starting RabbitMQ server"
	docker pull $(RABBIT_MQ_IMAGE)
	docker kill $(RABBIT_MQ_CONTAINER_NAME) || true
	docker run --rm -d \
		--name $(RABBIT_MQ_CONTAINER_NAME) \
		-p 5671:5671 \
		-p 5672:5672 \
		-p 15671:15671 \
		-p 15672:15672 \
		$(RABBIT_MQ_IMAGE)		

stop_rabbitmq:
	@echo "Stopping rabbitmq container..."
	docker kill $(RABBIT_MQ_CONTAINER_NAME) || true