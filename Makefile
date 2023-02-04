start_rabbitmq:
	@echo "Checking if rabbitmq container is running..."
	@if ! docker inspect rabbitmq &> /dev/null; then \
		echo "rabbitmq container not found. Setting up rabbitmq container..."; \
		docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management; \
	fi

stop_rabbitmq:
	@echo "Stopping rabbitmq container..."
	@if docker inspect rabbitmq &> /dev/null; then \
		docker stop rabbitmq; \
		docker rm rabbitmq; \
	else \
		echo "rabbitmq container not found."; \
	fi

.PHONY: stop_rabbitmq check_rabbitmq