SHELL := /bin/bash
PWD := $(shell pwd)
PYTHON := /usr/bin/python3.8
GIT_REMOTE = github.com/LaCumbancha/review-analysis

PROJECT_NAME = tp2

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	# Inputs
	GOOS=linux go build -o bin/review-scatter $(GIT_REMOTE)/nodes/inputs/reviews-scatter

	# Mappers
	GOOS=linux go build -o bin/funbiz-mapper $(GIT_REMOTE)/nodes/mappers/funbiz-mapper
	GOOS=linux go build -o bin/weekday-mapper $(GIT_REMOTE)/nodes/mappers/weekday
	GOOS=linux go build -o bin/user-mapper $(GIT_REMOTE)/nodes/mappers/user

	# Filters
	GOOS=linux go build -o bin/funbiz-filter $(GIT_REMOTE)/nodes/filters/funbiz-mapper

	# Aggregators
	GOOS=linux go build -o bin/funbiz-aggregator $(GIT_REMOTE)/nodes/aggregators/funbiz-mapper
	GOOS=linux go build -o bin/weekday-aggregator $(GIT_REMOTE)/nodes/aggregators/weekday
	GOOS=linux go build -o bin/user-aggregator $(GIT_REMOTE)/nodes/aggregators/user

	# Prettiers
	GOOS=linux go build -o bin/weekday-histogram $(GIT_REMOTE)/nodes/prettiers/weekday-histogram
	GOOS=linux go build -o bin/top-users $(GIT_REMOTE)/nodes/prettiers/top-users

	# Outputs
	GOOS=linux go build -o bin/sink $(GIT_REMOTE)/nodes/outputs/sink
	GOOS=linux go build -o bin/mock-receiver $(GIT_REMOTE)/nodes/outputs/mock-receiver
.PHONY: build

docker-image:
	# RabbitMQ
	docker build -f ./nodes/rabbitmq/Dockerfile -t "rabbitmq:custom" .

	# Inputs
	docker build -f ./nodes/inputs/reviews-scatter/Dockerfile -t "rvw_scatter:latest" .

	# Mappers
	docker build -f ./nodes/mappers/funny-business/Dockerfile -t "funbiz_mapper:latest" .
	docker build -f ./nodes/mappers/weekday/Dockerfile -t "weekday_mapper:latest" .
	docker build -f ./nodes/mappers/user/Dockerfile -t "user_mapper:latest" .

	# Filters
	docker build -f ./nodes/filters/funny-business/Dockerfile -t "funbiz_filter:latest" .
	docker build -f ./nodes/filters/user/Dockerfile -t "user_filter:latest" .

	# Aggregators
	docker build -f ./nodes/aggregators/funny-business/Dockerfile -t "funbiz_aggregator:latest" .
	docker build -f ./nodes/aggregators/weekday/Dockerfile -t "weekday_aggregator:latest" .
	docker build -f ./nodes/aggregators/user/Dockerfile -t "user_aggregator:latest" .

	# Prettiers
	docker build -f ./nodes/prettiers/weekday-histogram/Dockerfile -t "weekday_histogram_prettier:latest" .
	docker build -f ./nodes/prettiers/top-users/Dockerfile -t "top_users_prettier:latest" .

	# Outputs
	docker build -f ./nodes/outputs/sink/Dockerfile -t "sink:latest" .
	docker build -f ./nodes/outputs/mock-receiver/Dockerfile -t "mock_receiver:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	$(PYTHON) ./scripts/system-builder
	docker-compose -f docker-compose-dev.yaml --project-name $(PROJECT_NAME) up -d --build --remove-orphans
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose-dev.yaml --project-name $(PROJECT_NAME) stop -t 1
	docker-compose -f docker-compose-dev.yaml --project-name $(PROJECT_NAME) down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yaml --project-name $(PROJECT_NAME) logs -f
.PHONY: docker-compose-logs
