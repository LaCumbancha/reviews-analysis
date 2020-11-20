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
	GOOS=linux go build -o bin/review-scatter $(GIT_REMOTE)/nodes/inputs/reviews-scatter
	GOOS=linux go build -o bin/mock-receiver $(GIT_REMOTE)/nodes/outputs/mock-receiver
	GOOS=linux go build -o bin/funbiz-mapper $(GIT_REMOTE)/nodes/mappers/funbiz-mapper
	GOOS=linux go build -o bin/funbiz-filter $(GIT_REMOTE)/nodes/filters/funbiz-mapper
	GOOS=linux go build -o bin/funbiz-aggregator $(GIT_REMOTE)/nodes/aggregators/funbiz-mapper
.PHONY: build

docker-image:
	docker build -f ./nodes/rabbitmq/Dockerfile -t "rabbitmq:custom" .
	docker build -f ./nodes/inputs/reviews-scatter/Dockerfile -t "rvw_scatter:latest" .
	docker build -f ./nodes/mappers/funny-business/Dockerfile -t "funbiz_mapper:latest" .
	docker build -f ./nodes/filters/funny-business/Dockerfile -t "funbiz_filter:latest" .
	docker build -f ./nodes/aggregators/funny-business/Dockerfile -t "funbiz_aggregator:latest" .
	docker build -f ./nodes/outputs/sink/Dockerfile -t "sink:latest" .

	# Mocked receiver
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
