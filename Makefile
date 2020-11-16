SHELL := /bin/bash
PWD := $(shell pwd)
GIT_REMOTE = github.com/LaCumbancha/review-analysis

PROJECT_NAME = tp2

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	GOOS=linux go build -o bin/review-scatter $(GIT_REMOTE)/review-scatter
.PHONY: build

docker-image:
	docker build -f ./review-scatter/Dockerfile -t "review_scatter:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose-dev.yaml --project-name $(PROJECT_NAME) up -d --build --remove-orphans
	docker run --detach --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
.PHONY: docker-compose-up

docker-compose-down:
	./scripts/stop-extra-services
	docker-compose -f docker-compose-dev.yaml --project-name $(PROJECT_NAME) stop -t 1
	docker-compose -f docker-compose-dev.yaml --project-name $(PROJECT_NAME) down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yaml --project-name $(PROJECT_NAME) logs -f
.PHONY: docker-compose-logs
