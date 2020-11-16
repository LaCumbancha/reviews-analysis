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
	GOOS=linux go build -o bin/review-scatter $(GIT_REMOTE)/reviews-scatter
	GOOS=linux go build -o bin/review-receiver $(GIT_REMOTE)/reviews-receiver
.PHONY: build

docker-image:
	docker build -f ./reviews-scatter/Dockerfile -t "rvw_scatter:latest" .
	docker build -f ./reviews-receiver/Dockerfile -t "rvw_receiver:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose-dev.yaml --project-name $(PROJECT_NAME) up -d --build --remove-orphans
.PHONY: docker-compose-up

docker-compose-down:
	./scripts/stop-extra-services
	docker-compose -f docker-compose-dev.yaml --project-name $(PROJECT_NAME) stop -t 1
	docker-compose -f docker-compose-dev.yaml --project-name $(PROJECT_NAME) down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yaml --project-name $(PROJECT_NAME) logs -f
.PHONY: docker-compose-logs
