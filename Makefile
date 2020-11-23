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
	GOOS=linux go build -o bin/business-scatter $(GIT_REMOTE)/nodes/inputs/business-scatter

	# Mappers
	GOOS=linux go build -o bin/funbiz-mapper $(GIT_REMOTE)/nodes/mappers/funbiz-mapper
	GOOS=linux go build -o bin/funcit-mapper $(GIT_REMOTE)/nodes/mappers/citbiz-mapper
	GOOS=linux go build -o bin/weekday-mapper $(GIT_REMOTE)/nodes/mappers/weekday
	GOOS=linux go build -o bin/user-mapper $(GIT_REMOTE)/nodes/mappers/user
	GOOS=linux go build -o bin/stars-mapper $(GIT_REMOTE)/nodes/mappers/stars
	GOOS=linux go build -o bin/hash-mapper $(GIT_REMOTE)/nodes/mappers/hash-text

	# Filters
	GOOS=linux go build -o bin/funbiz-filter $(GIT_REMOTE)/nodes/filters/funbiz-filter
	GOOS=linux go build -o bin/funcit-filter $(GIT_REMOTE)/nodes/filters/funcit-filter
	GOOS=linux go build -o bin/user-filter $(GIT_REMOTE)/nodes/filters/user
	GOOS=linux go build -o bin/stars-filter $(GIT_REMOTE)/nodes/filters/stars
	GOOS=linux go build -o bin/dishash-filter $(GIT_REMOTE)/nodes/filters/distinct-hash
	GOOS=linux go build -o bin/botuser-filter $(GIT_REMOTE)/nodes/filters/bot-users

	# Aggregators
	GOOS=linux go build -o bin/funbiz-aggregator $(GIT_REMOTE)/nodes/aggregators/funbiz-aggregator
	GOOS=linux go build -o bin/funcit-aggregator $(GIT_REMOTE)/nodes/aggregators/funcit-aggregator
	GOOS=linux go build -o bin/weekday-aggregator $(GIT_REMOTE)/nodes/aggregators/weekday
	GOOS=linux go build -o bin/user-aggregator $(GIT_REMOTE)/nodes/aggregators/user
	GOOS=linux go build -o bin/stars-aggregator $(GIT_REMOTE)/nodes/aggregators/stars
	GOOS=linux go build -o bin/hash-aggregator $(GIT_REMOTE)/nodes/aggregators/hash-text
	GOOS=linux go build -o bin/dishash-aggregator $(GIT_REMOTE)/nodes/aggregators/distinct-hash

	# Joiners
	GOOS=linux go build -o bin/funcit-joiner $(GIT_REMOTE)/nodes/joiners/funny-city
	GOOS=linux go build -o bin/bestuser-joiner $(GIT_REMOTE)/nodes/joiners/best-users
	GOOS=linux go build -o bin/botuser-joiner $(GIT_REMOTE)/nodes/joiners/bot-users

	# Prettiers
	GOOS=linux go build -o bin/top-funniest-cities $(GIT_REMOTE)/nodes/prettiers/top-funniest-cities
	GOOS=linux go build -o bin/weekday-histogram $(GIT_REMOTE)/nodes/prettiers/weekday-histogram
	GOOS=linux go build -o bin/top-users $(GIT_REMOTE)/nodes/prettiers/top-users
	GOOS=linux go build -o bin/best-users $(GIT_REMOTE)/nodes/prettiers/best-users

	# Outputs
	GOOS=linux go build -o bin/sink $(GIT_REMOTE)/nodes/outputs/sink
	GOOS=linux go build -o bin/mock-receiver $(GIT_REMOTE)/nodes/outputs/mock-receiver
.PHONY: build

docker-image:
	# RabbitMQ
	docker build -f ./nodes/rabbitmq/Dockerfile -t "rabbitmq:custom" .

	# Inputs
	docker build -f ./nodes/inputs/reviews-scatter/Dockerfile -t "rvw_scatter:latest" .
	docker build -f ./nodes/inputs/business-scatter/Dockerfile -t "biz_scatter:latest" .

	# Mappers
	docker build -f ./nodes/mappers/funny-business/Dockerfile -t "funbiz_mapper:latest" .
	docker build -f ./nodes/mappers/city-business/Dockerfile -t "citbiz_mapper:latest" .
	docker build -f ./nodes/mappers/weekday/Dockerfile -t "weekday_mapper:latest" .
	docker build -f ./nodes/mappers/user/Dockerfile -t "user_mapper:latest" .
	docker build -f ./nodes/mappers/stars/Dockerfile -t "stars_mapper:latest" .
	docker build -f ./nodes/mappers/hash-text/Dockerfile -t "hash_mapper:latest" .

	# Filters
	docker build -f ./nodes/filters/funny-business/Dockerfile -t "funbiz_filter:latest" .
	docker build -f ./nodes/filters/funny-city/Dockerfile -t "funcit_filter:latest" .
	docker build -f ./nodes/filters/user/Dockerfile -t "user_filter:latest" .
	docker build -f ./nodes/filters/stars/Dockerfile -t "stars_filter:latest" .
	docker build -f ./nodes/filters/distinct-hash/Dockerfile -t "dishash_filter:latest" .
	docker build -f ./nodes/filters/bot-users/Dockerfile -t "botuser_filter:latest" .

	# Aggregators
	docker build -f ./nodes/aggregators/funny-business/Dockerfile -t "funbiz_aggregator:latest" .
	docker build -f ./nodes/aggregators/funny-city/Dockerfile -t "funcit_aggregator:latest" .
	docker build -f ./nodes/aggregators/weekday/Dockerfile -t "weekday_aggregator:latest" .
	docker build -f ./nodes/aggregators/user/Dockerfile -t "user_aggregator:latest" .
	docker build -f ./nodes/aggregators/stars/Dockerfile -t "stars_aggregator:latest" .
	docker build -f ./nodes/aggregators/hash-text/Dockerfile -t "hash_aggregator:latest" .
	docker build -f ./nodes/aggregators/distinct-hash/Dockerfile -t "dishash_aggregator:latest" .

	# Joiners
	docker build -f ./nodes/joiners/funny-city/Dockerfile -t "funcit_joiner:latest" .
	docker build -f ./nodes/joiners/best-users/Dockerfile -t "bestuser_joiner:latest" .
	docker build -f ./nodes/joiners/bot-users/Dockerfile -t "botuser_joiner:latest" .

	# Prettiers
	docker build -f ./nodes/prettiers/funniest-cities/Dockerfile -t "top_funniest_cities_prettier:latest" .
	docker build -f ./nodes/prettiers/weekday-histogram/Dockerfile -t "weekday_histogram_prettier:latest" .
	docker build -f ./nodes/prettiers/top-users/Dockerfile -t "top_users_prettier:latest" .
	docker build -f ./nodes/prettiers/best-users/Dockerfile -t "best_users_prettier:latest" .

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
