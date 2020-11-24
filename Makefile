SHELL := /bin/bash
PWD := $(shell pwd)
default: build

all:

docker-image:
	docker build -f ./business_controller/Dockerfile -t "business_controller:latest" .
	docker build -f ./review_controller/Dockerfile -t "review_controller:latest" .
	docker build -f ./aggregator/Dockerfile -t "aggregator:latest" .
	docker build -f ./joiner/Dockerfile -t "joiner:latest" .
	docker build -f ./reducer/Dockerfile -t "reducer:latest" .
	docker build -f ./sorter/Dockerfile -t "sorter:latest" .
	docker build -f ./filter/Dockerfile -t "filter:latest" .
	docker build -f ./multikeyaggregator/Dockerfile -t "multikeyaggregator:latest" .
	docker build -f ./multikeyreducer/Dockerfile -t "multikeyreducer:latest" .
	docker build -f ./sink/Dockerfile -t "sink:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose.yaml up -d --build --remove-orphans
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose.yaml stop -t 1
	docker-compose -f docker-compose.yaml down
.PHONY: docker-compose-down


docker-compose-logs:
	docker-compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs