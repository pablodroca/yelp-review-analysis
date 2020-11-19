SHELL := /bin/bash
PWD := $(shell pwd)
default: build

all:

docker-image:
	docker build -f ./reader/Dockerfile -t "reader:latest" .
	docker build -f ./business_controller/Dockerfile -t "business_controller:latest" .
	docker build -f ./review_controller/Dockerfile -t "review_controller:latest" .
	docker build -f ./joiner/Dockerfile -t "joiner:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose.yaml stop -t 1
	docker-compose -f docker-compose.yaml down
.PHONY: docker-compose-down


docker-compose-logs:
	docker-compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs