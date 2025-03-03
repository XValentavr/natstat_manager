remove-volumes:
	docker-compose down --volumes

start:
	docker compose up

build:
	docker compose build

stop:
	docker compose down

docker-prune:
	docker image prune && docker volume prune && docker container prune

lint:
	pre-commit run --all-files