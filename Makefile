PROJECT ?= telegram-max-bridge
COMPOSE ?= docker compose -p $(PROJECT)
WAIT_TIMEOUT ?= 180
TAIL ?= 200

.DEFAULT_GOAL := help
.PHONY: help build up down update logs status health test smoke stand

help:
	@echo 'make build   - собрать образ из uv.lock'
	@echo 'make up      - запустить bridge и monitor (ждёт healthcheck)'
	@echo 'make down    - остановить сервисы; volume с состоянием сохраняется'
	@echo 'make update  - собрать новый образ, затем пересоздать сервисы'
	@echo 'make logs    - последние логи (FOLLOW=1 для слежения)'
	@echo 'make status  - состояние контейнеров и очереди'
	@echo 'make health  - диагностика моста (exit 1 при проблеме)'
	@echo 'make test    - unit-тесты внутри контейнера'
	@echo 'make smoke   - smoke-стенды внутри контейнера и контейнерный стенд'
	@echo "Проект Compose: $(PROJECT) (переопределяется PROJECT=...)"

build:
	$(COMPOSE) build bridge

up:
	$(COMPOSE) up -d --wait --wait-timeout $(WAIT_TIMEOUT) bridge monitor

down:
	$(COMPOSE) down

# A failed build stops make before any running container is touched.
update: build
	$(COMPOSE) up -d --no-build --wait --wait-timeout $(WAIT_TIMEOUT) bridge monitor

logs:
	$(COMPOSE) logs --tail=$(TAIL) $(if $(FOLLOW),--follow) bridge monitor

status:
	$(COMPOSE) ps --all bridge monitor
	$(COMPOSE) run --rm --no-deps -T bridge status

health:
	$(COMPOSE) run --rm --no-deps -T bridge health

test: build
	$(COMPOSE) --profile tools run --rm --no-deps -T test

smoke: build
	$(COMPOSE) --profile tools run --rm --no-deps -T smoke
	$(MAKE) --no-print-directory stand

stand:
	sh 02-messenger-sync/poc/stand/stand.sh
