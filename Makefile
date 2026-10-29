PROJECT ?= telegram-max-bridge
COMPOSE ?= docker compose -p $(PROJECT) $(if $(wildcard 02-messenger-sync/.env),--env-file 02-messenger-sync/.env)
WAIT_TIMEOUT ?= 180
TAIL ?= 200
DISCOVER_SECONDS ?= 90
# One source of health thresholds for the Docker healthcheck, monitor, make health and make status.
HEALTH_STALE_AFTER ?= 180
HEALTH_QUEUE_AGE ?= 300
export HEALTH_STALE_AFTER HEALTH_QUEUE_AGE
HEALTH_ARGS = --stale-after $(HEALTH_STALE_AFTER) --queue-age $(HEALTH_QUEUE_AGE)
STAND_PROJECT ?= $(PROJECT)-stand

.DEFAULT_GOAL := help
.PHONY: help build up down update logs status health test smoke stand doctor discover stop-bridge cli

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
	@echo 'make doctor  - проверить ботов и группы'
	@echo 'make discover - показать ID групп (bridge должен быть остановлен)'
	@echo 'make stop-bridge - остановить только bridge перед изменяющими командами'
	@echo "make cli ARGS='queue list' - любая команда bridge.py в выбранном проекте"
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
	$(COMPOSE) run --rm --no-deps -T bridge status $(HEALTH_ARGS)

health:
	$(COMPOSE) run --rm --no-deps -T bridge health $(HEALTH_ARGS)

doctor:
	$(COMPOSE) run --rm --no-deps -T bridge doctor

discover:
	$(COMPOSE) run --rm --no-deps -T bridge discover --seconds $(DISCOVER_SECONDS)

stop-bridge:
	$(COMPOSE) stop bridge

cli:
	$(if $(strip $(ARGS)),,$(error Укажите команду: make cli ARGS='queue list'))
	$(COMPOSE) run --rm --no-deps -T bridge $(ARGS)

test: build
	$(COMPOSE) --profile tools run --rm --no-deps -T test

smoke: build
	$(COMPOSE) --profile tools run --rm --no-deps -T smoke
	sh 02-messenger-sync/poc/stand/guard_test.sh
	$(MAKE) --no-print-directory stand

stand:
	WORK_PROJECT='$(PROJECT)' STAND_PROJECT='$(STAND_PROJECT)' sh 02-messenger-sync/poc/stand/stand.sh
