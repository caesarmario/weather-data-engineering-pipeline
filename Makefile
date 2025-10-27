####
## [WIP] Makefile Weather Data Engineering Pipeline
## Mario Caesar // caesarmario87@gmail.com
####

PROJECT?=weather-pipeline

up:
\tdocker compose -p $(PROJECT) -f docker-compose.yml up -d

down:
\tdocker compose -p $(PROJECT) -f docker-compose.yml down

reup: down up

up-build:
\tdocker compose -p $(PROJECT) -f docker-compose.yml up -d --build

logs:
\tdocker compose -p $(PROJECT) -f docker-compose.yml logs -f --tail=200

ps:
\tdocker compose -p $(PROJECT) -f docker-compose.yml ps