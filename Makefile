IMAGE ?= shopping-sessions:latest
# Local Docker VM is small (4 GB); the scorer gives 8 CPUs / 24 GB. Override as needed.
CPUS  ?= 8
MEM   ?= 3g
PLATFORM ?= linux/amd64
PY ?= .venv/bin/python
SIZE ?= 1000000
SEED ?= 1
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures fixtures-local dev check-dev docker-check docker-time difftest

build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## all 36 fixtures in the scorer-like container
fixtures: build
	$(PY) scripts/run_fixtures.py fixtures -- scripts/docker_run.sh

## all 36 fixtures natively (fast loop)
fixtures-local:
	$(PY) scripts/run_fixtures.py fixtures -- ./scripts/run_rust.sh

dev:
	bin/generate --seed 1 --events 1000000 --clicks 250000 --output data/dev

## native run on dev + exact diff against the published answer
check-dev:
	rm -rf out/dev && ./scripts/run_rust.sh data/dev out/dev && cd scripts && ../$(PY) compare.py ../out/dev ../dev-answer

## docker run enforcing the scorer limits on dev, then exact diff
docker-check: build
	rm -rf out/docker-dev && time scripts/docker_run.sh data/dev out/docker-dev && cd scripts && ../$(PY) compare.py ../out/docker-dev ../dev-answer

## docker run on a generated set of SIZE events (data/s$(SIZE)), timed
docker-time: build
	scripts/gen.sh $(SIZE) $(SEED)
	rm -rf out/docker-$(SIZE) && time scripts/docker_run.sh data/s$(SIZE)_$(SEED) out/docker-$(SIZE)

difftest:
	scripts/difftest.sh
