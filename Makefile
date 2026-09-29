PYTHON ?= python3
CONFIG ?= config.json
INDEX_HOST ?= 0.0.0.0
INDEX_PORT ?= 9000
REPLICATION_FACTOR ?= 1
FILE ?= demo_a.txt
SOURCE_PEER ?= peer_a
.PHONY: all check index peer register search download
all: check
check:
	$(PYTHON) -m compileall -q .
index:
	$(PYTHON) server.py --host $(INDEX_HOST) --port $(INDEX_PORT) --replication-factor $(REPLICATION_FACTOR)
peer:
	$(PYTHON) peer_server.py --config $(CONFIG)
register:
	$(PYTHON) register.py --config $(CONFIG)
search:
	$(PYTHON) search.py --config $(CONFIG) --file "$(FILE)"
download:
	$(PYTHON) download.py --config $(CONFIG) --file "$(FILE)" --source-peer $(SOURCE_PEER)
