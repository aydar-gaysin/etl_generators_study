PROJECT_DIR = $(dir $(abspath $(firstword $(MAKEFILE_LIST))))
VENV = .venv
PYTHON = $(VENV)/bin/python
PRE_COMMIT = $(VENV)/bin/pre-commit

.PHONY: install
install:
	@echo 'Installing python dependencies...'
	@poetry install

	@echo 'Installing and updating pre-commit...'
	@$(PRE_COMMIT) install
	@$(PRE_COMMIT) autoupdate

.PHONY: format
format:
	@$(PRE_COMMIT) run --all

.PHONY: docker-start-postgres
docker-start-postgres:
	@docker run -d \
	--name etl-generators-postgres -p 6437:5432 \
	-e POSTGRES_PASSWORD=123qwe \
	-e POSTGRES_USER=app \
	-e POSTGRES_DB=etl-generators \
	postgres:13

.PHONY: docker-stop-postgres
docker-stop-postgres:
	@docker stop etl-generators-postgres
	@docker rm etl-generators-postgres

.PHONY: jupyter
jupyter:
	export PYTHONPATH=$(PROJECT_DIR)src && $(PYTHON) -m jupyter notebook --no-browser --notebook-dir=src/
