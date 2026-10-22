.PHONY: install lint test generate pipeline-local validate
install:
	python -m pip install -e ".[dev]"
lint:
	ruff check src tests
test:
	pytest
generate:
	python -m ledgershift.generator.cli all
pipeline-local:
	python -m ledgershift.pipelines.local
validate:
	python -m ledgershift.validation.runner --source data/source/snapshot --target data/gold
