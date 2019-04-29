# vim:ft=make:noexpandtab:
.PHONY: unit-tests
.DEFAULT_GOAL := help

unit-tests: test ## Run unit-tests

test: ## Run tests
	rm -rf *.egg
	pipenv run setup.py test

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
