FALA ?= uv run fala
DB ?= runtime.db
PIPELINE_DIR ?= ..
RUN_INPUT ?= run-input.example.yaml
SOURCE_LIST ?= source-list.example.csv
SOURCE_LIST_RUN_INPUT ?= run-input.from-source-list.json
RUN_ID ?= run_basic_enrichment_sample
WORKER_ID ?= local-worker

.PHONY: bootstrap validate doctor validate-run source-list create run-local clean

bootstrap: validate doctor validate-run source-list

validate:
	$(FALA) --pipeline-dir $(PIPELINE_DIR) validate --json --check-commands

doctor:
	$(FALA) --pipeline-dir $(PIPELINE_DIR) package-doctor

validate-run:
	$(FALA) --pipeline-dir $(PIPELINE_DIR) validate-run --run-input $(RUN_INPUT)

source-list:
	$(FALA) discover-documents \
	  --source-list $(SOURCE_LIST) \
	  --pipeline basic_enrichment \
	  --run-id $(RUN_ID) \
	  > $(SOURCE_LIST_RUN_INPUT)

create:
	$(FALA) --pipeline-dir $(PIPELINE_DIR) create-run \
	  --db $(DB) \
	  --run-input $(RUN_INPUT)

run-local:
	$(FALA) --pipeline-dir $(PIPELINE_DIR) run-until-idle \
	  --db $(DB) \
	  --pipeline basic_enrichment \
	  --run-id $(RUN_ID) \
	  --worker-id $(WORKER_ID) \
	  --adapter-kind subprocess

clean:
	rm -f $(DB) $(SOURCE_LIST_RUN_INPUT)
