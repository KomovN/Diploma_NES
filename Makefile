PY_SCRIPTS = $$(pwd)/py_scripts
DATA_DIR = $$(pwd)/data

.PHONY: process_raw_spark
process_raw_spark:
	python $(PY_SCRIPTS)/process_raw_spark.py \
	--data_dir $(DATA_DIR)/spark/raw_data \
	--output_path $(DATA_DIR)/spark/cur_spark_data_v2.parquet

.PHONY: process_raw_customs
process_raw_customs:
	python $(PY_SCRIPTS)/process_raw_customs.py \
	--data_path $(DATA_DIR)/gtd/gtd2005_2009 \
	--output_path $(DATA_DIR)/gtd/gtd_processed

.PHONY: prepare_tariffs
prepare_tariffs:
	python $(PY_SCRIPTS)/prepare_tariffs.py \
	--folder $(DATA_DIR)/tariffs/MFN \
	--target_path $(DATA_DIR)/instrument/tariffs.parquet

.PHONY: construct_instrument
construct_instrument:
	python $(PY_SCRIPTS)/construct_instrument_v2.py \
	--spark_path $(DATA_DIR)/spark/cur_spark_data_v2.parquet \
	--customs_path $(DATA_DIR)/gtd/gtd_processed/gtd2005.parquet \
	--tariffs_path $(DATA_DIR)/instrument/tariffs.parquet \
	--output_path $(DATA_DIR)/instrument/iv.parquet