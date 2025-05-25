PY_SCRIPTS = $$(pwd)/py_scripts
DATA_DIR = $$(pwd)/data

.PHONY: all process_raw_spark process_raw_customs prepare_tariffs construct_instrument prepare_data_simple

all: process_raw_spark process_raw_customs prepare_tariffs construct_instrument prepare_data_simple
process_raw_spark:
	python $(PY_SCRIPTS)/process_raw_spark.py \
	--data_dir $(DATA_DIR)/spark/raw_data \
	--output_path $(DATA_DIR)/spark/cur_spark_data_v3.parquet \
	--source CUR

process_raw_customs:
	python $(PY_SCRIPTS)/process_raw_customs.py \
	--data_path $(DATA_DIR)/gtd/gtd2005-2009 \
	--output_path $(DATA_DIR)/gtd/gtd_processed

prepare_tariffs:
	rm -rf $(DATA_DIR)/tariffs/MFN_processed
	python $(PY_SCRIPTS)/prepare_tariffs.py \
	--folder $(DATA_DIR)/tariffs/MFN \
	--target_path $(DATA_DIR)/instrument/tariffs.parquet

construct_instrument:
	python $(PY_SCRIPTS)/construct_instrument_v2.py \
	--spark_path $(DATA_DIR)/spark/cur_spark_data_v3.parquet \
	--customs_path $(DATA_DIR)/gtd/gtd_processed/gtd2005.parquet \
	--tariffs_path $(DATA_DIR)/instrument/tariffs.parquet \
	--output_path $(DATA_DIR)/instrument/iv.parquet

prepare_data_simple:
	python $(PY_SCRIPTS)/prepare_data_simple_v1.py \
	--spark_path $(DATA_DIR)/spark/cur_spark_data_v3.parquet \
	--ruslana_path $(DATA_DIR)/ruslana/ruslana.parquet \
	--gtd_path $(DATA_DIR)/gtd/gtd_processed \
	--iv_path $(DATA_DIR)/instrument/iv.parquet \
	--output_path $(DATA_DIR)/testing/cur_final_data_simple_v3.csv