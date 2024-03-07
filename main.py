import json
import argparse
import importlib
import logging
from pyspark.sql import SparkSession


def _parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required = True)
    return parser.parse_args()

def main():
    logger = logging.getLogger(__name__)
    logger.info('----Starting movies analysis----')
    args = _parse_arguments()

    with open('configuration.json') as config_file:
        config = json.load(config_file)
    
    spark = SparkSession.builder.appName(config.get("app_name")).getOrCreate()

    job_module = importlib.import_module(f"jobs.{args.job}")
    job_module.run_job(spark, config)

if __name__ == "__main__":
    main()


    
 