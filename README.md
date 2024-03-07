# NewDay

## Overview
 This repository contains PySpark code responsible for movies data analysis.

## Input data location
 http://files.grouplens.org/datasets/movielens/ml-1m.zip

## Code structure
 Program execution starts at main.py which executes movieanalysis.py job and this job performs Extract, Transform, Load sequentially.

## Execution
 ### Local
  > This program is supported with Makefile (commandsforwindows). Once executed using 'make all' command will execute pytest module followed by build. Once executed new zip files will be created under \dist directory which will be used for submitting the spark job.
  Note: this needs to be executed only if there is a change in source code
  > There is a requirements.txt file to suport intalling required packages 'pip install -r requirements'.
  > Use following spark submit command to execute the program
    **spark-submit --py-files dist\jobs.zip,dist\extract.zip,dist\transform.zip,dist\load.zip --files configuration.json main.py --job movieanalysis**
