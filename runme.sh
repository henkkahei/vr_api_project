#!/bin/bash

if [ -d "$PWD/data/datalake/staging/vr_data/2023/" ]; then
  rm -rf data/datalake/staging/vr_data/2023/
fi

if [ -f "$PWD/data/warehouse/vr.duckdb" ]; then
  rm data/warehouse/vr.duckdb
fi

if [ -d "$PWD/.venv/scripts/" ]; then
  source $PWD/.venv/Scripts/activate
else
  source $PWD/.venv/bin/activate
fi

mkdir -p data/warehouse

echo
echo \*\*\* Running project.py - add data to database
echo
sleep 0.5

$PWD/.venv/Scripts/python ./src/project.py $PWD

echo \*\*\* Data inserted to bronze
sleep 5

echo \*\*\* Moving to project\'s dbt_vr_data folder and running dbt build to add data to silver and gold tables

cd dbt_vr_data

dbt deps

dbt build

echo
echo \*\*\* Creating documentation
dbt docs generate

dbt docs serve

echo \*\*\* Move to visualization notebook \(2_bi_tool.ipynb\)