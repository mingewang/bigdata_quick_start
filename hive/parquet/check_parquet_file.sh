#!/bin/bash

# follow https://arrow.apache.org/docs/python/parquet.html
# to create parquet file

# pip install parquet-cli

parq example.parquet --schema

parq example.parquet --head  10
:
