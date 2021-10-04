#!/bin/bash

for filename in src/main/outputs/*.txt; do
  tail -n 1 "$filename" > "${filename%.*}"_reduced.out
  done