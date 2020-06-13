# README

## Overview

Reads harvest data entry templates for each harvest year then:

  1. Adjust/removes values based on QC file created after a manual review
  2. Appends data from other files (results from NIR and mass spec)
  3. Calculates yield (field dry and standard moisture at 12.5%) and biomass on area basis
  4. Cleans headers and merges datasets
  5. Prints to csv file

## Entry point

[src/clean-aggregate.py](src/clean-aggregate.py)

## TODO

* Once workflow is solid, need to create modules so can run individual years at a time, generate per-year output, then merge newly created files. Likely use a make file for this.
