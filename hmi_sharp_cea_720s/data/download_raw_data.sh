#!/bin/bash

conda activate ../../env/
# It seems like the step size could be kept constant at two weeks, which would make it unnecessary
# to run ../../scripts/download_raw_data.py multiple times. That would result in more requests though

python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20100501 20110430 MS keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20110501 20110531 16D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20110601 20110731 MS keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20110801 20120131 16D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20120201 20120229 MS keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20120301 20120630 16D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20120701 20120731 MS keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20120801 20121031 16D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20121101 20121130 MS keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20121201 20150831 16D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20150901 20151130 MS keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20151201 20151231 16D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20160101 20160131 MS keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20160201 20160229 16D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20160301 20220131 MS keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20220201 20220228 16D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20220301 20220331 MS keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20220401 20230228 16D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20230301 20230314 14D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20230315 20230331 17D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20230401 20230531 16D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20230601 20230630 15D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20230701 20230731 16D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20230801 20230930 16D keywords.txt
python ../../scripts/download_raw_data.py hmi.sharp_cea_720s 20231001 20240821 15D keywords.txt

conda deactivate
