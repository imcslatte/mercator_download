#!/bin/bash
#


source /etc/profile.d/z00_lmod.sh
source ~/.bashrc
conda activate mercator


echo STARTING MERCATOR_DOWNLOAD
cd /home/om/cron/mercator


/home/om/cron/mercator/download_mercator_doppio.py

