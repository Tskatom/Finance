#!/bin/bash
source /home/vic/.profile
python /home/vic/work/Finance/Util/notifier.py >> /tmp/q.log
echo "good! $(date)" >> /tmp/q.log
