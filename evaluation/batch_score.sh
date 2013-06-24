#!/bin/bash
echo $1 $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-01-01" -e "2012-02-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-02-01" -e "2012-03-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-03-01" -e "2012-04-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-04-01" -e "2012-05-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-05-01" -e "2012-06-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-06-01" -e "2012-07-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-07-01" -e "2012-08-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-08-01" -e "2012-09-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-09-01" -e "2012-10-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-10-01" -e "2012-11-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-11-01" -e "2012-12-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-12-01" -e "2013-01-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2013-01-01" -e "2013-02-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2013-02-01" -e "2013-03-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2013-03-01" -e "2013-04-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2013-04-01" -e "2013-05-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2
python /home/vic/work/Finance/evaluation/score.py -s "2012-01-01" -e "2013-05-01" -db "/home/vic/work/data/embers_v.db" -sw $1 -model $2 -all


