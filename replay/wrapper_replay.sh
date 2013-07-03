#!/bin/bash

pre_date=$1
stock_index=$2

if [[ $pre_date < '2013-06-01' ]]
then
    cat ./trend_range_2013-06-13.txt | python bayesian_model_v1.py -zw tcp://*:30115 -zs tcp://*:30116 -rg $pre_date -s $stock_index
else
    cat ./trend_range_2013-06-13.txt | python bayesian_model.py -zw tcp://*:30115 -zs tcp://*:30116 --rege_date $pre_date --stock_list $stock_index
fi
