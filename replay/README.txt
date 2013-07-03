Instruction to run the script to replay the warning
The warning will be directly output to the sysout

A> Bayesian Model
run wrapper_bayesian_model.sh to replay warnings
some sample bayesian warnings are stored in file bayesian_warn.txt

script params:
  predict_date: the eventDate fo warning
  stock_index: the population field in the warning
  trend_file: the configure file for model

example: 
    to replay the warning like 
        [IBVC, 0411, 06/28/13, (Venezuela, -, -)], 0.50}']
    execute the following command
        bash wrapper_bayesian_model.sh 2013-06-28 IBVC ./trend_range_2013-06-13.txt


B> Duration analysis model
run wrapper_duration_model.sh to replay warnings
some sample duration warnings are stored in duration_warn.txt

script params:
    model_version: 
        if the warning sent before 2013-05-28, then model_version should be set as "v1",
        otherwise set it as "v2" 
    message_file:
        the input enriched price data to replay the warnings

example:
    bash wrapper_duration_model.sh v1 ./replay_duration_enrich.txt
