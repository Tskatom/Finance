import statsmodels.tsa.api as tsa
import numpy as np

#data=<time* variables>
data=np.array([[1,2,3,20,30],[1,3,4,20,30],[1,4,5,100,200],[5,8,10,10,10]]).T
data = np.array([[1,2,3,4,5,6],[1,2,3,4,5,6]]).T
order = 1
#np.

#Training
var_model = tsa.VAR(data)
var_model_fit = var_model.fit(maxlags=order)
#var_model = var_model_fit.model


#Training results
intercept =var_model_fit.intercept
params=var_model_fit.params

#print intercept
#print params

#Prediction
#print var_model.y[-2:]
print type(var_model.y)
out_of_sample_prediction=var_model_fit.forecast([[7,7]],3)
#out_of_sample_prediction = var_model.predict(params, 5,8)
print out_of_sample_prediction