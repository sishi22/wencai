#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import time
import datetime
import math
import matplotlib.pyplot as plt
from matplotlib import style
from sklearn import model_selection
from sklearn import preprocessing
from sklearn import svm
from sklearn.linear_model import LinearRegression

pool_path = "C:\\Users\\ballma\\Desktop\\MyQuant\\csvs\\002222.csv"
df = pd.read_csv(pool_path, dtype=str)
df.index = df['Date']
df = df.drop(['Adj Close','Date'],1)
df = pd.DataFrame(df,dtype = np.float)
df = df[['Close', 'Volume']]

forecast_out = int(math.ceil(0.01 * len(df)))
df['label'] = df['Close'].shift(-forecast_out)

X = np.array(df.drop(['label'], 1))
X = preprocessing.scale(X)
X_lately = X[-forecast_out:]

X = X[:-forecast_out]
df.dropna(inplace=True)
y = np.array(df['label'])
X_train, X_test, y_train ,y_test = model_selection.train_test_split(X,y,test_size=0.1)
clf = LinearRegression()
clf.fit(X_train,y_train)
accuracy = clf.score(X_test,y_test)
forecast_set = clf.predict(X_lately)

df['Forecast']=np.nan
last_date = df.iloc[-1].name
last_time_stamp = time.mktime(time.strptime(last_date,'%Y-%m-%d'))
one_day = 86400
next_stamp = last_time_stamp + one_day

for i in forecast_set:
    next_date = time.strftime("%Y-%m-%d",time.localtime(next_stamp))
    next_stamp += 86400
    df.loc[next_date] = [np.nan for _ in range(len(df.columns)-1)]+[i]

df['Date'] = pd.to_datetime(df.index)
style.use('ggplot')
df.plot(kind = 'line',x = 'Date',y = ['Close','Forecast'])
plt.show()