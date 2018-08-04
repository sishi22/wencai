#! usr/bin/python #coding=utf-8 
# encoding: utf-8
import pandas as pd
import numpy as np
import tushare as ts
import talib
import os
from datetime import datetime
import time
import traceback
from threading import Thread

pd.set_option('display.width',450)

start_date = '2016-04-01'
end_date = '2018-04-08'
today_date = datetime.now().strftime('%Y-%m-%d');
data_dir = 'C:\\Users\\ballma\\Desktop\\MyQuant\\datas'
history_dir = 'C:\\Users\\ballma\\Desktop\\MyQuant\\history'
csv_dir = 'C:\\Users\\ballma\\Desktop\\MyQuant\\csvs'
pool_path = 'C:\\Users\\ballma\\Desktop\\MyQuant\\stock_pool.csv'
test_path = 'C:\\Users\\ballma\\Desktop\\MyQuant\\test'

year = [2015,2016,2017,2018]
quarter = [1,2,3,4]

#删除不需要的,当有code列时适用
def del_needless_stock(df):
    try:
        for index in df.index:
            if df['code'][index].upper().startswith('300'):
                # 去除创业板
                df.drop([index],inplace = True)
        return 0
    except Exception,e:
        return 1

# 获取基本面信息
def get_basics_data():
    path = '%s\\stock_basics.xlsx' % data_dir
    if os.path.exists(path):
        os.remove(path)
    df_basic = ts.get_stock_basics()
    # code为df的索引，无code列
    for code in df_basic.index:
        if code.upper().startswith('300'):
            df_basic.drop([code],inplace = True)
    df_basic.to_excel(path)

# 从基本面信息里去除一些
def from_basic_chose():
    get_basics_data()
    time.sleep(1)
    path = '%s\\stock_basics.xlsx' % data_dir
    if not os.path.exists(path):
        df_basic = pd.DataFrame()
        return df_basic
    df_basic = pd.read_excel(path,dtype = {"code":str})
    for code in df_basic.index:
        # 去除净利为负的    or 去除上市不到两年的
        if df_basic['npr'][code] < 0 or df_basic['timeToMarket'][code] > 20160101:
            df_basic.drop([code],inplace = True)
    return df_basic

# 获取全股季度业绩
def get_single_report_data(y,q):
    try:
        path = '%s\\report\\%s_%s.xlsx' % (data_dir,y,q)
        if os.path.exists(path):
            os.remove(path)
        df = ts.get_report_data(y,q)
        ret = del_needless_stock(df)
        if ret:
            print "del needless stock error~~~~~~~~~~~~~~~~"
            return 1
        df = df.drop(['eps_yoy','epcf','bvps'],axis = 1)
        df.columns = ['code','name',"esp(%s_%s)"%(y,q),"Roe(%s_%s)"%(y,q),"net_profits(%s_%s)"%(y,q),"profits_yoy(%s_%s)"%(y,q),"distrib(%s_%s)"%(y,q),"report_date(%s_%s)"%(y,q)]
        df.index = df['code']
        df = df.drop(['code'],axis = 1)
        df.to_excel(path,na_rep='NaN')
    except:
        print "get_single_report_data error at  %s %s " % (y,q)

# 批量获取全股季度业绩
def get_batch_report_data():
    th_pool = []
    for y in year:
        for q in quarter:
            t = Thread(target=get_single_report_data,args=(y,q,))
            t.start()
            th_pool.append(t)
    for th in th_pool:
        th.join()

# 生成综合业绩文件
def report_data_tofile():
    get_batch_report_data()
    report_df = pd.DataFrame()
    path1 = '%s\\report.xlsx' % data_dir
    if os.path.exists(path1):
        os.remove(path1)
    for y in year:
        for q in quarter:
            path = '%s\\report\\%s_%s.xlsx' % (data_dir,y,q)
            if os.path.exists(path):
                df = pd.read_excel(path,dtype = {"code":str})
                if y == year[0] and q == quarter[0]:
                    report_df = df
                else:
                    df = df.drop(['name'],axis = 1)
                    report_df = pd.merge(report_df,df,on = ['code'],how = 'outer')
    report_df.index = report_df['code']
    report_df = report_df.drop(['code'],axis = 1).drop_duplicates()
    report_df.to_excel(path1,na_rep='NaN')

# 获取全市场季度成长
def get_single_growth_data(y,q):
    try:
        path = '%s\\growth\\%s_%s.xlsx' % (data_dir,y,q)
        if os.path.exists(path):
            os.remove(path)
        df = ts.get_growth_data(y,q)
        ret = del_needless_stock(df)
        if ret:
            print "del needless stock error~~~~~~~~~~~~~~~~"
        df = df.drop(['targ','seg'],axis = 1)
        df.columns = ['code','name',"mbrg(%s_%s)"%(y,q),"nprg(%s_%s)"%(y,q),"nav(%s_%s)"%(y,q),"epsg(%s_%s)"%(y,q)]
        df.index = df['code']
        df = df.drop(['code'],axis = 1)
        df.to_excel(path,na_rep='NaN')
    except:
        print "get_single_growth_data error at  %s %s " % (y,q)

# 批量获取全市场季度成长
def get_batch_growth_data():
    th_pool = []
    for y in year:
        for q in quarter:
            t = Thread(target=get_single_growth_data,args=(y,q,))
            t.start()
            th_pool.append(t)
    for th in th_pool:
        th.join()

# 生成综合成长文件
def growth_data_tofile():
    get_batch_growth_data()
    growth_df = pd.DataFrame()
    path1 = '%s\\growth.xlsx' % data_dir
    if os.path.exists(path1):
        os.remove(path1)
    for y in year:
        for q in quarter:
            path = '%s\\growth\\%s_%s.xlsx' % (data_dir,y,q)
            if os.path.exists(path):
                df = pd.read_excel(path,dtype = {"code":str})
                if y == year[0] and q == quarter[0]:
                    growth_df = df
                else:
                    df = df.drop(['name'],axis = 1)
                    growth_df = pd.merge(growth_df,df,on = ['code'],how = 'outer')
    growth_df.index = growth_df['code']
    growth_df = growth_df.drop(['code'],axis = 1).drop_duplicates()
    growth_df.to_excel(path1,na_rep='NaN')

# 转为pyalgotrade格式的csv
def excel_to_csv(code):
    path = '%s\\%s.xlsx' % (history_dir,code)
    df_basic = pd.read_excel(path,dtype = {"code":str})
    for index in df_basic.index:
        if np.isnan(df_basic['close'][index]) or np.isnan(df_basic['close_fq'][index]):
            df_basic.drop([index],inplace = True)
    df = df_basic[['date','open','high','low','close','volume','close_fq']]
    df.columns = ['Date','Open','High','Low','Close','Volume','Adj Close']
    df.to_csv('%s\\%s.csv' % (csv_dir,code),index = False)

# 向stock_pool的csv文件中添加
def add_stock_pool(code_list):
    new_df = pd.DataFrame(code_list,columns={'code'})
    if os.path.exists(pool_path):
        old_df = pd.read_csv(pool_path,dtype=str)
        sum_df = pd.concat([old_df,new_df],axis = 0)
        new_df = sum_df.drop_duplicates().reset_index(drop = True)
    new_df.to_csv(pool_path,index = False)

# 获取历史数据
def get_history_data(code,start_time='',end_time=''):
    try:
        code = str(code)
        path = '%s\\%s.xlsx' % (history_dir,code)
        if os.path.exists(path):
            os.remove(path)
        df0 = pd.DataFrame()
        df1 = ts.get_hist_data(code,start = start_time,end = end_time)
        df1['date'] = df1.index
        df2 = ts.get_h_data(code,start = start_time,end = end_time ,autype='hfq')
        df0['close_fq'] = df2['close']
        df0['date'] = np.vectorize(lambda s:s.strftime('%Y-%m-%d'))(df2.index.to_pydatetime())
        df3 = pd.merge(df1,df0,how = 'outer',on = ['date'])
        df3 = df3.set_index('date').sort_index()
        df3['macd'], df3['signal'], hist = talib.MACDEXT(df3['close'],fastperiod=12, slowperiod=26, signalperiod=9)
        df3['EMA7'] = talib.EMA(df3['close'], timeperiod=7)
        df3['EMA14'] = talib.EMA(df3['close'], timeperiod=14)
        df3.to_excel(path,columns = ['open','high','close','low','p_change','ma5','ma20','volume','macd','signal','EMA7','EMA14','close_fq'],na_rep='NaN')
        return 0
    except Exception,e:
        print "core = %s  and error = %s" %(code,traceback.print_exc())
        return 1

def get_data_to_csv(code):
    ret = get_history_data(code,start_date,today_date)
    if not ret:
        excel_to_csv(code)

# 刷新分析对象数据
def refresh_in_pool_data():
    if os.path.exists(pool_path):
        with open(pool_path) as f:
            try:
                while True:
                    code = next(f).strip()
                    t = Thread(target=get_data_to_csv,args=(code,))
                    t.start()
            except:
                pass

# 获取分析对象列表
def get_stock_list():
    stock_list = []
    if os.path.exists(pool_path):
        with open(pool_path) as f:
            for line in f.readlines():
                stock_list.append(line.strip())
    return stock_list

# df = from_basic_chose()
# growth_data_tofile()
# report_data_tofile()

# 运行测试
def run():
    refresh_in_pool_data()

# run()