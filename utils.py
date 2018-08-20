#! usr/bin/python #coding=utf-8 
# encoding: utf-8
import datetime
import pandas as pd
import numpy as np
import os
import time

pd.set_option('display.width',500)
pd.set_option('display.max_columns', 15)
pool_path = 'C:\\Users\\ballma\\Desktop\\MyQuant\\stock_pool.csv'
result_path = 'C:\\Users\\ballma\\Desktop\\MyQuant\\result.xlsx'

# 计算date前n天为哪天
def getBeforeDay(n,date = ''):
    if date:
        today = datetime.date(*time.strptime(date,'%Y%m%d')[:3])
    else:
        today = datetime.date.today()
    days = datetime.timedelta(days=n)
    before_day = today - days
    before_day = before_day.strftime('%Y%m%d')
    return before_day

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

# 转为pyalgotrade格式的csv   (旧版)
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

# 获取分析对象列表
def get_stock_list():
    stock_list = []
    if os.path.exists(pool_path):
        with open(pool_path) as f:
            for line in f.readlines():
                stock_list.append(line.strip())
    return stock_list

# 转换代码格式，保证先去除300后再执行此函数
def get_new_code_name(code):
    code_str = ''
    code = str(code)
    code_length = len(code)
    if code_length == 1:
        code = '00000' + code
    elif code_length == 2:
        code = '0000' + code
    elif code_length == 3:
        code = '000' + code
    elif code_length == 4:
        code = '00' + code
    elif code_length == 5:
        code = '0' + code
    elif code_length == 6:
        pass
    else:
        return code_str

    if code.startswith('6'):
        code_str = str(code) + ".SH"
    elif code.startswith('0'):
        code_str = str(code) + ".SZ"
    else:
        print "core = %s  name error" %(code)
    return code_str

# 保存回测结果
def save_result_to_csv(result_list):
    col_list = ['code','mode','arg','date','sharpe','trade','suc_rate','draw_back','final']
    df1 = pd.DataFrame(result_list).ix[:,col_list]
    if os.path.exists(result_path):
        df0 = pd.read_excel(result_path,dtype = {'code':str,'draw_back':str,'sharpe':float,'suc_rate':float})
        # df = pd.concat([df0,df1],axis = 0,ignore_index = True).reset_index(drop = True)
        df = df1.append(df0).reset_index(drop = True)
        newdf = df.drop_duplicates()
        os.remove(result_path)
        df.to_excel(result_path,index = False)
    else:
        df1.to_excel(result_path,index = False)