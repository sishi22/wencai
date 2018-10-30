#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import datetime
import pandas as pd
import numpy as np
import os
import time
# import calendar

pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 15)
pool_path = 'C:\\Users\\ballma\\Desktop\\MyQuant\\stock_pool.csv'
result_path = 'C:\\Users\\ballma\\Desktop\\MyQuant\\result.xlsx'


def getBeforeDay(n, date=''):
    # 计算date前n天为哪天
    if date:
        today = datetime.date(*time.strptime(date, '%Y%m%d')[:3])
    else:
        today = datetime.date.today()
    days = datetime.timedelta(days=n)
    before_day = today - days
    before_day = before_day.strftime('%Y%m%d')
    return before_day


def del_needless_stock(df):
    # 删除不需要的,当有code列时适用
    try:
        for index in df.index:
            if df['code'][index].upper().startswith('300'):
                # 去除创业板
                df.drop([index], inplace=True)
        return 0
    except Exception as e:
        return 1


def excel_to_csv(code):
    # 转为pyalgotrade格式的csv   (旧版)
    path = '%s\\%s.xlsx' % (history_dir, code)
    df_basic = pd.read_excel(path, dtype={"code": str})
    for index in df_basic.index:
        if np.isnan(df_basic['close'][index]) or np.isnan(df_basic['close_fq'][index]):
            df_basic.drop([index], inplace=True)
    df = df_basic[['date', 'open', 'high', 'low', 'close', 'volume', 'close_fq']]
    df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
    df.to_csv('%s\\%s.csv' % (csv_dir, code), index=False)


def add_stock_pool(code_list):
    # 向stock_pool的csv文件中添加
    new_df = pd.DataFrame(code_list, columns={'code'})
    if os.path.exists(pool_path):
        old_df = pd.read_csv(pool_path, dtype=str)
        sum_df = pd.concat([old_df, new_df], axis=0)
        new_df = sum_df.drop_duplicates().reset_index(drop=True)
    new_df.to_csv(pool_path, index=False)


def get_stock_list():
    # 获取分析对象列表
    stock_list = []
    if os.path.exists(pool_path):
        with open(pool_path) as f:
            for line in f.readlines():
                stock_list.append(line.strip())
    return stock_list


def get_new_code_name(code):
    # 转换代码格式，保证先去除300后再执行此函数
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
        print("core = %s  name error" % (code))
    return code_str


def save_result_to_csv(result_list):
    # 保存回测结果
    col_list = ['code', 'mode', 'arg', 'date', 'sharpe', 'trade', 'suc_rate', 'draw_back', 'final']
    df1 = pd.DataFrame(result_list).ix[:, col_list]
    if os.path.exists(result_path):
        df0 = pd.read_excel(result_path, dtype={'code': str, 'draw_back': str, 'sharpe': float, 'suc_rate': float})
        # df = pd.concat([df0,df1],axis = 0,ignore_index = True).reset_index(drop = True)
        df = df1.append(df0).reset_index(drop=True)
        newdf = df.drop_duplicates()
        os.remove(result_path)
        df.to_excel(result_path, index=False)
    else:
        df1.to_excel(result_path, index=False)


def getMovieMonth(start_day,end_day=''):
    month_list = []
    try:
        start_date = datetime.date(*time.strptime(start_day, '%Y%m%d')[:3])
        today_date = datetime.date.today()
        if end_day:
            end_date = datetime.date(*time.strptime(end_day, '%Y%m%d')[:3])
            if end_date > today_date:
                end_date = today_date
        else:
            end_date = today_date
        if start_date > today_date:
            start_date = today_date
        if start_date > end_date:
            new_date = start_date
            start_date = end_date
            end_date = new_date

        first_day = datetime.date(start_date.year, start_date.month, 1)
        last_day = datetime.date(end_date.year, end_date.month, 1)
        while first_day != last_day:
            # 取前一个月第一天
            month_list.append(last_day.strftime('%Y%m%d'))
            pre_month = last_day - datetime.timedelta(days = 1)
            last_day = datetime.date(pre_month.year, pre_month.month, 1)
            # 若取后一个月第一天，需用到 calendar 模块
            # month_list.append(first_day.strftime('%Y%m%d'))
            # month_days_num = calendar.monthrange(first_day.year, first_day.month)[1]
            # first_day = first_day + datetime.timedelta(days = month_days_num)
        else:
            month_list.append(first_day.strftime('%Y%m%d'))
    except Exception as e:
        pass
    return month_list