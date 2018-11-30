#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import os
import time
import sys
import io
import datetime
import traceback
from threading import Thread
import pandas as pd
import tushare as ts
import utils

sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8')
ts.set_token('d332be99f6485927b4a17d15925bc2c7e1b0675cb9a28d8b4c7f7730')
pro = ts.pro_api()
start_date = '20150918'   #3100点
today_date = utils.getBeforeDay(0)
# today_date = datetime.datetime.now().strftime('%Y-%m-%d');
data_dir = 'C:\\Users\\ballma\\Desktop\\MyQuant\\datas'
csv_dir = 'C:\\Users\\ballma\\Desktop\\MyQuant\\csvs'
pool_path = 'C:\\Users\\ballma\\Desktop\\MyQuant\\stock_pool.csv'


def getMovieDf(start_day,end_day=''):
    # 获取电影票房和评分数据，时间格式："20181023"
    df = pd.DataFrame()
    try:
        movie_month = utils.getMovieMonth(start_day,end_day)
        if not movie_month:
            return df
        for month_date in movie_month:
            df1 = pro.bo_monthly(date = month_date)
            df = pd.concat([df,df1],axis = 0)

        df = df[['list_date', 'month_amount','wom_index','name']]
        df1 = df[['month_amount']].groupby(df['name']).sum()
        df2 = df[['wom_index']].groupby(df['name']).max()
        df3 = df[['list_date']].groupby(df['name']).min()
        df = df3.join([df1,df2]).apply(pd.to_numeric, errors='ignore')
        df["name"] = df.index
        df = df.sort_values(by='wom_index', ascending = False).reset_index(drop = True)
        df = df[df["wom_index"] > 6.0]
    except Exception as e:
        print("getMovieDf error = %s" % e)
    return df


def get_new_history_data(code, start_time='', end_time=''):
    # 获取历史数据(新版)       时间格式:20161009
    try:
        code_str = utils.get_new_code_name(code)
        if not code_str:
            return 1
        df1 = ts.pro_bar(pro_api=pro, ts_code=code_str, adj='hfq', start_date=start_time, end_date=end_time)
        df2 = pro.daily(ts_code=code_str, start_date=start_time, end_date=end_time)
        hfq_df1 = df1.drop(['ts_code', 'trade_date', 'pre_close', 'change', 'pct_chg', 'amount'], axis=1).reset_index()
        hfq_df2 = df2.copy()
        hfq_df2['adj_factor'] = hfq_df2['open']
        hfq_df2 = hfq_df2[['trade_date','adj_factor']]
        hfq_df3 = pd.merge(hfq_df1, hfq_df2, how='inner', on=['trade_date'])
        hfq_df3['trade_date'] = hfq_df3['trade_date'].apply(lambda x: datetime.datetime(
            *time.strptime(x, '%Y%m%d')[:3]).strftime('%Y-%m-%d'))
        hfq_df3.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
        hfq_df3 = hfq_df3.set_index('Date').sort_index()
        hfq_df3.to_csv('%s\\%s_hfq.csv' % (csv_dir, code))

        qfq_df1 = df1.copy()
        qfq_df1['adj_factor'] = qfq_df1['open']
        qfq_df1 = qfq_df1[['adj_factor']].reset_index()
        qfq_df2 = df2.drop(['ts_code', 'pre_close', 'change', 'pct_chg', 'amount'], axis=1)
        qfq_df3 = pd.merge(qfq_df2, qfq_df1, how='inner', on=['trade_date'])
        qfq_df3['trade_date'] = qfq_df3['trade_date'].apply(lambda x: datetime.datetime(
            *time.strptime(x, '%Y%m%d')[:3]).strftime('%Y-%m-%d'))
        qfq_df3.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
        qfq_df3 = qfq_df3.set_index('Date').sort_index()
        qfq_df3.to_csv('%s\\%s.csv' % (csv_dir, code))
        print("save %s data successfully……" % code)
    except Exception as e:
        print("code = %s  and error = %s" % (code, traceback.print_exc()))
        return 1


def refresh_in_pool_data():
    # 刷新分析对象数据
    if os.path.exists(pool_path):
        th_pool = []
        with open(pool_path) as f:
            try:
                while True:
                    code = next(f).strip()
                    t = Thread(target=get_new_history_data, args=(code, start_date, today_date))
                    t.start()
                    th_pool.append(t)
                for th in th_pool:
                    th.join()
            except:
                pass


def get_sigle_roe(code,start_date,end_date):
    #获取个股roe和负债数据
    df_roe = pd.DataFrame()
    try:
        df = pro.query('fina_indicator', ts_code = code, start_date='20151231', end_date='20181231',fields='end_date,roe_yearly,debt_to_assets')
        debt_ratio = df['debt_to_assets'][0]
        df.index = df['end_date']
        df = df.drop(['end_date','debt_to_assets'], axis=1).drop_duplicates()
        df_index = df.index
        df_roe = df[(df_index == "20151231") | (df_index == "20161231") | (df_index == "20171231") | (df_index == df_index[0])].T
        col_list = list(df_roe.columns)
        if "20171231" not in col_list:
            df_roe["20171231"] = ""
        if "20161231" not in col_list:
            df_roe["20161231"] = ""
        if "20151231" not in col_list:
            df_roe["20151231"] = ""
        df_roe['ts_code'] = code
        df_roe['debt_ratio'] = debt_ratio
        df_roe.fillna(0)
    except:
        print("error = %s" % traceback.print_exc())
    return df_roe


def get_rerights_data(date):
    # 复权因子的增长性
    df_basic = pro.daily_basic(ts_code='', trade_date=date, fields='ts_code,close,pe_ttm,pb')
    n = 0
    while df_basic.empty:
        n += 1
        date = utils.getBeforeDay(n, date)
        df_basic = pro.daily_basic(ts_code='', trade_date=date, fields='ts_code,close,pe_ttm,pb')
        df_re = pro.adj_factor(ts_code='', trade_date=date, fields='ts_code,adj_factor')
    else:
        df_re = pro.adj_factor(ts_code='', trade_date=date, fields='ts_code,adj_factor')
    print("get rerights_data on %s" % (date))

    s_data = pro.stock_basic(exchange_id='', is_hs='S', fields='ts_code,name,area,industry,list_date,market')
    h_data = pro.stock_basic(exchange_id='', is_hs='H', fields='ts_code,name,area,industry,list_date,market')
    df_data = pd.concat([h_data, s_data], axis=0)
    df_data = df_data.drop(df_data.index[(df_data['ts_code'].str.startswith(
        '300')) | (df_data['list_date'].str.startswith('2018'))])

    df1 = pd.merge(df_data, df_re, on=['ts_code'], how='inner')
    df2 = pd.merge(df1, df_basic, on=['ts_code'], how='inner')
    df = df2[['ts_code', 'name', 'list_date', 'area', 'industry', 'market', 'adj_factor', 'pe_ttm', 'pb', 'close']]
    df = df.set_index('ts_code', drop=True)
    df = df.drop(df.index[df['pe_ttm'].isnull()])
    df = df.apply(pd.to_numeric, errors='ignore')
    # 平均每年的复权因子增长率
    df['growth'] = df.apply(lambda x: (x['adj_factor'] * 10000 /
                                       (int(date) - x['list_date'])), axis=1)
    return df


def get_roe_data_to_file():
    df = pd.DataFrame()
    try:
        roe_file = '%s\\%s.xlsx' % (data_dir, "financial_data")
        df_reright = get_rerights_data(today_date)
        code_list = list(df_reright.index)
        for code in code_list:
            single_roe = get_sigle_roe(code,start_date, today_date)
            time.sleep(1)
            try:
                df = pd.concat([df,single_roe], axis=0,sort=False)
            except:
                print("concat data of %s cause error" % code)
                continue
        df = df.set_index('ts_code')
        df.columns = ['roe(2018)', 'roe(2017)', 'roe(2016)', 'roe(2015)', 'debt_ratio']
        col_list = ['roe(2015)', 'roe(2016)', 'roe(2017)', 'roe(2018)', 'debt_ratio']
        df.to_excel(roe_file, columns = col_list, na_rep='NaN')
        print("save roe data sucessfully…………")
    except:
        print("error = %s " % traceback.print_exc())


def get_roe_data_from_file():
    roe_file = '%s\\%s.xlsx' % (data_dir, "financial_data")
    df = pd.read_excel(roe_file)
    return df


def chose_stock_from_data(date):
    # 选择
    df_reright = get_rerights_data(date)
    df_roe = get_roe_data_from_file()
    df_reright_roe = pd.merge(df_reright, df_roe, on=['ts_code'], how='inner')
    df_reright_roe = df_reright_roe.sort_values(['growth'], ascending=False).reset_index()
    df_reright_roe = df_reright_roe[
        ['ts_code', 'growth', 'roe(2015)', 'roe(2016)', 'roe(2017)', 'roe(2018)', 'debt_ratio', 'pe_ttm', 'pb', 'list_date',
         'area', 'name', 'industry', 'market']]
    df = df_reright_roe.copy()

    # print(df[df['name'] == "东方园林"])
    df = df[(df['growth'] > 1) & (df['pe_ttm'] < 20) & (df['roe(2016)'] > 20) & (df['roe(2017)'] > 20)]
    return df


def run():
    # 运行测试
    # print(",".join(utils.get_stock_list()))            # 用于DY系统
    # get_roe_data_to_file()          #季度手动更新一次
    # df = get_sigle_roe("002456.SZ",start_date, today_date)   #更新的调试

    df = chose_stock_from_data(today_date)
    # df = getMovieDf("20180901","20181125")
    # refresh_in_pool_data()
    # print(df)
run()