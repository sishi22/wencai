#! usr/bin/python #coding=utf-8 
# encoding: utf-8
import os
import time
import Queue
import datetime
import traceback
from threading import Thread
import pandas as pd
import numpy as np
import tushare as ts
import talib
import utils

ts.set_token('d332be99f6485927b4a17d15925bc2c7e1b0675cb9a28d8b4c7f7730')
start_date = '20160401'
today_date = utils.getBeforeDay(0)
# today_date = datetime.datetime.now().strftime('%Y-%m-%d');
data_dir = 'C:\\Users\\ballma\\Desktop\\MyQuant\\datas'
history_dir = 'C:\\Users\\ballma\\Desktop\\MyQuant\\history'
csv_dir = 'C:\\Users\\ballma\\Desktop\\MyQuant\\csvs'

report_queue = Queue.Queue()
growth_queue = Queue.Queue()
pro = ts.pro_api()
year = [2015, 2016, 2017, 2018]
quarter = [1, 2, 3, 4]


def get_basics_data():
    # 获取基本面信息
    path = '%s\\stock_basics.xlsx' % data_dir
    if os.path.exists(path):
        os.remove(path)
    df_basic = ts.get_stock_basics()
    # code为df的索引，无code列
    for code in df_basic.index:
        if code.upper().startswith('300'):
            df_basic.drop([code], inplace=True)
    df_basic.to_excel(path)


def from_basic_chose():
    # 从基本面信息里去除一些
    get_basics_data()
    time.sleep(1)
    path = '%s\\stock_basics.xlsx' % data_dir
    if not os.path.exists(path):
        df_basic = pd.DataFrame()
        return df_basic
    df_basic = pd.read_excel(path, dtype={"code": str})
    for code in df_basic.index:
        # 去除净利为负的    or 去除上市不到两年的
        if df_basic['npr'][code] < 0 or df_basic['timeToMarket'][code] > 20160101:
            df_basic.drop([code], inplace=True)
    df_basic.to_excel(path, na_rep='NaN')
    return df_basic


def get_single_report_data(y, q):
    # 获取全市场季度业绩
    try:
        df = ts.get_report_data(y, q)
        ret = utils.del_needless_stock(df)
        if ret:
            print "del needless stock error~~~~~~~~~~~~~~~~"
        df = df.drop(['eps_yoy', 'epcf', 'bvps'], axis=1).drop_duplicates()
        df.columns = ['code', 'name', "esp(%s_%s)" % (y, q), "Roe(%s_%s)" % (y, q), "net_profits(%s_%s)" % (
            y, q), "profits_yoy(%s_%s)" % (y, q), "distrib(%s_%s)" % (y, q), "report_date(%s_%s)" % (y, q)]
        report_queue.put(df)
    except:
        print "get_single_report_data error at  %s %s " % (y, q)


def get_report_data_tofile():
    # 生成综合业绩文件
    report_path = '%s\\report.xlsx' % data_dir
    name_list = ["esp", "Roe", "net_profits", "profits_yoy", "distrib", "report_date"]
    col_list = ['code', 'name']
    th_pool = []
    result = []
    for y in year:
        for q in quarter:
            t = Thread(target=get_single_report_data, args=(y, q,))
            t.start()
            th_pool.append(t)
    for th in th_pool:
        th.join()
    while not report_queue.empty():
        result.append(report_queue.get())
    df_report = result[0]
    if len(result) > 1:
        for df in result[1:]:
            df_report = pd.merge(df_report, df, on=['code', 'name'], how='outer')

    for y in year:
        for q in quarter:
            for name in name_list:
                col = "%s(%s_%s)" % (name, y, q)
                if col in df_report.columns:
                    col_list.append("%s(%s_%s)" % (name, y, q))
    df_report = df_report.ix[:, col_list]
    df_report.index = df_report['code']
    df_report = df_report.drop(['code'], axis=1).drop_duplicates()
    if os.path.exists(report_path):
        os.remove(report_path)
    df_report.to_excel(report_path, na_rep='NaN')


def get_single_growth_data(y, q):
    # 获取全市场季度成长
    try:
        df = ts.get_growth_data(y, q)
        ret = utils.del_needless_stock(df)
        if ret:
            print "del needless stock error~~~~~~~~~~~~~~~~"
        df = df.drop(['targ', 'seg'], axis=1)
        df.columns = ['code', 'name', "mbrg(%s_%s)" % (y, q), "nprg(%s_%s)" % (
            y, q), "nav(%s_%s)" % (y, q), "epsg(%s_%s)" % (y, q)]
        growth_queue.put(df)
    except:
        print "get_single_growth_data error at  %s %s " % (y, q)


def get_growth_data_tofile():
    # 生成综合成长文件
    growth_path = '%s\\growth.xlsx' % data_dir
    name_list = ["mbrg", "nprg", "nav", "epsg"]
    col_list = ['code', 'name']
    th_pool = []
    result = []
    for y in year:
        for q in quarter:
            t = Thread(target=get_single_growth_data, args=(y, q,))
            t.start()
            th_pool.append(t)
    for th in th_pool:
        th.join()
    while not growth_queue.empty():
        result.append(growth_queue.get())
    df_growth = result[0]
    if len(result) > 1:
        for df in result[1:]:
            df_growth = pd.merge(df_growth, df, on=['code', 'name'], how='outer')

    for y in year:
        for q in quarter:
            for name in name_list:
                col = "%s(%s_%s)" % (name, y, q)
                if col in df_growth.columns:
                    col_list.append("%s(%s_%s)" % (name, y, q))
    df_growth = df_growth.ix[:, col_list]
    df_growth.index = df_growth['code']
    df_growth = df_growth.drop(['code'], axis=1).drop_duplicates()
    if os.path.exists(growth_path):
        os.remove(growth_path)
    df_growth.to_excel(growth_path, na_rep='NaN')


def get_history_data(code, start_time='', end_time=''):
    # 获取历史数据    时间格式:2016-10-09   (旧版)
    try:
        code = str(code)
        path = '%s\\%s.xlsx' % (history_dir, code)
        if os.path.exists(path):
            os.remove(path)
        df0 = pd.DataFrame()
        df1 = ts.get_hist_data(code, start=start_time, end=end_time)
        df1['date'] = df1.index
        df2 = ts.get_h_data(code, start=start_time, end=end_time, autype='hfq')
        df0['close_fq'] = df2['close']
        df0['date'] = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(df2.index.to_pydatetime())
        df3 = pd.merge(df1, df0, how='outer', on=['date'])
        df3 = df3.set_index('date').sort_index()
        df3['macd'], df3['signal'], hist = talib.MACDEXT(
            df3['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        df3['EMA7'] = talib.EMA(df3['close'], timeperiod=7)
        df3['EMA14'] = talib.EMA(df3['close'], timeperiod=14)
        df3.to_excel(path, columns=['open', 'high', 'close', 'low', 'p_change', 'ma5',
                                    'ma20', 'volume', 'macd', 'signal', 'EMA7', 'EMA14', 'close_fq'], na_rep='NaN')
        return 0
    except Exception, e:
        print "core = %s  and error = %s" % (code, traceback.print_exc())
        return 1


def get_data_to_csv(code):
    # 在新版本里不再需要此步骤  (旧版)
    ret = get_history_data(code, start_date, today_date)
    if not ret:
        utils.excel_to_csv(code)


def get_new_history_data(code, start_time='', end_time=''):
    # 获取历史数据(新版)       时间格式:20161009
    try:
        code_str = utils.get_new_code_name(code)
        if not code_str:
            return 1
        df1 = pro.daily(ts_code=code_str, start_date=start_time, end_date=end_time)
        df1 = df1.drop(['ts_code', 'pre_close', 'change', 'pct_change', 'amount'], axis=1)
        df2 = pro.adj_factor(ts_code=code_str, trade_date='')
        df2 = df2.drop(['ts_code'], axis=1)

        df3 = pd.merge(df1, df2, how='inner', on=['trade_date'])
        df3['adj_factor'] = (df3['close'] * df3['adj_factor']).apply(lambda x: format(x, '.2f'))
        df3['trade_date'] = df3['trade_date'].apply(lambda x: datetime.datetime(
            *time.strptime(x, '%Y%m%d')[:3]).strftime('%Y-%m-%d'))
        df3.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
        df3 = df3.set_index('Date').sort_index()
        df3.to_csv('%s\\%s.csv' % (csv_dir, code))
    except Exception, e:
        print "core = %s  and error = %s" % (code, traceback.print_exc())
        return 1


def refresh_in_pool_data():
    # 刷新分析对象数据
    if os.path.exists(pool_path):
        th_pool = []
        with open(pool_path) as f:
            try:
                while True:
                    code = next(f).strip()
                    t = Thread(target=get_new_history_data, args=(code, start_date, today_date,))
                    t.start()
                    th_pool.append(t)
                for th in th_pool:
                    th.join()
            except:
                pass


def get_roe_data():
    # 两年净资产收益率
    df_roe = pd.DataFrame()
    try:
        report_path = '%s\\report.xlsx' % data_dir
        df_report = pd.read_excel(report_path)
        df_roe = df_report[['code', 'Roe(2016_4)', 'Roe(2017_4)', 'Roe(2018_2)']]
        df_roe.columns = ['ts_code', 'roe(2016)', 'roe(2017)', 'roe(2018_2)']
        df_roe = df_roe.copy().drop_duplicates()
        df_roe['ts_code'] = df_roe['ts_code'].apply(lambda x: utils.get_new_code_name(x))
    except:
        print "error = %s" % traceback.print_exc()
    return df_roe


def get_area_data():
    # 获取行业地域信息
    df_area = pd.DataFrame()
    try:
        basic_path = '%s\\stock_basics.xlsx' % data_dir
        df_data = pd.read_excel(basic_path)
        df_area = df_data[['code', 'industry', 'area']]
        df_area.columns = ['ts_code', 'industry', 'area']
        df_area = df_area.copy().drop_duplicates()
        df_area['ts_code'] = df_area['ts_code'].apply(lambda x: utils.get_new_code_name(x))
    except:
        print "error = %s" % traceback.print_exc()
    return df_area


def get_rerights_data(date):
    # 复权因子的增长性
    df_basic = pro.daily_basic(ts_code='', trade_date=date, fields='ts_code,close,pe,pb')
    n = 0
    while df_basic.empty:
        n += 1
        date = utils.getBeforeDay(n, date)
        df_basic = pro.daily_basic(ts_code='', trade_date=date, fields='ts_code,close,pe,pb')
        df_re = pro.adj_factor(ts_code='', trade_date=date, fields='ts_code,adj_factor')
    else:
        df_re = pro.adj_factor(ts_code='', trade_date=date, fields='ts_code,adj_factor')
    print "get rerights_data on %s" % (date)

    s_data = pro.stock_basic(exchange_id='', is_hs='S', fields='ts_code,name,list_date')
    h_data = pro.stock_basic(exchange_id='', is_hs='H', fields='ts_code,name,list_date')
    df_data = pd.concat([h_data, s_data], axis=0)
    df_data = df_data.drop(df_data.index[(df_data['ts_code'].str.startswith(
        '300')) | (df_data['list_date'].str.startswith('2018'))])

    df1 = pd.merge(df_data, df_re, on=['ts_code'], how='inner')
    df2 = pd.merge(df1, df_basic, on=['ts_code'], how='inner')
    df = df2[['ts_code', 'name', 'list_date', 'adj_factor', 'pe', 'pb', 'close']]
    df = df.set_index('ts_code', drop=True)
    df = df.drop(df.index[df['pe'].isnull()])
    df = df.apply(pd.to_numeric, errors='ignore')
    # 平均每年的复权因子增长率
    df['growth'] = df.apply(lambda x: (x['adj_factor'] * 10000 /
                                       (int(date) - x['list_date'])), axis=1)
    # df = df.sort_values(['growth'],ascending=False)      # 排序
    return df


def chose_stock_from_data(date):
    # 选择
    df_reright = get_rerights_data(date)
    df_roe = get_roe_data()
    df_area = get_area_data()
    df_reright_roe = pd.merge(df_reright, df_roe, on=['ts_code'], how='inner')
    df_reright_roe_area = pd.merge(df_reright_roe, df_area, on=['ts_code'], how='inner')
    df_reright_roe_area = df_reright_roe_area.sort_values(['growth'], ascending=False)
    df_reright_roe_area = df_reright_roe_area[
        ['ts_code', 'name', 'industry', 'area', 'list_date', 'growth', 'roe(2016)', 'roe(2017)', 'pe', 'pb']]

    df = df_reright_roe_area.copy()
    df = df[(df['growth'] > 1) & (df['pe'] < 15) & (df['roe(2016)'] > 15) & (df['roe(2017)'] > 15)]
    return df


def run():
    # 运行测试
    # df = from_basic_chose()
    # get_growth_data_tofile()
    # get_report_data_tofile()
    # refresh_in_pool_data()
    df = chose_stock_from_data(today_date)
    # print df

# run()
