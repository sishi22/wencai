#-*- coding: utf-8 -*-
# import os
from pyalgotrade import plotter
from pyalgotrade.barfeed import yahoofeed
from pyalgotrade.optimizer import local
from pyalgotrade.stratanalyzer import sharpe, drawdown, trades
import itertools
import get_stock
import MyStrategy
import os
import sys
import subprocess
import json
import pandas as pd
from datetime import datetime

pd.set_option('display.width',450)
csv_path = "C:\\Users\\ballma\\Desktop\\MyQuant\\csvs\\"
strategy_path = 'C:\\Users\\ballma\\Desktop\\MyQuant\\MyStrategy.py'
result_path = 'C:\\Users\\ballma\\Desktop\\MyQuant\\result.xlsx'
cmd = 'python %s' % strategy_path
core_list = get_stock.get_stock_list()

def run_Ma_Rsi(core, arg_set, ma_short, ma_long, rsi_in, rsi_out):
    result = {}
    feed = yahoofeed.Feed()
    trade = trades.Trades()
    sharpe_ratio = sharpe.SharpeRatio()
    draw_down = drawdown.DrawDown()
    feed.addBarsFromCSV(core, csv_path + "%s.csv" % core)
    myStrategy = MyStrategy.Ma_Rsi(feed, core,arg_set,ma_short, ma_long, rsi_in, rsi_out)
    myStrategy.attachAnalyzer(sharpe_ratio)
    myStrategy.attachAnalyzer(draw_down)
    myStrategy.attachAnalyzer(trade)
    plt = plotter.StrategyPlotter(myStrategy)
    myStrategy.run()
    result['core'] = core
    result['final_value'] = float("%.2f" % myStrategy.getBroker().getEquity())
    result['sharpe_ratio'] = float("%.2f" % sharpe_ratio.getSharpeRatio(0.04))
    result['max_draw_down'] = "%.2f %%" % (draw_down.getMaxDrawDown()*100)
    result['trade_count'] = trade.getCount()
    result['profit_count'] = trade.getProfitableCount()
    if plot_flag:
        plt.plot()
    # print "trade_commissions: %s" % trade.getCommissionsForAllTrades()
    return result

def run_Ma_Macd(core, arg_set, ma_short, ma_long, macd_fast, macd_slow):
    result = {}
    feed = yahoofeed.Feed()
    trade = trades.Trades()
    sharpe_ratio = sharpe.SharpeRatio()
    draw_down = drawdown.DrawDown()
    feed.addBarsFromCSV(core, csv_path + "%s.csv" % core)
    myStrategy = MyStrategy.Ma_Macd(feed, core,arg_set,ma_short, ma_long, macd_fast, macd_slow)
    myStrategy.attachAnalyzer(sharpe_ratio)
    myStrategy.attachAnalyzer(draw_down)
    myStrategy.attachAnalyzer(trade)
    plt = plotter.StrategyPlotter(myStrategy)
    myStrategy.run()
    result['core'] = core
    result['final'] = float("%.2f" % myStrategy.getBroker().getEquity())
    result['sharpe_ratio'] = float("%.2f" % sharpe_ratio.getSharpeRatio(0.04))
    result['draw_down'] = "%.2f %%" % (draw_down.getMaxDrawDown()*100)
    result['trade_count'] = trade.getCount()
    result['profit_count'] = trade.getProfitableCount()
    if plot_flag:
        plt.plot()
    # print "trade_commissions: %s" % trade.getCommissionsForAllTrades()
    return result

def run_Macd_Kdj(core, arg_set , kdj_fast, kdj_slow, macd_fast, macd_slow):
    result = {}
    feed = yahoofeed.Feed()
    trade = trades.Trades()
    sharpe_ratio = sharpe.SharpeRatio()
    draw_down = drawdown.DrawDown()
    feed.addBarsFromCSV(core, csv_path + "%s.csv" % core)
    myStrategy = MyStrategy.Macd_Kdj(feed, core,arg_set,kdj_fast,kdj_slow,macd_fast,macd_slow)
    myStrategy.attachAnalyzer(sharpe_ratio)
    myStrategy.attachAnalyzer(draw_down)
    myStrategy.attachAnalyzer(trade)
    plt = plotter.StrategyPlotter(myStrategy)
    myStrategy.run()
    result['core'] = core
    result['final'] = "%.2f" % myStrategy.getBroker().getEquity()
    result['sharpe'] = "%.2f" % sharpe_ratio.getSharpeRatio(0.04)
    result['draw_back'] = "%.2f %%" % (draw_down.getMaxDrawDown()*100)
    result['trade'] = str(trade.getProfitableCount()) + "/" + str(trade.getCount())
    result['suc_rate'] = "%.2f" % (float(trade.getProfitableCount())/float(trade.getCount()))
    result["arg"] = "%s,%s,%s,%s" % (kdj_fast,kdj_slow,macd_fast,macd_slow)
    result["mode"] = "Macd_Kdj"
    result["date"] = datetime.now().strftime('%Y-%m-%d')

    if plot_flag:
        plt.plot()
    return result

def argument_for_single():
    result_list = []
    if strategy_name == "Ma_Rsi":
        for i in core_list:
            if i == "002222":
                ret = run_Ma_Rsi(i,"fixed",5, 15, 8, 10)
            else:
                ret = run_Ma_Rsi(i,"fixed",5,10,15,30)
            result_list.append(ret)
    elif strategy_name == "Macd_Kdj":
        for i in core_list:
            if i == "002222":
                ret = run_Macd_Kdj(i,"fixed", 9,12,14,29)
            elif i == "601211":
                ret = run_Macd_Kdj(i,"fixed", 5,22,7,25)
            else:
                ret = run_Macd_Kdj(i,"fixed", 3,9,12,26)
            result_list.append(ret)
    elif strategy_name == "Ma_Macd":
        for i in core_list:
            if i == "002222":
                ret = run_Ma_Macd(i,"fixed",3,10,5,20)
            elif i == "601211":
                ret = run_Ma_Macd(i,"fixed",4,10,5,17)
            else:
                ret = run_Ma_Macd(i,"fixed",5,10,15,30)
            result_list.append(ret)
    if result_list:
        for result in result_list:
            print result
    else:
        print "None"

def argument_for_multiple():
    feed = yahoofeed.Feed()
    for core in core_list:
        feed.addBarsFromCSV(core, csv_path + "%s.csv" % core)
    if strategy_name == "Ma_Rsi":
        arg_set = "&"
        ma_short = range(3,10)
        ma_long = range(10, 25)
        rsi_in = range(3, 10)
        rsi_out = range(10, 25)
        generators = itertools.product(core_list, arg_set , ma_short, ma_long, rsi_in, rsi_out)
        local.run(MyStrategy.Ma_Rsi, feed, generators)
    elif strategy_name == "Macd_Kdj":
        arg_set = "&"
        kdj_fast = range(5, 10)
        kdj_slow = range(10, 25)
        macd_fast = range(3, 10)
        macd_slow = range(10,30)
        generators = itertools.product(core_list, arg_set , kdj_fast, kdj_slow, macd_fast, macd_slow)
        local.run(MyStrategy.Macd_Kdj, feed, generators)
    elif strategy_name == "Ma_Macd":
        arg_set = "&"
        ma_short = range(3,10)
        ma_long = range(10, 25)
        macd_fast = range(3, 10)
        macd_slow = range(10,30)
        generators = itertools.product(core_list, arg_set , ma_short, ma_long, macd_fast, macd_slow)
        local.run(MyStrategy.Ma_Macd, feed, generators)

def single_to_excel():
    # result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # result.wait()

    # data_list = []
    # for line in result.stdout.readlines():
    #     data = json.loads(line.strip().replace("'","\""))
    #     data_list.append(data)
    # df = pd.DataFrame(data_list)
    result2_path = "C:\\Users\\ballma\\Desktop\\MyQuant\\datas\\report.xlsx"
    if os.path.exists(result2_path):
        df0 = pd.read_excel(result2_path,dtype = {"code":str})
        print df0
    #     if df['date'][0] not in list(df0['date']):
    #         df = pd.concat([df0,df],axis = 0)
    #         df.to_excel(result_path)
    # else:
    #     df.to_excel(result_path)

# strategy_mode: single | multiple
# strategy_name: Ma_Rsi | Macd_Kdj | Ma_Macd
strategy_mode = "multiple"
strategy_name = "Ma_Macd"
plot_flag = False

# if __name__ == '__main__':
#     # print sys.argv[1]
#     if strategy_mode == "single":
#         argument_for_single()
#     elif strategy_mode == "multiple":
#         argument_for_multiple()





single_to_excel()













