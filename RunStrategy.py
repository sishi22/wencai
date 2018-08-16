#-*- coding: utf-8 -*-
# import os
from pyalgotrade import plotter
from pyalgotrade.barfeed import yahoofeed
from pyalgotrade.optimizer import local
from pyalgotrade.stratanalyzer import sharpe, drawdown, trades
import itertools
import utils
import MyStrategy
import sys
from datetime import datetime

csv_path = "C:\\Users\\ballma\\Desktop\\MyQuant\\csvs\\"
code_list = utils.get_stock_list()

def run_Ma_Rsi(code, arg_set, ma_short, ma_long, rsi_in, rsi_out):
    result = {}
    feed = yahoofeed.Feed()
    trade = trades.Trades()
    sharpe_ratio = sharpe.SharpeRatio()
    draw_down = drawdown.DrawDown()
    feed.addBarsFromCSV(code, csv_path + "%s.csv" % code)
    myStrategy = MyStrategy.Ma_Rsi(feed, code,arg_set,ma_short, ma_long, rsi_in, rsi_out)
    myStrategy.attachAnalyzer(sharpe_ratio)
    myStrategy.attachAnalyzer(draw_down)
    myStrategy.attachAnalyzer(trade)
    plt = plotter.StrategyPlotter(myStrategy)
    myStrategy.run()
    result['code'] = code
    result['final'] = int(myStrategy.getBroker().getEquity())
    result['sharpe'] = "%.2f" % sharpe_ratio.getSharpeRatio(0.04)
    result['draw_back'] = "%.2f %%" % (draw_down.getMaxDrawDown()*100)
    result['trade'] = str(trade.getProfitableCount()) + "/" + str(trade.getCount())
    result['suc_rate'] = "%.2f" % (float(trade.getProfitableCount())/float(trade.getCount()))
    result["arg"] = "%s,%s,%s,%s" % (ma_short,ma_long,rsi_in,rsi_out)
    result["mode"] = "Ma_Rsi"
    result["date"] = datetime.now().strftime('%Y-%m-%d')
    if plot_flag:
        plt.plot()
    # print "trade_commissions: %s" % trade.getCommissionsForAllTrades()
    return result

def run_Ma_Macd(code, arg_set, ma_short, ma_long, macd_fast, macd_slow):
    result = {}
    feed = yahoofeed.Feed()
    trade = trades.Trades()
    sharpe_ratio = sharpe.SharpeRatio()
    draw_down = drawdown.DrawDown()
    feed.addBarsFromCSV(code, csv_path + "%s.csv" % code)
    myStrategy = MyStrategy.Ma_Macd(feed, code,arg_set,ma_short, ma_long, macd_fast, macd_slow)
    myStrategy.attachAnalyzer(sharpe_ratio)
    myStrategy.attachAnalyzer(draw_down)
    myStrategy.attachAnalyzer(trade)
    plt = plotter.StrategyPlotter(myStrategy)
    myStrategy.run()
    result['code'] = code
    result['final'] = int(myStrategy.getBroker().getEquity())
    result['sharpe'] = "%.2f" % sharpe_ratio.getSharpeRatio(0.04)
    result['draw_back'] = "%.2f %%" % (draw_down.getMaxDrawDown()*100)
    result['trade'] = str(trade.getProfitableCount()) + "/" + str(trade.getCount())
    result['suc_rate'] = "%.2f" % (float(trade.getProfitableCount())/float(trade.getCount()))
    result["arg"] = "%s,%s,%s,%s" % (ma_short,ma_long,macd_fast,macd_slow)
    result["mode"] = "Ma_Macd"
    result["date"] = datetime.now().strftime('%Y-%m-%d')
    if plot_flag:
        plt.plot()
    return result

def run_Macd_Kdj(code, arg_set , kdj_fast, kdj_slow, macd_fast, macd_slow):
    result = {}
    feed = yahoofeed.Feed()
    trade = trades.Trades()
    sharpe_ratio = sharpe.SharpeRatio()
    draw_down = drawdown.DrawDown()
    feed.addBarsFromCSV(code, csv_path + "%s.csv" % code)
    myStrategy = MyStrategy.Macd_Kdj(feed, code,arg_set,kdj_fast,kdj_slow,macd_fast,macd_slow)
    myStrategy.attachAnalyzer(sharpe_ratio)
    myStrategy.attachAnalyzer(draw_down)
    myStrategy.attachAnalyzer(trade)
    plt = plotter.StrategyPlotter(myStrategy)
    myStrategy.run()
    result['code'] = code
    result['final'] = int(myStrategy.getBroker().getEquity())
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
        for i in code_list:
            if i == "002222":
                ret = run_Ma_Rsi(i,"fixed",5, 15, 8, 10)
            else:
                ret = run_Ma_Rsi(i,"fixed",5,10,15,30)
            result_list.append(ret)
    elif strategy_name == "Macd_Kdj":
        for i in code_list:
            if i == "002222":
                ret = run_Macd_Kdj(i,"fixed", 9,12,14,29)
            elif i == "601211":
                ret = run_Macd_Kdj(i,"fixed", 5,22,7,25)
            else:
                ret = run_Macd_Kdj(i,"fixed", 3,9,12,26)
            result_list.append(ret)
    elif strategy_name == "Ma_Macd":
        for i in code_list:
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
        # utils.save_result_to_csv(result_list)
    else:
        print "None"

def argument_for_multiple():
    feed = yahoofeed.Feed()
    for code in code_list:
        feed.addBarsFromCSV(code, csv_path + "%s.csv" % code)
    if strategy_name == "Ma_Rsi":
        arg_set = "&"
        ma_short = range(3,10)
        ma_long = range(10, 25)
        rsi_in = range(3, 10)
        rsi_out = range(10, 25)
        generators = itertools.product(code_list, arg_set , ma_short, ma_long, rsi_in, rsi_out)
        local.run(MyStrategy.Ma_Rsi, feed, generators)
    elif strategy_name == "Macd_Kdj":
        arg_set = "&"
        kdj_fast = range(3, 10)
        kdj_slow = range(10, 25)
        macd_fast = range(3, 15)
        macd_slow = range(15,30)
        generators = itertools.product(code_list, arg_set , kdj_fast, kdj_slow, macd_fast, macd_slow)
        local.run(MyStrategy.Macd_Kdj, feed, generators)
    elif strategy_name == "Ma_Macd":
        arg_set = "&"
        ma_short = range(3,10)
        ma_long = range(10, 25)
        macd_fast = range(3, 10)
        macd_slow = range(10,30)
        generators = itertools.product(code_list, arg_set , ma_short, ma_long, macd_fast, macd_slow)
        local.run(MyStrategy.Ma_Macd, feed, generators)

# strategy_mode: single | multiple
# strategy_name: Ma_Rsi | Macd_Kdj | Ma_Macd
strategy_mode = "single"
strategy_name = "Macd_Kdj"
plot_flag = False

if __name__ == '__main__':
    if strategy_mode == "single":
        argument_for_single()
    elif strategy_mode == "multiple":
        argument_for_multiple()
