#! usr/bin/python #coding=utf-8 
# encoding: utf-8
from pyalgotrade import strategy
from pyalgotrade.technical import ma
from pyalgotrade.technical import rsi
from pyalgotrade.technical import stoch  # kdj
# from pyalgotrade.talibext import indicator
from pyalgotrade.technical import macd
from pyalgotrade import broker


class Ma_Rsi(strategy.BacktestingStrategy):

    def __init__(self, feed, instrument, arg_set, ma_short=5, ma_long=10, rsi_in=6, rsi_out=12):
        broker_commission = broker.backtesting.TradePercentage(0.0004)
        brokers = broker.backtesting.Broker(10000, feed, broker_commission)
        super(Ma_Rsi, self).__init__(feed, brokers)
        self.__instrument = instrument
        self.__feed = feed
        # self.setUseAdjustedValues(True)
        self.__position = None
        self.initState = False  # 开头的几个过滤掉
        self.code = feed.getKeys()[0]
        self.priceDS = feed[instrument].getPriceDataSeries()
        self.arg_set = arg_set
        self.ma_short = ma.SMA(self.priceDS, ma_short)
        self.ma_long = ma.SMA(self.priceDS, ma_long)
        self.rsi_in = rsi.RSI(self.priceDS, rsi_in)
        self.rsi_out = rsi.RSI(self.priceDS, rsi_out)

    def onEnterOk(self, position):
        if self.arg_set == "fixed":
            execInfo = position.getEntryOrder().getExecutionInfo()
            self.info("%s BUY at %.2f" % (self.code, execInfo.getPrice()))
        pass

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitOk(self, position):
        if self.arg_set == "fixed":
            execInfo = position.getExitOrder().getExecutionInfo()
            self.info("%s SELL at %.2f" % (self.code, execInfo.getPrice()))
        self.__position = None

    def onExitCanceled(self, position):
        self.__position.exitMarket()

    def checkMaCondition(self):
        if self.ma_short[-1] > self.ma_long[-1]:
            return 'UP'
        if self.ma_short[-1] < self.ma_long[-1]:
            return 'DOWN'
        return 'UN_FLUNE'

    def onBars(self, bars):
        if self.initState is False:
            if self.ma_long[-1] is None:
                return
            else:
                self.initState = True
        if self.__position == None:
            if self.checkMaCondition() == "UP" and self.rsi_in[-1] < 40:
                shares = int(self.getBroker().getCash() * 0.95 / bars[self.__instrument].getPrice() / 100) * 100
                self.__position = self.enterLong(self.__instrument, shares, True)
        elif not self.__position.exitActive():
            if self.checkMaCondition() == "DOWN" and self.rsi_out[-1] > 60:
                self.__position.exitMarket()


class Ma_Macd(strategy.BacktestingStrategy):

    def __init__(self, feed, instrument, arg_set, ma_short=5, ma_long=10, macd_fast=6, macd_slow=12):
        broker_commission = broker.backtesting.TradePercentage(0.0004)
        brokers = broker.backtesting.Broker(10000, feed, broker_commission)
        super(Ma_Macd, self).__init__(feed, brokers)
        self.__instrument = instrument
        self.__feed = feed
        # self.setUseAdjustedValues(True)
        self.__position = None
        self.initState = False  # 开头的几个过滤掉
        self.code = feed.getKeys()[0]
        self.priceDS = feed[instrument].getPriceDataSeries()
        self.arg_set = arg_set
        self.ma_short = ma.EMA(self.priceDS, ma_short)
        self.ma_long = ma.EMA(self.priceDS, ma_long)
        self.diff = macd.MACD(self.priceDS, fastEMA=macd_fast, slowEMA=macd_slow, signalEMA=9)
        self.dea = self.diff.getSignal()
        self.macd = self.diff.getHistogram()

    def onEnterOk(self, position):
        if self.arg_set == "fixed":
            execInfo = position.getEntryOrder().getExecutionInfo()
            self.info("%s BUY at %.2f" % (self.code, execInfo.getPrice()))
        pass

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitOk(self, position):
        if self.arg_set == "fixed":
            execInfo = position.getExitOrder().getExecutionInfo()
            self.info("%s SELL at %.2f" % (self.code, execInfo.getPrice()))
        self.__position = None

    def onExitCanceled(self, position):
        self.__position.exitMarket()

    def checkMacdCondition(self):
        if self.macd[-1] > 0 and self.macd[-2] < 0:  # macd由负转正
            return 'UP'
        elif self.diff[-1] > self.dea[-1] and self.diff[-2] < self.dea[-2]:  # diff上穿dea
            return 'UP'

        elif self.diff[-1] < self.dea[-1] and self.diff[-2] > self.dea[-2]:  # diff下穿dea
            return 'DOWN'
        elif self.macd[-1] < 0 and self.macd[-2] > 0:
            return 'DOWN'
        else:
            if self.diff[-1] > self.diff[-2] and self.dea[-1] > self.dea[-2]:
                return 'UP'
            if self.diff[-1] < self.diff[-2] and self.dea[-1] < self.dea[-2]:
                return 'DOWN'
        return 'UN_FLUNE'

    def checkMaCondition(self):
        if self.ma_short[-1] > self.ma_long[-1]:
            return 'UP'
        if self.ma_short[-1] < self.ma_long[-1]:
            return 'DOWN'
        return 'UN_FLUNE'

    def onBars(self, bars):
        if self.initState is False:
            if self.ma_long[-1] is None:
                return
            else:
                self.initState = True

        if self.__position == None:
            if self.checkMaCondition() == "UP" and self.checkMacdCondition() == "UP":
                shares = int(self.getBroker().getCash() * 0.95 / bars[self.__instrument].getPrice() / 100) * 100
                self.__position = self.enterLong(self.__instrument, shares, True)
        elif not self.__position.exitActive():
            if self.checkMaCondition() == "DOWN" and self.checkMacdCondition() == "DOWN":
                self.__position.exitMarket()


class Macd_Kdj(strategy.BacktestingStrategy):

    def __init__(self, feed, instrument, arg_set, kdj_fast=3, kdj_slow=9, macd_fast=12, macd_slow=26):
        broker_commission = broker.backtesting.TradePercentage(0.0004)
        brokers = broker.backtesting.Broker(10000, feed, broker_commission)
        super(Macd_Kdj, self).__init__(feed, brokers)
        self.__instrument = instrument
        self.__feed = feed
        # self.setUseAdjustedValues(True)
        self.__position = None
        self.initState = False
        self.code = feed.getKeys()[0]
        self.priceDS = feed[instrument].getPriceDataSeries()
        self.arg_set = arg_set
        self.kdj_k = stoch.StochasticOscillator(feed[instrument], period=kdj_fast, dSMAPeriod=kdj_slow)
        self.kdj_d = self.kdj_k.getD()
        self.diff = macd.MACD(self.priceDS, fastEMA=macd_fast, slowEMA=macd_slow, signalEMA=9)
        self.dea = self.diff.getSignal()
        self.macd = self.diff.getHistogram()

    def onEnterOk(self, position):
        if self.arg_set == "fixed":
            execInfo = position.getEntryOrder().getExecutionInfo()
            self.info("%s BUY at %.2f" % (self.code, execInfo.getPrice()))
        pass

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitOk(self, position):
        if self.arg_set == "fixed":
            execInfo = position.getExitOrder().getExecutionInfo()
            self.info("%s SELL at %.2f" % (self.code, execInfo.getPrice()))
        self.__position = None

    def onExitCanceled(self, position):
        self.__position.exitMarket()

    def checkMacdCondition(self):
        if self.macd[-1] > 0 and self.macd[-2] < 0:  # macd由负转正
            return 'UP'
        elif self.diff[-1] > self.dea[-1] and self.diff[-2] < self.dea[-2]:  # diff上穿dea
            return 'UP'

        elif self.diff[-1] < self.dea[-1] and self.diff[-2] > self.dea[-2]:  # diff下穿dea
            return 'DOWN'
        elif self.macd[-1] < 0 and self.macd[-2] > 0:
            return 'DOWN'
        else:
            if self.diff[-1] > self.diff[-2] and self.dea[-1] > self.dea[-2]:
                return 'UP'
            if self.diff[-1] < self.diff[-2] and self.dea[-1] < self.dea[-2]:
                return 'DOWN'
        return 'UN_FLUNE'

    def checkKdjCondition(self):
        if self.kdj_k[-1] < self.kdj_d[-1] and self.kdj_k[-2] > self.kdj_d[-2]:  # fast下穿slow
            return 'DOWN'
        elif self.kdj_k[-1] > self.kdj_d[-1] and self.kdj_k[-2] < self.kdj_d[-2]:  # fast上穿slow
            return 'UP'
        elif 25 > self.kdj_k[-1] > self.kdj_k[-2] and self.kdj_d[-1] > self.kdj_d[-2]:
            return 'UP'
        elif 75 < self.kdj_k[-1] < self.kdj_k[-2] and self.kdj_d[-1] < self.kdj_d[-2]:
            return 'DOWN'
        return 'UN_FLUNE'

    def onBars(self, bars):
        if self.initState is False:
            if self.kdj_d[-1] is None or self.diff[-1] is None:
                return
            else:
                self.initState = True

        if self.__position == None:
            if self.checkMacdCondition() == "UP" and self.checkKdjCondition() == "UP":
                shares = int(self.getBroker().getCash() * 0.95 / bars[self.__instrument].getPrice() / 100) * 100
                self.__position = self.enterLong(self.__instrument, shares, True)
        elif not self.__position.exitActive():
            if self.checkMacdCondition() == "DOWN" and self.checkKdjCondition() == "DOWN":
                self.__position.exitMarket()
