import requests
import pandas as pd


class DataStore:
    store = {}

    @classmethod
    def loadKlineHistory(cls, symbol="dotusdt", timeframe="1m", klines=1000, endTime=None, startTime=None):
        sSig = "" if startTime is None else str(startTime)
        eSig = "" if endTime is None else str(endTime)
        sig = symbol + "@" + timeframe + "-" + str(klines) + "|" + sSig + "-" + eSig
        if sig in cls.store:
            return cls.store[sig]
        uri = "https://api.binance.com/api/v3/klines?symbol={}&interval={}&limit={}"
        uri = uri.format(symbol.upper(), timeframe, klines)
        if startTime:
            uri += "&startTime={}".format(startTime)
        if endTime:
            uri += "&endTime={}".format(endTime)
        r = requests.get(uri)
        data = r.json()
        cls.store[sig] = data
        return data

    @classmethod
    def loadKlineClosesHistory(cls, symbol="dotusdt", timeframe="1m", klines=1000, endTime=None, startTime=None):
        cols = ["OpenTime",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "CloseTime",
                "Quote-asset-volume",
                "Number-of-trades",
                "Taker-buy-base-asset-volume",
                "Taker-buy-quote-asset-volumze",
                "Ignore"]
        hist = cls.loadKlineHistory(symbol, timeframe, klines, endTime, startTime)
        df = pd.DataFrame(hist, columns=cols)
        df["Close"] = df["Close"].apply(lambda x: float(x))
        data = df["Close"].to_numpy()
        return data


    @classmethod
    def loadKlineFrame(cls, symbol="dotusdt", timeframe="1m", klines=1000, endTime=None, startTime=None):
        cols = ["OpenTime",
                "Open",
                "High", 
                "Low",
                "Close",
                "Volume",
                "CloseTime",
                "Quote-asset-volume",
                "Number-of-trades",
                "Taker-buy-base-asset-volume",
                "Taker-buy-quote-asset-volumze",
                "Ignore"]
        hist = cls.loadKlineHistory(symbol, timeframe, klines, endTime, startTime)
        df = pd.DataFrame(hist, columns=cols)
        for c in cols:
            df[c] = df[c].apply(lambda x: float(x))
        df["OpenTime"] = df["OpenTime"].apply(lambda x: int(x))
        df["CloseTime"] = df["CloseTime"].apply(lambda x: int(x))
        df["Number-of-trades"] = df["Number-of-trades"].apply(lambda x: int(x))
        return df

    @classmethod
    def loadKlineFrameLong(cls, symbol="dotusdt", timeframe="1m", klines=1000):
        n = klines // 1000
        remnant = klines % 1000
        dfList = []
        for i in range(n+1):
            endTime = None
            if len(dfList) > 0:
                endTime = int(dfList[-1].iloc[0].OpenTime) - 1
            if i != n:
                dfList.append(cls.loadKlineFrame(symbol, timeframe, 1000, endTime=endTime))
            elif i == n and remnant != 0:
                dfList.append(cls.loadKlineFrame(symbol, timeframe, remnant, endTime=endTime))
        dfList.reverse()
        return pd.concat(dfList)
            