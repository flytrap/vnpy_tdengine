#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Created by flytrap
# Created time: 2022/03/26
from datetime import datetime
from typing import List

import pytz
import requests
from loguru import logger
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import BarOverview, BaseDatabase
from vnpy.trader.object import BarData, TickData
from vnpy.trader.setting import SETTINGS
from vnpy.trader.utility import extract_vt_symbol


class TdEngineDatabase(BaseDatabase):
    def __init__(self) -> None:
        """初始化，数据库配置"""
        self.database: str = SETTINGS["database.database"]
        self.user: str = SETTINGS["database.user"]
        if not self.user:
            self.user = "root"
        self.password: str = SETTINGS["database.password"]
        if not self.password:
            self.password = "taosdata"
        self.host: str = SETTINGS["database.host"]
        self.port: int = SETTINGS["database.port"]
        if not self.port:
            self.port = 6041
        self.url = f"http://{self.user}:{self.password}@{self.host}:{self.port}/rest/sql/{self.database}"
        self.bar_super_table = "bar_data_table"
        self.tick_super_table = "tick_data_table"
        self.bar_fields = [
            "time",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "turnover",
            "open_interest",
        ]
        self.tick_fields = [
            "time",
            "name",
            "volume",
            "turnover",
            "open_interest",
            "last_price",
            "last_volume",
            "limit_up",
            "limit_down",
            "open_price",
            "high_price",
            "low_price",
            "pre_close",
            "bid_price_1",
            "bid_price_2",
            "bid_price_3",
            "bid_price_4",
            "bid_price_5",
            "ask_price_1",
            "ask_price_2",
            "ask_price_3",
            "ask_price_4",
            "ask_price_5",
            "bid_volume_1",
            "bid_volume_2",
            "bid_volume_3",
            "bid_volume_4",
            "bid_volume_5",
            "ask_volume_1",
            "ask_volume_2",
            "ask_volume_3",
            "ask_volume_4",
            "ask_volume_5",
            "localtime",
        ]
        self.init_table()

    def execute_sql(self, sql: str):
        """执行sql"""
        resp = requests.post(self.url, data=sql.encode("utf8"))
        data = resp.json()
        if resp.status_code == 200:
            if sql.strip().startswith("select"):
                return data.get("data", [])
            return data.get("rows", 0)
        logger.warning(f"TDENGINE: {data.get('code')} {data.get('desc')}")
        return []

    def init_table(self):
        """初始化超级表"""
        self.execute_sql(
            f"create database if not exists {self.database} keep 36500 update 2"
        )
        bar_sql = f"create stable if not exists {self.bar_super_table} (time TIMESTAMP, {','.join([f'{field} DOUBLE' for field in self.bar_fields[1:]])}) tags (intervals BINARY(10), vt_symbol BINARY(20), total int)"
        self.execute_sql(bar_sql)
        tick_sql = f"create stable if not exists {self.tick_super_table} (time TIMESTAMP, {','.join([f'{field} DOUBLE' for field in self.tick_fields[1:-1]])}, localtime TIMESTAMP) tags (vt_symbol BINARY(20))"
        self.execute_sql(tick_sql)

    def save_bar_data(self, bars: List[BarData]) -> bool:
        if len(bars) == 0:
            return False
        bar: BarData = bars[0]
        vt_symbol: str = bar.vt_symbol
        interval: Interval = bar.interval

        values: List[str] = []
        for bar in bars:
            v = f"""({int(bar.datetime.timestamp()*1000)},{bar.open_price},{bar.high_price},{bar.low_price},{bar.close_price},{bar.volume},{bar.turnover},{bar.open_interest})"""
            values.append(v)
        results = self.execute_sql(
            f"select total from {self.bar_super_table} where vt_symbol='{vt_symbol}' and intervals='{interval.value}'"
        )
        total = results[0][0] if results else 0
        sql = f"""insert into {bar.exchange.value}_{bar.symbol}_{interval.value} using {self.bar_super_table} tags ({interval.value}, {vt_symbol}, {len(bars)+total}) values {' '.join(values)}"""
        self.execute_sql(sql)
        return True

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        if len(ticks) == 0:
            return False
        tick: TickData = ticks[0]
        vt_symbol: str = tick.vt_symbol
        values: List[str] = []
        for tick in ticks:
            vs = [str(getattr(tick, field)) for field in self.tick_fields[2:-1]]
            v = f"""({int(tick.datetime.timestamp()*1000)},{tick.name or "null"},{','.join(vs)},{int(tick.localtime.timestamp()*1000)})"""
            values.append(v)
        sql = f"""insert into tick_{tick.exchange.value}_{tick.symbol} using {self.tick_super_table} tags ({vt_symbol}) values {' '.join(values)}"""
        if self.execute_sql(sql):
            return True
        return False

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime,
    ) -> List[BarData]:
        sql = f"""select {','.join(self.bar_fields)} from {exchange.value}_{symbol}_{interval.value} where time between {start.timestamp()*1000} and {end.timestamp()*1000}"""
        data = self.execute_sql(sql)
        results: List[BarData] = []
        for item in data:
            bar = {key: float(item[i + 1]) for i, key in enumerate(self.bar_fields[1:])}
            results.append(
                BarData(
                    gateway_name="tdengine",
                    symbol=symbol,
                    exchange=exchange,
                    datetime=datetime.strptime(item[0], "%Y-%m-%d %H:%M:%S.%f").replace(
                        tzinfo=pytz.utc
                    ),
                    interval=interval,
                    **bar,
                )
            )
        return results

    def load_tick_data(
        self, symbol: str, exchange: Exchange, start: datetime, end: datetime
    ) -> List[TickData]:
        sql = f"""select {','.join(self.tick_fields)} from tick_{exchange.value}_{symbol} where time between {start.timestamp()*1000} and {end.timestamp()*1000}"""
        data = self.execute_sql(sql)
        results: List[TickData] = []
        for item in data:
            tick = {
                key: float(item[i + 2]) for i, key in enumerate(self.tick_fields[2:-1])
            }
            results.append(
                TickData(
                    gateway_name="tdengine",
                    symbol=symbol,
                    exchange=exchange,
                    datetime=datetime.strptime(item[0], "%Y-%m-%d %H:%M:%S.%f").replace(
                        tzinfo=pytz.utc
                    ),
                    name=item[1],
                    **tick,
                )
            )
        return results

    def delete_bar_data(
        self, symbol: str, exchange: Exchange, interval: Interval
    ) -> int:
        tb_name = f"{exchange.value}_{symbol}_{interval.value}"
        sql = f"""drop table if exists {tb_name}"""
        return self.execute_sql(sql)

    def delete_tick_data(self, symbol: str, exchange: Exchange) -> int:
        tb_name = f"tick_{exchange.value}_{symbol}"
        sql = f"""drop table if exists {tb_name}"""
        return self.execute_sql(sql)

    def get_bar_overview(self) -> List[BarOverview]:
        sql = f"""select tbname, intervals, vt_symbol, total from {self.bar_super_table}"""
        data = self.execute_sql(sql)
        results: List[BarOverview] = []
        for item in data:
            symbol, exchange = extract_vt_symbol(item[2].upper())
            min_ts = self.execute_sql(
                f"select time from {exchange.value}_{symbol}_{item[1]} order by time limit 1"
            )
            max_ts = self.execute_sql(
                f"select time from {exchange.value}_{symbol}_{item[1]} order by time desc limit 1"
            )
            if max_ts and min_ts:
                results.append(
                    BarOverview(
                        symbol=symbol.lower(),
                        exchange=exchange,
                        interval=Interval(item[1]),
                        count=int(item[3]),
                        start=datetime.strptime(min_ts[0][0], "%Y-%m-%d %H:%M:%S.%f"),
                        end=datetime.strptime(max_ts[0][0], "%Y-%m-%d %H:%M:%S.%f"),
                    )
                )
        return results
