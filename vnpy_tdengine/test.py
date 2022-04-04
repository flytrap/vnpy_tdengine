#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Created by flytrap
# Created time: 2022/04/04
from datetime import datetime
from tdengine_database import (
    TdEngineDatabase,
    SETTINGS,
    BarData,
    Interval,
    Exchange,
    TickData,
)
from loguru import logger


class TestTdengineDatabase(object):
    def __init__(self) -> None:
        SETTINGS["database.database"] = "test_vnpy"
        SETTINGS["database.host"] = "192.168.0.102"
        SETTINGS["database.port"] = 6041
        SETTINGS["database.user"] = "root"
        SETTINGS["database.password"] = "taosdata"

        self.db = TdEngineDatabase()

        self.symbol = "test"
        self.bar = BarData(
            interval=Interval.DAILY,
            symbol=self.symbol,
            exchange=Exchange.SZSE,
            datetime=datetime.now(),
            gateway_name="tdengine",
        )
        self.tick = TickData(
            symbol=self.symbol,
            exchange=Exchange.SZSE,
            datetime=datetime.now(),
            localtime=datetime.now(),
            gateway_name="tdengine",
        )

    def test_save_bar(self):
        logger.info("test save bar")
        result = self.db.save_bar_data([self.bar])
        assert result

    def test_save_tick(self):
        logger.info("test save tick")
        result = self.db.save_tick_data([self.tick])
        assert result

    def test_load_bar(self):
        logger.info("test load bar")
        data = self.db.load_bar_data(
            self.symbol,
            Exchange.SZSE,
            Interval.DAILY,
            datetime.now().replace(hour=0),
            datetime.now(),
        )
        assert len(data) > 0

    def test_load_tick(self):
        logger.info("test load tick")
        data = self.db.load_tick_data(
            self.symbol, Exchange.SZSE, datetime.now().replace(hour=0), datetime.now()
        )
        assert len(data) > 0

    def test_del_bar(self):
        logger.info("test delete bar")
        num = self.db.delete_bar_data(self.symbol, Exchange.SZSE, Interval.DAILY)
        assert num > 0
        assert (
            len(
                self.db.load_bar_data(
                    self.symbol,
                    Exchange.SZSE,
                    Interval.DAILY,
                    datetime.now().replace(hour=0),
                    datetime.now(),
                )
            )
            == 0
        )

    def test_del_tick(self):
        logger.info("test delete tick")
        num = self.db.delete_tick_data(self.symbol, Exchange.SZSE)
        assert num > 0
        assert (
            len(
                self.db.load_tick_data(
                    self.symbol,
                    Exchange.SZSE,
                    datetime.now().replace(hour=0),
                    datetime.now(),
                )
            )
            == 0
        )

    def test_get_overview(self):
        logger.info("test overview")
        overviews = self.db.get_bar_overview()
        assert len(overviews) > 0
        overview = overviews[0]
        assert overview.count > 0


def main():
    td = TestTdengineDatabase()
    td.test_save_bar()
    td.test_save_tick()
    td.test_load_bar()
    td.test_load_tick()
    td.test_get_overview()
    td.test_del_bar()
    td.test_del_tick()


if __name__ == "__main__":
    main()
