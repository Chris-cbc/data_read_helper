# -*- coding: utf-8 -*-
from config.logger import Logging

logger = Logging().log

import pymysql
import json

INSERT = "insert into table {TABLE} values"
SELECTWithCondition = "select %s from %s where %s=%s"
UPDATE = "UPDATE {TABLE} SET %s = %s WHERE mobile = %s"


class Writer(object):
    def __init__(self, **kwargs):
        super().__init__()
        self._writer_info = kwargs
        assert self._writer_info
        self._TABLE = self._writer_info["target_table"]
        self._writer_account = self._writer_info["account"]
        self._target_table = INSERT.format(TABLE=self._TABLE)
        self._conn = None
        self._writer_name = kwargs['name']

    def _connection(self):
        raise NotImplementedError

    def write(self, df):
        raise NotImplementedError


class ClickHouseWriter(Writer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        assert self._writer_name.strip() == "ClickHouseWriter"

    def _connection(self, **kwargs):
        self._writer_account["user"] = self._writer_account.pop("username")
        return Client(**kwargs)

    def write(self, df):
        for line in df:
            try:
                line = map(lambda x: x if x else "", line)
                self._conn.execute(self._target_table, params=[tuple(line)])
            except Exception as e:
                logger.error(line)
                logger.error(e)

    def __enter__(self):
        if not self._conn:
            self._conn = self._connection(**self._writer_account)
            return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            self._conn.disconnect()


class MysqlWriter(Writer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        assert self._writer_name.strip() == "MysqlWriter"

    def _connection(self, **kwargs):
        kwargs["user"] = kwargs.pop("username")
        kwargs["db"] = kwargs.pop("database")
        conn = pymysql.connect(**kwargs, charset='utf8')
        return conn

    # def _select(self, **kwargs):
    #     columns = kwargs.get("column", "")
    #     table = kwargs.get("table", TARGET_TABLE)
    #     searchCol = kwargs.get("searchCol", "")
    #     condition = kwargs.get("condition", "")
    #     self._curs.execute(SELECTWithCondition, (columns, table, searchCol, condition))
    #     return self._curs.fetchall()

    def _update(self, **kwargs):
        production = kwargs.get("production", False)
        # exist = self._select(**kwargs)
        try:
            # TODO 生产模式
            if production:
                mobile = ""
                Col = ""
                value = [{}]
                self._curs.execute(UPDATE, (Col, value, mobile))
            else:
                # 测试
                names = list(kwargs)
                data = kwargs.copy()
                for k, v in kwargs.items():
                    if isinstance(v, dict):
                        data[k] = json.dumps(v, ensure_ascii=False)
                cols = ', '.join(map(lambda x: '`{}`'.format(x.replace('`', '``')), names))
                placeholders = ', '.join(['%({})s'.format(name) for name in names])
                query = 'INSERT INTO {} ({}) VALUES ({})'.format(self._TABLE, cols, placeholders)
                self._curs.execute(query, data)
        except Exception as e:
            if len(e.args) == 2 and e.args[1].startswith("Incorrect string value"):
                data["msg"] = self.removeSpecialChar(data["msg"])
                try:
                    self._curs.execute(query, data)
                except Exception as e:
                    logger.error(e)
                    logger.error(data)
                    self._conn.rollback()
            else:
                logger.error(e)
                logger.error(data)
                self._conn.rollback()
        finally:
            self._conn.autocommit(True)

    def write(self, **kwargs):
        self._update(**kwargs)

    def __enter__(self):
        if not self._conn:
            self._conn = self._connection(**self._writer_account)
            self._curs = self._conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._curs:
            self._curs.close()
        if self._conn:
            self._conn.close()
