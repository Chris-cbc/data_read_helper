import pymysql


class Reader(object):
    def __init__(self, **kwargs):
        self._reader_info = kwargs["reader"]
        assert self._reader_info
        self._reader_account = self._reader_info["account"]
        self._reader_conn = None
        self._reader_curs = None
        self._reader_query = self._reader_info['sql']
        self._reader_name = self._reader_info['name']
        assert self._reader_query.lower().strip().startswith("select")

    def read(self):
        raise NotImplementedError

    @staticmethod
    def _connection(**kwargs):
        kwargs["user"] = kwargs.pop("username")
        kwargs["db"] = kwargs.pop("database")
        conn = pymysql.connect(**kwargs, charset='utf8')
        return conn

    def __enter__(self):
        if not self._reader_conn:
            self._reader_conn = self._connection(**self._reader_account)
            self._reader_curs = self._reader_conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._reader_curs:
            self._reader_curs.close()
        if self._reader_conn:
            self._reader_conn.close()

    @property
    def reader_curs(self):
        return self._reader_curs

    @property
    def reader_query(self):
        return self._reader_query
