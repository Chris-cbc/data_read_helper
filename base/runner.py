# -*- coding: utf-8 -*-
import copy
from base.reader import Reader
from libs.utils import load_config_file
from config.logger import Logging

logger = Logging().log


class Writer(object):
    def __init__(self, **kwargs):
        self._info = kwargs.get("output", list())
        self._output = dict()

    def write(self, df):
        raise NotImplementedError

    @property
    def output(self):
        return self._output

    @property
    def info(self):
        return self._info


class Packer(object):
    def __init__(self, *args, **kwargs):
        self.params = kwargs
        self._script_name = kwargs.get("script", list())
        self.script_info = self._task(**kwargs)
        self.task = self.init_task()

    def init_task(self):
        ret = dict()
        for script, row in self.script_info.items():
            row_append = copy.deepcopy(row)
            for tf in row_append:
                func = tf["task"]
                if func:
                    tf["task"] = getattr(__import__(name="workplace.script." + script, fromlist=[func]), func)
            ret.setdefault(script, row_append)
        return ret

    def _task(self, **kwargs):
        scripts = dict()
        for script in self._script_name:
            scripts.setdefault(script, kwargs[script])
        return scripts

    def process(self):
        raise NotImplementedError


# 执行TASK中的脚本与函数
class Executor(Packer, Reader, Writer):
    def __init__(self, config):
        self.config = load_config_file(config)
        Packer.__init__(self, **self.config)
        Writer.__init__(self, **self.config)

    def read(self):
        with Reader(**self.config):
            return self.read_all()

    def write(self, df):
        stack = dict()
        for ot in self.info:
            if ot in df:
                stack.setdefault(ot, df[ot])
        return stack

    @staticmethod
    def execute_task(df, tf):
        df0 = copy.deepcopy(df)
        df.update({tf["out"]: tf["task"](*[df0[x] for x in tf["in"]])})
        return df

    def process(self, **df):
        for script, tasks in self.task.items():
            for tf in tasks:
                execute = tf.get("execute", None)
                if execute:
                    df.update(self.execute_task(df, tf))
        return self.write(df)

    def process_table(self):
        return [self.process(data=line) for line in self.read()]
