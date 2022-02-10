# -*- coding: utf-8 -*-
from config.logger import Logging

logger = Logging().log

_SINGLE = "SINGLE"
_MSG = "msg"
_SIGNATURE = "signature"
_MOBILE = "mobile"
_ROLE_PREFIX = ":"


class Container(object):
    def __init__(self, **kwargs):
        _property = kwargs.get("property", "")
        self._outCol = kwargs.get("output", dict())
        assert _property and self._outCol
        self._include = set(self._outCol.get("INCLUDE", list()))
        # 加载抽取签名的正则
        self._signature = compile4Signature()
        # 加载抽取字段的正则
        self._property = compile4Property(_property)
        _allPropertyItems = set()
        _allYmlItems = set()
        for _, items in self._property.items():
            _allPropertyItems = _allPropertyItems | items.keys()
        for _, entityList in self._outCol.items():
            _allYmlItems = _allYmlItems | set(entityList)
        for p in _allPropertyItems:
            if p not in _allYmlItems:
                logger.info(p + " is not in yml file")

    def extract(self, line):
        signature = self._signature.search(line)
        if signature:
            sig = signature.group(0)
            if sig in self._property:
                row = dict()
                package = self._property[sig]
                for col, value in package.items():
                    df = dict()
                    if isinstance(value, list):
                        for seed in value:
                            result = seed.search(line)
                            if result:
                                if col.startswith(_ROLE_PREFIX):
                                    result = col.split(":")[2]
                                    col = col.split(":")[1]
                                    df.setdefault(col, result)
                                else:
                                    df.setdefault(col, result.group(0))
                                # 抽取到后断开
                                break
                    elif isinstance(value, str):
                        df.setdefault(col, value)
                    # 如果没有提到字段，那么这条为空
                    # if not df:
                    #     continue
                    # 添加签名字段
                    if _SIGNATURE in self._include:
                        row.update({_SIGNATURE: sig})
                    # 添加短信内容字段
                    if _MSG in self._include:
                        row.update({_MSG: line})
                    if _MOBILE in self._include:
                        row.update({_MOBILE: ""})
                    # 如果没有提到字段，那么这条只有signature和msg被插入
                    if not df:
                        continue
                    # 字段分类
                    for name, entity in self._outCol.items():
                        if col in set(entity):
                            # 非复合字段，string类型
                            if name == _SINGLE:
                                row.update(df)
                            # 复合字段，json类型
                            else:
                                name = name.lower()
                                row.setdefault(name, dict())
                                row[name].update(df)
                return row
            else:
                # 短信内容中找到签名，但没有对应正则
                logger.info("signature not build: {msg}".format(msg=line))
        else:
            # 短信内容中没有找到签名
            logger.info("signature not found: {msg}".format(msg=line))
