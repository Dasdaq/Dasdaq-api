#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""易盾文本在线检测接口python示例代码
接口文档: http://dun.163.com/api.html
python版本：python2.7
运行:
    1. 修改 SECRET_ID,SECRET_KEY,BUSINESS_ID 为对应申请到的值
    2. $ python text_check_api_demo.py
"""
__author__ = 'yidun-dev'
__date__ = '2016/3/10'
__version__ = '0.1-dev'

import hashlib
import time
import random
import requests
import json


class TextCheckAPIDemo(object):
    """文本在线检测接口示例代码"""

    API_URL = "https://as.dun.163yun.com/v3/text/check"
    VERSION = "v3.1"

    def __init__(self, secret_id, secret_key, business_id):
        """
        Args:
            secret_id (str) 产品密钥ID，产品标识
            secret_key (str) 产品私有密钥，服务端生成签名信息使用
            business_id (str) 业务ID，易盾根据产品业务特点分配
        """
        self.secret_id = secret_id
        self.secret_key = secret_key
        self.business_id = business_id

    def gen_signature(self, params=None):
        """生成签名信息
        Args:
            params (object) 请求参数
        Returns:
            参数签名md5值
        """
        buff = ""
        for k in sorted(params.keys()):
            buff += str(k) + str(params[k])

        buff += self.secret_key
        buff = buff.encode('utf-8')
        return hashlib.md5(buff).hexdigest()

    def check(self, params):
        """请求易盾接口
        Args:
            params (object) 请求参数
        Returns:
            请求结果，json格式
        """
        params["secretId"] = self.secret_id
        params["businessId"] = self.business_id
        params["version"] = self.VERSION
        params["timestamp"] = int(time.time() * 1000)
        params["nonce"] = int(random.random()*100000000)
        params["signature"] = self.gen_signature(params)

        try:
            z = requests.post(self.API_URL, params=params)
            return z.json()
        except Exception as ex:
            print("调用API接口失败:", str(ex))


def run(content):
    SECRET_ID = "5fc32391fc2a775407fd77ad2a0c52db"  # 产品密钥ID，产品标识
    SECRET_KEY = "aee662db39194d024d2624aa7f009396"  # 产品私有密钥，服务端生成签名信息使用，请严格保管，避免泄露
    BUSINESS_ID = "f6e78a47a13ceadf177f54a9dffe16fa"  # 业务ID，易盾根据产品业务特点分配
    text_api = TextCheckAPIDemo(SECRET_ID, SECRET_KEY, BUSINESS_ID)

    params = {
        "dataId": "ebfcad1c-dba1-490c-b4de-e784c2691768",
        "content": content,
    }
    ret = text_api.check(params)
    if ret["code"] == 200:
        action = ret["result"]["action"]
        taskId = ret["result"]["taskId"]
        labelArray = json.dumps(ret["result"]["labels"], ensure_ascii=False)
        if action == 0:
            return True
        # elif action == 1:
        #     # return {'status': 1, 'msg': labelArray}
        #     # print("taskId=%s，文本机器检测结果：嫌疑，需人工复审，分类信息如下：%s" %
        #     #       (taskId, labelArray))
        # elif action == 2:
        #     # print("taskId=%s，文本机器检测结果：不通过，分类信息如下：%s" % (taskId, labelArray))
    return False
    # print("ERROR: ret.code=%s, ret.msg=%s" % (ret["code"], ret["msg"]))


if __name__ == "__main__":
    run('习近平')
