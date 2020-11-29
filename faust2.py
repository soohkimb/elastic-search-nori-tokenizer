import faust
import json
import math
import re
import requests
import numpy as np
from elasticsearch import Elasticsearch, helpers
from ast import literal_eval
import os

elasticsearch_ip = os.getenv('ELASTICSEARCH_HOST','localhost')

es = Elasticsearch(elasticsearch_ip)

app = faust.App('test', broker='kafka://49.50.174.75:9092', broker_max_poll_record=3)

def searchAll():
    print('--')
    res = es.search(
            index = "naver.finance.board",
            body = {
                "query":{"match_all":{}}
                }
    )
    print(json.dumps(res, ensure_ascii=False, indent = 1))

searchAll()

class Message(faust.Record):
    date: str
    collected_at: str
    id: str
    ip: str
    title: str
    body: str
    good: int
    bad: int
    is_reply: str
    emotion: int = 0
    positive_score: float = 0.0
    normal_score: float = 0.0
    negative_score: float = 0.0

nf_topic = app.topic('naver.finance.board.second', value_type=Message)
target_topic = app.topic('naver.finance.board', value_type=Message)
target_url = "http://118.67.133.179:8888/target"


@app.agent(nf_topic)
async def finance_board(messages):
    async for msg in messages:
        #여기에 넣어
        
        res = msg.asdict()
       # requests.post(target_url, json=res)
       


if __name__ == '__main__':
    app.main()
   
