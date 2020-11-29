import faust
import json
import math
import re
import requests
import numpy as np
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from elasticsearch.client import IndicesClient
from ast import literal_eval
import os

elasticsearch_ip = os.getenv('ELASTICSEARCH_HOST','localhost')

es = Elasticsearch(elasticsearch_ip)
indices_client = IndicesClient(es)

app = faust.App('nf-worker-3', broker='kafka://49.50.174.75:9092', broker_max_poll_record=3)


def analyzeNori(date, tmp_id, tmp_ip, tmp_title, tmp_body):
    res = indices_client.analyze(index = "nori_test", 
    body={
        "analyzer": "nori_analyzer",
        "text": str(tmp_title+tmp_body)
    })
    token_list = [res['tokens'][x]['token'] for x in range(len(res['tokens']))]
    for token in token_list:
        if len(token)>1:
            result = es.index(index = 'nori_test', body = {
                    
                        "tokenlist" : token,
                        "date": date,
                        "id":tmp_id,
                        "ip":tmp_ip,
                        "title":tmp_title,
                        "body":tmp_body
                        
                    })
    # print(json.dumps(result, ensure_ascii=False, indent = 1))

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
target_url = "http://118.67.133.179:8888/score"


@app.agent(nf_topic)
async def finance_board(messages):
    async for msg in messages:
        analyzeNori(msg.date, msg.id, msg.ip, msg.title, msg.body)
       

if __name__ == '__main__':
    app.main()
