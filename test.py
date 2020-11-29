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

def get_suspicious_score(user_name, ip, spam_title, content, reply, write_time):
    # 동일한 ip를 사용하는 id 중 도배하는 사람 색출
    spam_cnt = 0

    data = es.search(
            index = 'naver.finance.board',
            body = {
                "query" : {
                    "match":{
                        'title': spam_title
                        }
                    },
                "size" : 10000
                }
            )

    for document in [x['_source'] for x in data['hits']['hits']]:
        if (document['date'] == write_time):
            #print('duplicate pass')
            continue
        if (reply != 0):
            #print('댓글 pass')
            continue;

        # 도배 글 쓴 사람이 같다면 -> ip 와 id 가 같은 사람.. 지수 + 1, id를 바꿔서 도배.. 지수 + 100
        if document['ip'] == ip and document['id'] == user_name:
            spam_cnt += 1
        elif document['ip'] == ip and document['id'] != user_name:
            spam_cnt += 100

    return spam_cnt
        # print('예전 글 작성 시간 : ' + document['date'])
        # print('방금 들어온 글 작성 시간 : ' + write_time)
        # print(spam_title)

app = faust.App('nf-worker-2', broker='kafka://49.50.174.75:9092', broker_max_poll_record=3)

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
    suspicious_score: float = 0.0
    
nf_topic = app.topic('naver.finance.board.second', value_type=Message)
target_topic = app.topic('naver.finance.board', value_type=Message)
target_url = "http://118.67.133.179:8888/target"

@app.agent(nf_topic)
async def finance_board(messages):
    async for msg in messages:
        msg.suspicious_score = get_suspicious_score(msg.id, msg.ip, msg.title,
                msg.body, msg.is_reply, msg.date)
        print(msg)
        res = msg.asdict()
        requests.post(target_url, json=res)
       


if __name__ == '__main__':
   
    app.main()
