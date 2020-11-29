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
spam_cnt = 1.0

es = Elasticsearch(elasticsearch_ip)

def update_suspicious_score(user_name, ip, spam_title, content, reply, write_time, spam_cnt):
    # 동일한 ip를 사용하는 id 중 도배하는 사람 색출
    data = es.search(
            index = 'naver.finance.board',
            body = {
                "min_score": 15,
                "query" : {
                    "bool":{
                        "must":{
                            "match":{
                                "title" : spam_title    
                                }
                            },
                    "filter":{
                        "term":{"is_reply":0}
                        }
                    }
                    },
                "size" : 10
                }
            )
    # 해당 유저 id와 spam_cnt 가져오기
    get_spam_cnt(ip, user_name, spam_cnt)

    for document in [x['_source'] for x in data['hits']['hits']]:
        if (document['date'] == write_time):
            #print('duplicate pass')
            continue

        # 도배 글 쓴 사람이 같다면 -> ip 와 id 가 같은 사람.. 지수 + 1, id를 바꿔서 도배.. 지수 + 100
        if document['ip'] == ip and document['id'] == user_name:
            spam_cnt += 0.01
        elif document['ip'] == ip and document['id'] != user_name:
            spam_cnt += 1
        # print('예전 글 작성 시간 : ' + document['date'])
        # print('방금 들어온 글 작성 시간 : ' + write_time)
        # print(spam_title)

    update_user_list(ip, user_name,spam_cnt)

app = faust.App('nf-worker-2', broker='kafka://49.50.174.75:9092', broker_max_poll_record=3)

def get_spam_cnt(ip, user_name, spam_cnt):
    data = es.search(
            index = 'score.storage.board',
            body = {
                "query" : {
                    "bool":{
                        "filter":[
                            {"match": {"id": user_name}},
                            {"match": {"ip": ip}}
                            ]
                        }
                }
            }
            )
    
    print(data['hits']['hits'])
    max_num = -1
    # 해당 유저가 존재하지 않을 경우..
    if data['hits']['total']['value'] == 0:
        return
    else:
        # 존재 한다면 값 가져오기
        for document in [x['_source'] for x in data['hits']['hits']]:
            if max_num < document['suspicious_score']:
                max_num = document['suspicious_score']
                best_id = document['id']
        try:
            es.delete(index='score.storage.board', id = data['hits']['hits'][0]['_id'])
        except:
            print("there is no data for id : " + data['hits']['hits'][0]['_id'])

        spam_cnt =  data['hits']['hits'][0]['_source']['suspicious_score']
        es.indices.refresh(index="score.storage.board")

def update_user_list(ip, user_name, spam_cnt):
    new_user = {
        'ip' : ip,
        'id' : user_name,
        'suspicious_score' : spam_cnt
        }
    print("new_data_update",new_user)
    es.index(index = 'score.storage.board', body = new_user)


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
        print("check:",spam_cnt)
        update_suspicious_score(msg.id, msg.ip, msg.title, msg.body, msg.is_reply, msg.date,spam_cnt)
        print(msg)
        res = msg.asdict()
        requests.post(target_url, json=res)
       


if __name__ == '__main__':
   
    app.main()
