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

    res = es.search(
            index = "naver.finance.board",

            body = {
                "aggs": {
                    "same_ip_group": {
                        "terms": {
                            "field" : ip,
                            "size":100
                        },
                        "aggs":{
                            "same_body_group": {
                                "terms": {
                                    "field": "title.keyword",
                                    "size":100
                                    },
                                "aggs":{
                                    "same_id_group": {
                                        "terms": {
                                            "field": "id.keyword",
                                            "size":100
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            )



def decide_bad_user(res):

    b_IpArr = res['aggregations']['same_ip_group']['buckets']
    b_IpCnt = len(b_IpArr)

    for i in range(b_IpCnt): #ip 값 하나씩 검사
        print('\n\n\n')
        userId=[]
        ip_score = 1
        b_Ip = res['aggregations']['same_ip_group']['buckets'][i]['key']; #ip
        b_bodies_arr = res['aggregations']['same_ip_group']['buckets'][i]['same_body_group']['buckets'] #같은 ip에서 사용한 글들의 집합
        b_bodiesCnt = len(b_bodies_arr) #같은 ip에서 작성한  각 게시글 수, 동일한 게시글은 한개>로 판단

        #ip_score = 1; #각 ip의 악성점수 1로 초기화

        #if b_Ip == '218.209.***.136':
        #    print(b_bodies_arr)


        for p in range(b_bodiesCnt): #한 ip당 게시글 값 하나씩 검사
            b_body = res['aggregations']['same_ip_group']['buckets'][i]['same_body_group']['buckets'][p] #ip 당 p번째 게시글
            #print(b_body)
            #b_bodyCnt = len(b_body)
            b_bodyCnt_2 = res['aggregations']['same_ip_group']['buckets'][i]['same_body_group']['buckets'][p]['doc_count'] #한 ip당 하나의 게시글을 쓴 총 user 의 개수 = 동일한 게시글의 수  ##>다른 user 의 개수가 아니라 총 user 임!!!

            if b_bodyCnt_2 > 2: #동일 게시글이 3개 이상일 때만 도배글로 판단
                ip_score += b_bodyCnt_2;

            print(str(b_Ip)+"b_bodyCnt_2: "+str(b_bodyCnt_2))

            b_idArr = res['aggregations']['same_ip_group']['buckets'][i]['same_body_group']['buckets'][p]['same_id_group']['buckets']
            b_idCnt = len(b_idArr) #한 게시글을 작성한 다른 user 의 개수

            for s in range(b_idCnt):
                var = res['aggregations']['same_ip_group']['buckets'][i]['same_body_group']['buckets'][p]['same_id_group']['buckets'][s]['key']
                checkID(userId, var) #user 를 보내줌

            #같은 사용자가 같은 게시글을 몇번 썼는지
            for s in range(b_idCnt):
                b_id_sameBody_cnt = res['aggregations']['same_ip_group']['buckets'][i]['same_body_group']['buckets'][p]['same_id_group']['buckets'][s]['doc_count']
                if b_id_sameBody_cnt > 1:
                    ip_score += 1.1

            ## 몇개의 아이디를 써서 도배글을 쓰느냐에 따른 악성도 계산
            if b_idCnt > 1 and b_idCnt < 4: # 동일 아이디가 2,3개일때
                for k in range(b_idCnt):
                    ip_score += 1.1 #ip_score : 각 ip에 id개수별 점수만큼 곱합


            elif b_idCnt >= 4: # 동일 아이디가 4개 이상일 때
                for k in range(b_idCnt):
                    ip_score += 1.2 #ip_score : 각 ip에 id개수별 점수만큼 곱합

            #print("ip 당 점수 입니다 : " + str(ip_score))

        check(userId)

        for p in range(len(userId)):
            #print(userId[p])
            doc1 = {'ip' : b_Ip, 'id' : userId[p], 'ip_score' : ip_score, 'id_score' : 0 }
            print(doc1)
            es.index(index = 'score.storage.board',  body=doc1)

def checkID(userId, var):
    i=0
    if not var in userId:
        userId.append(var)

def check(userId):
    print('----------------------------------!!!!')
    for i in range(len(userId)):
        print(userId[i])
    print('----------------------------------!!!!')
    return



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
        get_spam_cnt(ip, user_name, spam_cnt)
        #update_suspicious_score(msg.id, msg.ip, msg.title, msg.body, msg.is_reply, msg.date,spam_cnt)
        print(msg)
        res = msg.asdict()
        requests.post(target_url, json=res)
       


if __name__ == '__main__':
   
    app.main()
