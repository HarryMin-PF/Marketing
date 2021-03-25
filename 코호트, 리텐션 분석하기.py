# -*- coding: utf-8 -*-
"""
Created on Sat Mar  6 17:38:32 2021

@author: mhj73
"""

#%% 패키지 임포트
import pandas as pd
import numpy as np
import os

#%% 데이터 불러오기
#e커머스 샘플 데이터 불러오기
ecommerce_data = pd.read_csv('C://Users//ecommerce_data.csv')

#샘플 데이터 확인
ecommerce_data_100 = ecommerce_data.iloc[:200,]

#데이터를 일자별로 정리하기 위해 이벤트 데이트타임을 기준으로 정렬
ecommerce_data = ecommerce_data.sort_values(['event_datetime'])

#유저별로 유입된 날짜를 구별해주기 위해서 유저별, 날짜별 최초 데이터만 남기고 중복 제거
ecommerce_data_uni = ecommerce_data.drop_duplicates(['user_id','date'],keep='first')

#유저별로 가장 처음 유입된 데이터만 남기기 위해서 유저 기준으로 최초 데이터만 남기고 중복 제거
ecommerce_data_first = ecommerce_data.drop_duplicates(['user_id'],keep='first')

#유저의 최초 유입날짜와 플랫폼, 채널만 남기고 나머지 컬럼은 제거
ecommerce_data_first = ecommerce_data_first[['user_id', 'date', 'platform','channel']]

#최초 유입날짜, 최초 유입 플랫폼, 최포 유입 채널을 식별해주기 위해 컬럼명을 변경
#rename함수 이용, columns에 딕셔너리로 기존 컬럼명을 key로 변경될 컬럼명을 value로 넣어줌
ecommerce_data_first = ecommerce_data_first.rename(columns={"date":"first_date",
                                                    "platform":"first_platform",
                                                    "channel":"first_channel"})

#유저,일자별 중복 제거 데이터도 event time 컬럼은 필요없으니 제거
ecommerce_data_uni = ecommerce_data_uni[['user_id', 'date', 'platform','channel']]

#유저들의 일자별 이벤트 데이터에 최초 유입 날짜를 병합
ecommerce_data_merge = pd.merge(left = ecommerce_data_uni, right = ecommerce_data_first, how = 'left',on = ['user_id'])
#데이터를 열어서 확인해보면 아이디별로 일자별 유입된 날짜가 있고 옆에 최초 유입 날짜가 기록

#최초 유입날짜와 이후 일자별 유입까지의 차이를 구해서 최초 유입 후 재유입까지 걸린 시간을 확인
#최초 유입일과 유입일인 first_date와 date를 데이트 타임으로 변경
ecommerce_data_merge.info()
ecommerce_data_merge['date'] = pd.to_datetime(ecommerce_data_merge['date'])
ecommerce_data_merge['first_date'] = pd.to_datetime(ecommerce_data_merge['first_date'])

#유입일에서 최초유입일을 빼서 최초 유입일부터 유입일까지의 기간을 구함
ecommerce_data_merge['time_gap'] = ecommerce_data_merge['date']-ecommerce_data_merge['first_date']

#데이터를 열어 확인해보고 인포를 사용하여 자료형을 확인해보면 타임델타라는 자료형을 확인 
#타임 델타: 날짜간의 연산 결과를 표현하는 자료형임
ecommerce_data_merge['time_gap'].head() 
ecommerce_data_merge.info()

#이 타임 델타를 dt함수를 사용하여 원하는 형태로 변경
#dt 패키지로 타임델타를 일자, 초 등으로 변환
ecommerce_data_merge['time_gap'] = ecommerce_data_merge['time_gap'].dt.days
ecommerce_data_merge.info()

#데이트 타임으로 변경했던 date, first date를 위에서 확인한 dt패키지의 strftime이라는 함수를 사용해 원하는 날짜 형태로 변환
# %Y는 연도를 %m은 월을 %d는 일자를 나타내는 기호, strftime함수는 데이트타임 자료형을 앞서 확인한 기호들을 이용해 문자열로 변환
ecommerce_data_merge['date'] = ecommerce_data_merge['date'].dt.strftime('%Y-%m-%d')
ecommerce_data_merge['first_date'] = ecommerce_data_merge['first_date'].dt.strftime('%Y-%m-%d')
ecommerce_data_merge.info()

#코호트 분석과 리텐션 분석에서 x축에 표현될 최초 유입일로부터의 경과 일자의 리스트를 time_gap을 이용해 생성
time_gap_list = list(ecommerce_data_merge['time_gap'].unique())
time_gap_list.sort()
#타임갭 리스트에 보면 -1, -2이 나오는 것을 확인
#트래커 데이터의 경우 서버에서 서버로 데이터가 이전되는 과정에서 일부 전송이 늦어지는 데이터가 발생하는 경우
#이런 경우는 데이터를 이상치로 판단하여 제거하고 사용(전체 6만건 중 2건 = 0.003%)
del time_gap_list[0:2]
ecommerce_data_merge = ecommerce_data_merge[ecommerce_data_merge['time_gap']!=-1]
ecommerce_data_merge = ecommerce_data_merge[ecommerce_data_merge['time_gap']!=-2]

#이 time_gap 리스트에 들어있는 0~30 경과일을 이용하여 리텐션 테이블 생성
#비율 대신 실제 유입 유저 수로 구해보기
ecommerce_total_retention = pd.DataFrame()
#0~30일까지 모든 경과일을 계산해주기 위해 반복문을 사용
for days in time_gap_list:
#전체 데이터에서 타임갭에 해당하는 데이터만 남겨줌
        data_1 = ecommerce_data_merge[ecommerce_data_merge['time_gap']==days]
#타임갭이 하나로 통일된 데이터에서 최초 유입날짜를 기준으로 유저 수를 카운트 -> 최초 유입날짜별 유저 코호트 생성
        ecommerce_data_1_group = data_1.groupby(['first_date'])['user_id'].count().to_frame('{} days'.format(str(days)))
#최초 유입일자별로 만들어진 경과 일자별 유저 수 데이터를 0~30까지 반복하여 합쳐줌
        ecommerce_total_retention = pd.concat([ecommerce_total_retention,ecommerce_data_1_group],axis = 1)

#미디어 종류별로 데이터도 추출해주기 위해 반복문을 사용
#데이터의 채널을 유니크로 가져와 리스트로 사용
for channel_name in list(ecommerce_data_merge['channel'].unique()):
    globals()['ecommerce_{}_retention'.format(channel_name)] = pd.DataFrame()
    #채널별로 데이터를 추출해서 할당
    ecommerce_data_channel = ecommerce_data_merge[ecommerce_data_merge['channel']==channel_name]
    #채널별 데이터에서 최초 유입일자별로 경과일에 해당하는 유저 수를 카운트하는 것을 반복문으로 설정
    for days in time_gap_list:
        data_1 = ecommerce_data_channel[ecommerce_data_channel['time_gap']==days]
        ecommerce_data_1_group = data_1.groupby(['first_date'])['user_id'].count().to_frame('{} days'.format(str(days)))
        #globals()로 할당한 변수를 그대로 사용
        globals()['ecommerce_{}_retention'.format(channel_name)] = pd.concat([globals()['ecommerce_{}_retention'.format(channel_name)],ecommerce_data_1_group],axis = 1)

#이렇게 만들어진 전체 그리고 채널별 코호트 리텐션 데이터를 판다스의 엑셀 라이터를 이용해 각각 시트로 저장함
#저장될 파일명 할당
writer = pd.ExcelWriter('C://Users//ecommerce_10_retention_count.xlsx', engine='xlsxwriter')
#전체 + 채널 리스트를 반복문에 넣고 전체 + 채널 코호트 리텐션 데이터를 to_excel함수를 이용해 위에서 할당한 파일명에 저장함
for channel_name in ['total']+list(ecommerce_data_merge['channel'].unique()):
    globals()['ecommerce_{}_retention'.format(channel_name)].to_excel(writer, sheet_name= '{}_retention_10_count'.format(channel_name),index=True)

#위 전체 + 채널별 코호트 리텐션 데이터가 엑셀의 시트로 저장된 데이터들을 한번에 추출
writer.save()

#리텐션을 최초 유입일 당일에 발생한 유저수로 각 경과일의 데이터를 나누어 비율로 환산함
for channel_name in ['total']+list(ecommerce_data_merge['channel'].unique()):
    #최초 유입일 당일 데이터인 0 days 데이터를 first day로 할당
    globals()['ecommerce_{}_retention'.format(channel_name)]['first day'] = globals()['ecommerce_{}_retention'.format(channel_name)]['0 days']
    #타입갭 리스트의 0~30일 경과일 목록을 반복문으로 돌려 0~30일까지 경과일이 first day 값으로 나누어지도록 계산
    for days in time_gap_list:
        globals()['ecommerce_{}_retention'.format(channel_name)]['{} days'.format(str(days))] = globals()['ecommerce_{}_retention'.format(channel_name)]['{} days'.format(str(days))]/globals()['ecommerce_{}_retention'.format(channel_name)]['first day']
        
#위에서 데이터를 저장한 것과 동일한 방식으로 비율 데이터도 저장함
writer = pd.ExcelWriter('C://Users//ecommerce_10_retention_ratio.xlsx', engine='xlsxwriter')
for channel_name in ['total']+list(ecommerce_data_merge['channel'].unique()):
    globals()['ecommerce_{}_retention'.format(channel_name)].to_excel(writer, sheet_name= '{}_retention_ratio_10'.format(channel_name),index=True)
writer.save()





#%%구매 이벤트로 진행
ecommerce_data = pd.read_csv('C://Users//ecommerce_data.csv')

#샘플 데이터 확인
ecommerce_data_100 = ecommerce_data.iloc[:200,]

#이벤트데이트타임으로 정렬
ecommerce_data = ecommerce_data.sort_values(['event_datetime'])

#isin함수를 이용해 구매 데이터만 남겨주기
ecommerce_data_pur = ecommerce_data[ecommerce_data['event_category'].isin(['Order Complete (App)','Order Complete (Web)'])]
ecommerce_data_pur = ecommerce_data_pur.sort_values(by=['user_id', 'event_datetime'])

#일별 유니크 & 전체 최초 남기기
ecommerce_data_uni = ecommerce_data_pur.drop_duplicates(['user_id','date'],keep='first')
ecommerce_data_first = ecommerce_data_pur.drop_duplicates(['user_id'],keep='first')
ecommerce_data_first = ecommerce_data_first[['user_id', 'date', 'platform','channel']]
ecommerce_data_first = ecommerce_data_first.rename(columns={"date":"first_date",
                                                    "platform":"first_platform",
                                                    "channel":"first_channel"})
#최초 유입일과 이벤트 데이터 합치기 
ecommerce_data_uni = ecommerce_data_uni[['user_id', 'date', 'platform','channel']]
ecommerce_data_merge = pd.merge(left = ecommerce_data_uni, right = ecommerce_data_first, how = 'left',on = ['user_id'])
#타임갭 구하기
ecommerce_data_merge['date'] = pd.to_datetime(ecommerce_data_merge['date'], utc=False)
ecommerce_data_merge['first_date'] = pd.to_datetime(ecommerce_data_merge['first_date'], utc=False)
ecommerce_data_merge['time_gap'] = ecommerce_data_merge['date'] - ecommerce_data_merge['first_date']
ecommerce_data_merge['time_gap'] = ecommerce_data_merge['time_gap'].dt.days
#일자 만들기
ecommerce_data_merge['date'] = ecommerce_data_merge['date'].dt.strftime('%Y-%m-%d')
ecommerce_data_merge['first_date'] = ecommerce_data_merge['first_date'].dt.strftime('%Y-%m-%d')


#-1보다 큰 것들만 미리 남겨줌
ecommerce_data_merge = ecommerce_data_merge[ecommerce_data_merge['time_gap']>-1]
time_gap_list = list(ecommerce_data_merge['time_gap'].unique())

time_gap_list.sort()

#전체 구매 코호트 리텐션 데이터 추출
ecommerce_total_retention = pd.DataFrame()
for days in time_gap_list:
        data_1 = ecommerce_data_merge[ecommerce_data_merge['time_gap']==days]
        ecommerce_data_1_group = data_1.groupby(['first_date'])['user_id'].count().to_frame('{} days'.format(str(days)))
        ecommerce_total_retention = pd.concat([ecommerce_total_retention,ecommerce_data_1_group],axis = 1)

#채널별 구매 코호트 리텐션 데이터 추출
for channel_name in list(ecommerce_data_merge['channel'].unique()):
    globals()['ecommerce_{}_retention'.format(channel_name)] = pd.DataFrame()
    ecommerce_data_channel = ecommerce_data_merge[ecommerce_data_merge['channel']==channel_name]
    for days in time_gap_list:
        data_1 = ecommerce_data_channel[ecommerce_data_channel['time_gap']==days]
        ecommerce_data_1_group = data_1.groupby(['first_date'])['user_id'].count().to_frame('{} days'.format(str(days)))
        globals()['ecommerce_{}_retention'.format(channel_name)] = pd.concat([globals()['ecommerce_{}_retention'.format(channel_name)],ecommerce_data_1_group],axis = 1)

#구매 코호트 리텐션 카운트 데이터 엑셀로 내보내기
writer = pd.ExcelWriter('C://Users//ecommerce_pur_10_retention_count.xlsx', engine='xlsxwriter')
for channel_name in ['total']+list(ecommerce_data_merge['channel'].unique()):
    globals()['ecommerce_{}_retention'.format(channel_name)].to_excel(writer, sheet_name= '{}_retention_10_count'.format(channel_name),index=True)
writer.save()

#리텐션 비율 구하기
for channel_name in ['total']+list(ecommerce_data_merge['channel'].unique()):
    globals()['ecommerce_{}_retention'.format(channel_name)]['first day'] = globals()['ecommerce_{}_retention'.format(channel_name)]['0 days'.format(str(days))]
    for days in time_gap_list:
        globals()['ecommerce_{}_retention'.format(channel_name)]['{} days'.format(str(days))] = globals()['ecommerce_{}_retention'.format(channel_name)]['{} days'.format(str(days))]/globals()['ecommerce_{}_retention'.format(channel_name)]['first day']

#구매 코호트 리텐션 비율로 데이터 엑셀로 내보내기
writer = pd.ExcelWriter('C://Users//ecommerce_pur_10_retention_ratio.xlsx', engine='xlsxwriter')
for channel_name in ['total']+list(ecommerce_data_merge['channel'].unique()):
    globals()['ecommerce_{}_retention'.format(channel_name)].to_excel(writer, sheet_name= '{}_retention_ratio_10'.format(channel_name),index=True)
writer.save()
