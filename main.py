import requests
import json
import pandas as pd
from pandas import json_normalize
import tweepy

import psycopg2
from sqlalchemy import create_engine
import datetime
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from config import *

import openai

# urlの作成
# ランキングAPIのベースとなるURL
urlbase = 'https://app.rakuten.co.jp/services/api/IchibaItem/Ranking/20170628?'

parameters = {
    'applicationId': applicationId,  # アプリID
    'affiliateId': affiliateId,  # アフィリエイトID
    'period': 'realtime'  # ランキング集計期間
}

# Twitterコード
CONSUMER_KEY = CONSUMER_KEY
CONSUMER_SECRET = CONSUMER_SECRET
ACCESS_TOKEN = ACCESS_TOKEN
ACCESS_SECRET = ACCESS_SECRET

client = tweepy.Client(
    consumer_key        = CONSUMER_KEY,
    consumer_secret     = CONSUMER_SECRET,
    access_token        = ACCESS_TOKEN,
    access_token_secret = ACCESS_SECRET,
)




# スプシと接続
def open_spreadsheets():

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    credentials = Credentials.from_service_account_file(
        "./rakuten-ranking-373504-cb389f3a8e17.json",
        scopes=scopes
    )

    gc = gspread.authorize(credentials)

    spreadsheet_url = "https://docs.google.com/spreadsheets/d/1rMzkPMncO7SxeaqcC62VUZ27p8Xbxc4Lgg7obUUrhSQ/edit#gid=0"

    spreadsheet = gc.open_by_url(spreadsheet_url)

    return spreadsheet

# 楽天のランキングを取得


def get_ranking():
    # jsonデータの取得
    r = requests.get(urlbase, params=parameters)
    json_data = r.json()

    # 時間取得
    t_delta = datetime.timedelta(hours=9)
    JST = datetime.timezone(t_delta, 'JST')
    now = datetime.datetime.now(JST)
    # YYYY/MM/DD hh:mm:ss形式に書式化
    d = now.strftime('%Y/%m/%d %H:%M:%S')

    # jsondata内のItemsにアクセスした後に、データフレームに格納
    df = json_normalize(json_data['Items'])
    # print(df.head())

    # 必要な情報だけ抽出
    df_pickup = df.loc[:,
                       ['Item.rank',
                        'Item.itemName',
                        'Item.affiliateUrl']]

    # 項目の日本語辞書作成
    rename_dic = \
        {'Item.rank': 'rank',
         'Item.itemName': 'name',
         'Item.affiliateUrl': 'url'}

    # 項目名変更
    df_pick_rename = df_pickup.rename(columns=rename_dic)
    df_pick_rename['flag'] = 0

    # スプレッドシート取得
    spreadsheet = open_spreadsheets()

    # データフレームをデータベースに格納
    # df_pick_rename.to_sql('info', con=engine, if_exists='replace', index=True)
    set_with_dataframe(spreadsheet.worksheet("info"),
                       df_pick_rename, include_index=True)

    client.create_tweet(text='楽天人気商品ランキングを更新しました！\n'+d)

def chatGPT(name):
    openai.api_key = OpenAI_API

    system_msg = "あなたは，YouTuberです．【条件】を満たすように，【商品】を訴求する最高の紹介文を120文字以内で出力してください．\
        【条件】\
        ・読んだ人が買いたくなるような魅力的な文章を書く．\
        ・紹介文以外は出力しない．\
        ・120文字以内で\
            ・生成された紹介文が120文字以内か確認．\
            ・120文字を超えている場合，120文字以内になるまで☆の処理を繰り返す．\
                ☆文章から余分な単語やフレーズを削除します．"

    message = "【商品】"+name

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": message}
        ]
    )

    return response["choices"][0]["message"]["content"].lstrip()

def tweet(data, context):

    # スプレッドシート取得
    spreadsheet = open_spreadsheets()

    df = get_as_dataframe(spreadsheet.worksheet("info"), dtype=[], usecols=[
                          0, 1, 2, 3, 4], skiprows=0, header=0, index_col=0)
    df = df.head(30)
    df['rank'] = df['rank'].astype('int')
    df['flag'] = df['flag'].astype('int')

    # print(df)


    # データフレームを逆順
    df = df.iloc[::-1]

    # 30件flag
    flag_all = 0

    # Tweet作成
    for index, row in df.iterrows():
        if row['flag'] == 0:
            if len('☆第'+str(row['rank'])+'位☆\n\n' + row['name']) > 116:
                delete = len('☆第'+str(row['rank'])+'位☆\n\n' + row['name'])-116
                item_name = row['name'][:-delete]
            else:
                item_name=row['name']
                
            tweet = '☆第'+str(row['rank'])+'位☆\n\n' + \
                item_name+"\n\n"+row['url']
            # print(tweet)
            df['flag'][index] = 1
            tweet_info = client.create_tweet(text=tweet)
            flag_all = 1
            set_with_dataframe(spreadsheet.worksheet("info"),
                            df.iloc[::-1], include_index=True)

            # １０位以内はchatgptで引用リツイート
            if row['rank'] < 11:
                retweet_url = 'https://twitter.com/rakutenranking5/status/' + str(tweet_info.data['id'])
                
                gpt_msg = chatGPT(row['name'])

                if len(gpt_msg) > 125:
                    delete = len(gpt_msg)-125
                    gpt_msg = gpt_msg[:-delete]

                retweet_msg = gpt_msg+retweet_url
                client.create_tweet(text=retweet_msg)

            break

    if flag_all == 0:
        get_ranking()


# if __name__ == "__main__":
#     # get_ranking()
#     tweet()

