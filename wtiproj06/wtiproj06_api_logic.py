import json
import time
from itertools import repeat
import pandas as pd
import numpy as np
import wtiproj06_CassClient

class api_logic:
    def __init__(self):
        self.cass=wtiproj06_CassClient.cassandra()
        self.avg_genres=[]
    def clear_table(self,table):
        self.cass.clear_table(table)
    def push_data(self,table,data,id=None):
        self.cass.push_data_table(table,data,id)
    def get_data(self,table,id=None):
        return self.cass.get_data_table(table,id)
    def load(self,rows=None):
        if rows==None:
            ratings = pd.read_csv("/home/vagrant/PycharmProjects/wtiproj03/user_ratedmovies.dat.txt", sep='\t', dtype={"userID": int, "movieID": int}, delimiter='\t').drop(
            ["date_day", "date_minute", "date_month", "date_second", "date_year", "date_hour"], axis=1)
        else:
            ratings = pd.read_csv("/home/vagrant/PycharmProjects/wtiproj03/user_ratedmovies.dat.txt", sep='\t', dtype={"userID": int, "movieID": int},nrows=rows, delimiter='\t').drop(
            ["date_day", "date_minute", "date_month", "date_second", "date_year", "date_hour"], axis=1)
        genres = pd.read_csv("/home/vagrant/PycharmProjects/wtiproj03/movie_genres.dat.txt", header=0, sep='\t', delimiter='\t')
        genres['dummy'] = int(1)
        genre_list = []

        pivoted=genres.pivot_table(index='movieID',columns='genre',values='dummy')

        for q in pivoted.columns:
            temp='genre-'+q
            genre_list.append(temp)
        pivoted.columns=genre_list
        joined = pd.merge(ratings, pivoted, on='movieID')

        return joined,genre_list



    def convert_to_dict_list(self,df):
        list_of_dict=[]
        iter=df.iterrows()
        for row in iter:
            dict_row=row[1].to_dict()
            json_row=json.dumps(dict_row)
            dict_row=json.loads(json_row)
            list_of_dict.append(dict_row)

        return list_of_dict

    def bezstratnosc(self,df):

        xx = df.getJoined().sort_index(axis=1)

        xx2 = dictToDF(dfToDict(df.getJoined()))

        return (xx == xx2).all()

    def ToDf(selfself,dict):
        df = pd.DataFrame.from_dict(dict)
        df=df.fillna(0)
        return df #df.sort_index(axis=1)

    def convert_to_df(self,dicts):
        dict_of_list={}
        keys_of_dict= list(dicts[0])
        for tempkey in keys_of_dict:
            dict_of_list[tempkey]=[]
        for item in dicts:
            for tempkey in keys_of_dict:
                dict_of_list[tempkey].append(item[tempkey])
        converted=pd.DataFrame.from_dict(dict_of_list)
        converted=converted.fillna(0)
        return converted, dict_of_list


    def avg(self, df, genre_names, unbiased=True):
        columns=df.columns
        non_genre_from_set=list(set(columns).difference(set(genre_names)))
        non_genre_columns=[]
        for temp_column in columns:
            if temp_column in non_genre_from_set:
                non_genre_columns.append(temp_column)
        ratings_vector=df['rating'].values
        ratings_array=ratings_vector.reshape(ratings_vector.shape[0],1)
        genres_array=df[genre_names].values
        non_genre_columns_array=df[non_genre_columns].values
        ratings_broadcast=ratings_array * genres_array
        average_genre_rating=np.nanmean(ratings_broadcast, axis=0)
        if unbiased:
            avg_matrix=average_genre_rating.reshape(1,average_genre_rating.shape[0])
            ratings_broadcast_unbiased=ratings_broadcast-avg_matrix
        else:
            ratings_broadcast_unbiased=ratings_broadcast
        df_unbiasedarray=np.hstack([non_genre_columns_array,ratings_broadcast_unbiased])
        df_unbiased=pd.DataFrame(df_unbiasedarray, columns=columns)
        return df_unbiased, average_genre_rating

    def userav(self, df, genre_names, id):
        df, lista=api_logic.avg(self,df,genre_names,unbiased=False)
        user_df=df.where(df['userID']==id)
        user_avg_genres=np.nanmean(user_df[genre_names].values, axis=0)
        #user_avg_genres=user_avg_genres.reshape(1,user_avg_genres.shape[0])
        return user_avg_genres

    def user_profile(self,df,lista,id):
        useravg = api_logic.userav(self,df,lista,id)
        df_unbiased,avg_ratings=api_logic.avg(self,df,lista)
        profile=useravg-avg_ratings
        profile=np.nan_to_num(profile)
        return profile

'''TEST DZIALANIA'''

if __name__ == '__main__':
    api=api_logic()
    df,lista=api.load(100)
    print('lista kolumn: ',lista)
    print(len(df.index))
    for index, row in df.iterrows():
        print(row.to_json(orient='columns'))
    #print('DICTIONARY')
    #print(dfToDict(df))


    print('DICTIONARY converted')
    print(convert_to_dict_list(df))
    conv,l=convert_to_df(convert_to_dict_list(df))
    print('Data Frame converted')
    conv=conv.fillna(0)
    print(conv)
    print('DF oryginalny')
    df=df.fillna(0)
    #print(df)
    comp_result=df==conv
    comp_result2=comp_result.all(axis=None)
    print('wynik porwnania: ',comp_result2)

    print('AVG')
    df2,srednia=avg(df,lista)
    print(df2)
    print()
    print('srednia ocena gatunkow')
    print(srednia)




    print('srednia uzytkownika 75')
    print(user_avg(df,lista,75))
    print()
    print()
    print()
    print('Profil uzytkownika 75')
    print(user_profile(df,lista,75))