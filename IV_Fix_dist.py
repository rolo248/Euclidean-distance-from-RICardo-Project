#!/usr/bin/env python
# coding: utf-8

# In[107]:


# python script to build sqlite for visualization
# coding=utf8
import subprocess
import sqlite3
import os
import json
import itertools
# import utils
import re
# import custom_exports
import shutil
import pandas as pd
import glob
import matplotlib.pyplot as plt
import numpy as np

#Set working: C:\Users\Joyas\Desktop\github\ricardo_data\database_scripts


# In[86]:


# with open("config.json", "r") as f_conf:   #Leo json config y lo llamo como f_conf.
#     conf=json.load(f_conf)                 # Cargo json.
#     database_filename=os.path.join('../sqlite_data',conf["sqlite_viz"]) #me devuelvo a la carpeta sqlite_data y utilizo
#                                                                         # "sqlite_viz" desde el json.


# In[87]:


# conn=sqlite3.connect('../sqlite_data\\RICardo_viz.sqlite')
# c=conn.cursor()


# In[88]:


# create the dataframe from a query
# df = pd.read_sql_query("SELECT * FROM flows", conn)
# df
#csv=pd.read_csv('../data/currencies.csv')
#csv=df

#csv_files = glob.glob('../data\\*.csv')
#csv_files


# In[434]:


#Read csv
currencies=pd.read_csv('../data/currencies.csv')
flows=pd.read_csv('../data/flows.csv')
entity_names=pd.read_csv('../data/entity_names.csv')
exchange_rates=pd.read_csv('../data/exchange_rates.csv')
expimp_spegen=pd.read_csv('../data/expimp_spegen.csv')
RICentities=pd.read_csv('../data/RICentities.csv')
RICentities_groups=pd.read_csv('../data/RICentities_groups.csv')
sources=pd.read_csv('../data/sources.csv')


# #### Cruce de tablas obtenidas de Git

# In[435]:


entity_names_reporting=entity_names
entity_names_partner=entity_names


# In[436]:


entity_names_reporting=entity_names_reporting.rename(columns={'original_name': "reporting"})
#entity_names_reporting
entity_names_partner=entity_names_partner.rename(columns={'original_name': "partner"})
#entity_names_partner


# In[437]:


######################################################################################################
##################### Tablón que contenga reporting y partner con nombres en inglés###################
######################################################################################################

#Join1
df = pd.merge(flows, entity_names_reporting, how='left', on=["reporting"])
df=df.rename(columns={'french_name': "eliminar", 'RICname': "RICname_reporting" })

#Join2
df = pd.merge(df, entity_names_partner, how='left', on=["partner"])
df=df.rename(columns={'french_name': "eliminar2", 'RICname': "RICname_partner" })

df=df.drop(['eliminar', 'eliminar2'], axis=1)

################################################################################
##################### Tablón para convertir flujos a libras#####################
################################################################################

#Join exchangerate to currencies
df_currencies = pd.merge(currencies, exchange_rates[["rate_to_pounds","year","modified_currency"]], how='left', on=["year","modified_currency"])

#Join df_currencies to df
df= pd.merge(df, df_currencies, how='left', on=["currency","year","reporting"])
df.head(5)


# #### Limpieza 

# In[438]:


df['year'] = df["year"].astype(str)
df['export_import'] = df["export_import"].astype(str)

# Detectar llave única de la tabla
df=df.drop(['notes'], axis=1)
df=df.drop_duplicates()
df["flow2"] = df["flow"].astype(str)
df['key1'] =  df['reporting'] + df['partner'] + df['export_import'] + df['year'] + df['flow2']+ df['source']  

df["flow"] = df["flow"].astype(str)
df['flow'] = df.flow.replace(' ','')

df=df[~df['flow'].str.contains('-')]
df=df[~df['flow'].str.contains('revoir')]
df=df[df['flow']!='nan']
df=df[df['RICname_partner']!='***NA']
df=df[df['RICname_reporting']!='***NA']
df=df[(df['unit']==1)|(df['unit']==10)|(df['unit']==1000)|(df['unit']==100000)|(df['unit']==1000000)]
df["RICname_partner"] = df["RICname_partner"].astype(str)
df=df[~df['RICname_partner'].str.contains('World')]

df["flow"] = df["flow"].astype(float)

df['flujo_unit'] = (df.flow*df.unit)/df.rate_to_pounds   #Convertir flujos a libras.


# #### Creación de tablas para generar ranking

# In[439]:


#Filtro por type:países utilizando tabla RICentities 

df=df.rename(columns={'RICname_partner': "RICname"})
df3 = pd.merge(df, RICentities, how='left', on=["RICname"]) #RICentities contiene el grupo: Country, continent, etc.
df3=df3.rename(columns={'RICname': "RICname_partner"})
df3=df3[df3['type']=='country']  #Filtro solo países
# df3["type"].unique() #DF3 contiene latitud y longitud


# #### Creación de tabla con coordenadas

# In[440]:


lat_long=df3.groupby(['RICname_partner'],as_index=False).agg({'lat':['max'],'lng':['max']})
lat_long.columns=[x[0] if x[1]=='' else '_'.join(x) for x in lat_long.columns]
lat_long=lat_long.rename(columns={'lat_max': "lat", 'lng_max': "lng" })
# repetidos=lat_long.groupby('RICname_partner').size().loc[lambda x: x>1]


# #### Caluculo de distancia de cada país con las N potencias de comercio

# In[441]:


df4=df3.groupby(['RICname_reporting'],as_index=False).agg({'flujo_unit':['mean']})
df4.columns=[x[0] if x[1]=='' else '_'.join(x) for x in df4.columns]
df4=df4.sort_values(['flujo_unit_mean'],ascending=False)
df4=df4.iloc[0:8,:] # Tabla con N principales países según comercio
df4=df4.rename(columns={'RICname_reporting': "RICname_partner"})
df4=df4.reset_index(drop=True)
df4=df4.drop([3],axis=0)
df4


# In[442]:


#Obtener 8 potencias de comercio  para calcular distancia desde cada país a potencia.

## asignar latitud y longitud para paises de reporting
lat_log_repo=lat_long.rename(columns={'RICname_partner': "RICname_reporting",'lat': "lat_reporting",'lng': "lng_reporting"})
df_reporting=df[["RICname_reporting"]].drop_duplicates() # solo paises reporting
df5 = pd.merge(df_reporting, lat_log_repo, how='left', on=["RICname_reporting"])

## asignar latitud y longitud para paises 8 paises de partner
df6 = pd.merge(df4, lat_long, how='left', on=["RICname_partner"])
df6=df6.rename(columns={'lat': "lat_partner",'lng': "lng_partner"})

df5['key'] = 1
df6['key'] = 1
df7 = pd.merge(df5, df6, how='outer', on=["key"])
df7.drop(['key'], axis = 'columns', inplace=True)
df7  #agregar lat y long de Canadá manualmente. 
df7


# ## Cálculo de distancias Euclideanas

# ##### Cálculo de distancia euclidena promedio respecto a potencias

# In[443]:


#Calcular distancias euclideanas
import osmnx as ox

df7=df7[~df7["lat_partner"].isnull()]
df7["dist"]=ox.distance.euclidean_dist_vec(df7['lat_reporting'], df7['lng_reporting'], df7['lat_partner'], df7['lng_partner'])
# df7.iloc[265:275,:] # Ver casos(brazil)
df7 #Tabla que contiene distancia para N pares

df8=df7.groupby(['RICname_reporting'],as_index=False).agg({'dist':['mean']})
df8.columns=[x[0] if x[1]=='' else '_'.join(x) for x in df8.columns]
df8 # Tabla que contiene distancias euclideana promedio


# ##### Cálculo de distancia euclidena promedio respecto a potencia, luego de ponderar cada distancia por el flujo del país con la potencia.

# In[444]:


# Obtengo flujo de pares
df9=df3
df9=df9.rename(columns={'RICname': "RICname_partner"})
pares_flow=df9.groupby(['RICname_reporting','RICname_partner'],as_index=False).agg({'flujo_unit':['mean']})
pares_flow.columns=[x[0] if x[1]=='' else '_'.join(x) for x in pares_flow.columns]
pares_flow=pares_flow.rename(columns={'flujo_unit_mean': "flujo_partner"})
pares_flow['key'] = pares_flow['RICname_reporting']+pares_flow['RICname_partner']
pares_flow.drop(['RICname_reporting', 'RICname_partner'], axis = 'columns', inplace=True)
pares_flow


# In[445]:


df7['key'] = df7['RICname_reporting']+df7['RICname_partner'] #creo llave
flujo_dist= pd.merge(df7, pares_flow, how='left', on=["key"]) 
flujo_dist.drop(['key', 'lat_reporting',"lng_reporting", "lat_partner", "lng_partner"], axis = 'columns', inplace=True)
flujo_dist['flujo_dist'] = flujo_dist['dist']*flujo_dist['flujo_partner']

flujo_dist_mean=flujo_dist.groupby(['RICname_reporting'],as_index=False).agg({'flujo_dist':['mean']})
flujo_dist_mean.columns=[x[0] if x[1]=='' else '_'.join(x) for x in flujo_dist_mean.columns]
flujo_dist_mean=flujo_dist_mean.rename(columns={'flujo_dist_mean': "flujo*dist_mean" })
flujo_dist_mean


# ##### Cálculo de distancia euclidena promedio respecto a potencias (en este caso P=7), luego de ponderar cada distancia por: el flujo del país "i" con la potencia "p" y por el peso del flujo de cada país con respecto a su flujo total: 
# #### $\bar{D}(d(x,y),flujo,W)_{i, mean}=\frac{ \sum_{p=1}^{P=7} d(x,y)_{i,p}*flujo_{i,p}*\frac{Flujo_{i,p}}{Flujo Total_{i}}}{P}$
# 

# In[446]:


#Para construir W:
df_w=df3.groupby(['RICname_reporting'],as_index=False).agg({'flujo_unit':['sum']})
df_w.columns=[x[0] if x[1]=='' else '_'.join(x) for x in df_w.columns]


# In[447]:


pares_flow_sum=df9.groupby(['RICname_reporting','RICname_partner'],as_index=False).agg({'flujo_unit':['sum']})
pares_flow_sum.columns=[x[0] if x[1]=='' else '_'.join(x) for x in pares_flow_sum.columns]
pares_flow_sum=pares_flow_sum.rename(columns={'flujo_unit_sum': "flujo_partner"})
pares_flow_sum['key'] = pares_flow_sum['RICname_reporting']+pares_flow_sum['RICname_partner']
pares_flow_sum.drop(['RICname_reporting', 'RICname_partner'], axis = 'columns', inplace=True)


# In[448]:


df_w['key'] = 1
df4['key'] = 1
df10 = pd.merge(df_w, df4, how='outer', on=["key"])
df10.drop(['key','flujo_unit_mean'], axis = 'columns', inplace=True)

df10['key'] = df10['RICname_reporting']+df10['RICname_partner']
df10=pd.merge(df10, pares_flow_sum, how='left', on=["key"])
df10['weigth']=df10['flujo_partner']/df10['flujo_unit_sum']
df10=df10[['RICname_reporting','RICname_partner','weigth']]

weigth2 = pd.merge(flujo_dist, df10, how='left', on=["RICname_reporting",'RICname_partner'])
weigth2['flujo*dist_mean*w']=weigth2['dist']*weigth2['flujo_partner']*weigth2['weigth']


# In[449]:


df11=weigth2.groupby(['RICname_reporting'],as_index=False).agg({'flujo*dist_mean*w':['mean']})
df11.columns=[x[0] if x[1]=='' else '_'.join(x) for x in df11.columns]


# In[459]:


df_final=pd.merge(df8, flujo_dist_mean, how='left', on=["RICname_reporting"])
df_final2=pd.merge(df_final, df11, how='left', on=["RICname_reporting"])
df_final2=df_final2[~df_final2["dist_mean"].isnull()]


# In[460]:


df_final2.to_csv(r'C:\Users\Joyas\Desktop\github\IV.csv',index = False, sep=';', decimal=',')


# In[458]:




