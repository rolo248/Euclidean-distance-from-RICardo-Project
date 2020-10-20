#!/usr/bin/env python
# coding: utf-8

# python script to build sqlite for visualization
# coding=utf8
import subprocess
import sqlite3
import os
import json
import itertools
import utils
import re
import custom_exports
import shutil
import pandas as pd
import glob
import matplotlib.pyplot as plt
import numpy as np

#Set working: C:\Users\Joyas\Desktop\github\ricardo_data\database_scripts


#Antes de leer csv, se deben compilar las tablas utilizando códigos disponible en:


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

# In[54]:


entity_names_reporting=entity_names
entity_names_partner=entity_names


# In[55]:


entity_names_reporting=entity_names_reporting.rename(columns={'original_name': "reporting"})
#entity_names_reporting
entity_names_partner=entity_names_partner.rename(columns={'original_name': "partner"})
#entity_names_partner


# In[56]:


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

# In[12]:


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

# In[48]:


#Filtro por type:países utilizando tabla RICentities 
df=df.rename(columns={'RICname_partner': "RICname"})
df3 = pd.merge(df, RICentities, how='left', on=["RICname"]) #RICentities contiene el grupo: Country, continent, etc.
df3=df3.rename(columns={'RICname': "RICname_partner"})
df3=df3[df3['type']=='country'] #Filtro solo países
df3.head(5) #DF3 contiene latitud y longitud


# In[47]:


#Calcular flujo promedio (import & export) del periodo para cada par: Reporting-Partner.
df4=df3.groupby(['RICname_reporting','RICname_partner'],as_index=False).agg({'flujo_unit':['mean']})
df4.columns=[x[0] if x[1]=='' else '_'.join(x) for x in df4.columns] #Arreglar nombre de columnas
df4=df4.sort_values(['RICname_reporting','flujo_unit_mean'],ascending=False) #Orden descendente
df4.head(5)


# In[36]:


#Para quedarme con las 5 primeras tuplas con mayor flujo
df5 = df4.groupby(['RICname_reporting']).apply(lambda x: x.head(5)) 
df5 = df5.reset_index(drop=True)
df5.head(10) #DF5 contiene el flujo promedio


# #### Creación de tabla con coordenadas

# In[34]:


# Obtengo latitud y longitud del Partner desde DF3
lat_long=df3.groupby(['RICname_partner'],as_index=False).agg({'lat':['max'],'lng':['max']})
lat_long.columns=[x[0] if x[1]=='' else '_'.join(x) for x in lat_long.columns] #Arreglar nombre de columnas
lat_long=lat_long.rename(columns={'lat_max': "lat", 'lng_max': "lng" })
lat_long.head(10)


# In[42]:


# Obtengo latitud y longitud para los Reporting desde "lat_long" a "df5"
lat_long=lat_long.rename(columns={'RICname_partner': "RICname_reporting"})
df6 = pd.merge(df5, lat_long, how='left', on=["RICname_reporting"])
df6=df6.rename(columns={'lat': "lat_reporting",'lng':'lng_reporting'})
df6.iloc[10:20,:]


# In[46]:


# Obtengo latitud y longitud para los Partner desde "lat_long" a "df6"
lat_long=lat_long.rename(columns={'RICname_reporting': "RICname_partner"})
df7= pd.merge(df6, lat_long, how='left', on=["RICname_partner"])
df7=df7.rename(columns={'lat': "lat_partner",'lng':'lng_partner'})
df7.iloc[7:20,:]


# #### Calculo de distancia de cada país con sus 5 principales socios comerciales

# In[45]:


import osmnx as ox

df7=df7[~df7["lat_reporting"].isnull()]
df7["dist"]=ox.distance.euclidean_dist_vec(df7['lat_reporting'], df7['lng_reporting'], df7['lat_partner'], df7['lng_partner'])
df8=df7.groupby(['RICname_reporting'],as_index=False).agg({'dist':['mean','sum']})
df8.columns=[x[0] if x[1]=='' else '_'.join(x) for x in df8.columns]
df8


# #### Caluculo de distancia de cada país con los 5 principales 

# In[75]:


df4=df3.groupby(['RICname_reporting'],as_index=False).agg({'flow':['mean']})
df4.columns=[x[0] if x[1]=='' else '_'.join(x) for x in df4.columns]
df4=df4.sort_values(['flow_mean'],ascending=False)
df4.head(5)


# In[321]:


#len(df),df["key1"].count()
# df_1=df.drop_duplicates(['key0'], keep='last')
# repetidos=df.groupby('key2').size().loc[lambda x: x>1]

