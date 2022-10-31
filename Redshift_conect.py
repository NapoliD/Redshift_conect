
import awswrangler as wr
import pandas as pd
import boto3
import psycopg2
import funciones
import io

from datetime import datetime, timedelta
ayer=datetime.now() - timedelta(days = 15)

ayer=ayer.strftime("%Y-%m-%d")
#ayer=f'{ayer}'

print(ayer)


def lambda_handler(event, context):
    
    #conexion_redshift
    cluster_creds=funciones.conexion_redshift()
    print(cluster_creds)
    
    
    con = psycopg2.connect(host='',port='',user=cluster_creds['DbUser'],password=cluster_creds['DbPassword'],database='')
    print(con)
    cursor=con.cursor()


    sql_Delete_query = f""" """
    print()
    cursor.execute()
    con.commit()

    print('read_sql')

    #print(cursor)
    datos=[]
    contador=0
    cursor=con.cursor()
    for i,row in dataset.iterrows():

        datos.append(tuple(row))

    stmt = "INSERT INTO schema.table VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.executemany(stmt, datos)
    con.commit()

    print('all good')

    
    
    
