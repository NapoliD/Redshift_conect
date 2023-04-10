
import boto3
import psycopg2


client = boto3.client('redshift',region_name = '',aws_access_key_id = ',aws_secret_access_key = '')
cluster_creds = client.get_cluster_credentials(DbUser='',DbName='',ClusterIdentifier='',AutoCreate=False)


print('connect psycopg2')
con = psycopg2.connect(host='redshift-.redshift.amazonaws.com',port='0000',user=cluster_creds['DbUser'],password=cluster_creds['DbPassword'],database='base')
print(con)
cursor=con.cursor()


print('copy to redshift')

sql_COPY_query = f"""COPY  FROM 's3://bucket/.csv' IAM_ROLE 'arn:aws:iam::' DELIMITER '|' IGNOREHEADER as 1 CSV;"""
print(sql_COPY_query)
cursor.execute(sql_COPY_query)
con.commit()
con.close()
print('ok sql_COPY_query')
