import boto3
import pandas as pd
from io import StringIO
import psycopg2
import os
import logging

def create_connection():
    dbname=os.environ['DB_NAME']
    user=os.environ['DB_USER']
    password=os.environ['DB_PASSWORD']
    host=os.environ['DB_HOST']
    port=os.environ['DB_PORT']
        
    conn = psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT']
    )

    if not conn.closed:
            logging.info("Connection established successfully.")
    else:
        return {
        'statusCode': 404,
        'body': 'Failed to establish connection.'}
    
    return conn

def delete_csv(bucket, key, file_names, index):
    # create S3 client
    s3 = boto3.client('s3')
    
    if(file_names != None):
        for f in file_names:
            s3.delete_object(Bucket=bucket, Key=key+f)
            
    if(index != None):
        for i in range(1, index):
            s3.delete_object(Bucket=bucket, Key=key+f'{i}.csv')
        
    logging.info("deleted the files")
    return    

def transform_data(bucket):
    # create S3 client
    s3 = boto3.client('s3')
    
    # list all objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket, Prefix='raw_layer/')
    
    # extract the list of file names
    file_names = []
    for obj in response['Contents']:
        if obj['Key'].startswith('raw_layer/'):
            f_name = obj['Key'].split('/')[-1]
            if(f_name != ''):
                file_names.append(f_name)
            
    
    index = 1
    for f in file_names:
        obj = s3.get_object(Bucket=bucket, Key='raw_layer/'+f)
        file_content = obj['Body'].read().decode('utf-8')
        # create a DataFrame from the list of file contents
        df = pd.read_csv(StringIO(file_content))
    
        # Add a new column "CURRENCY" with value "USD"
        df['CURRENCY'] = 'USD'

        # Remove unwanted characters from the PRICE column and convert it to numeric
        df['PRICE'] = pd.to_numeric(df['PRICE'].str.replace('$', '').str.replace(',', '').str.replace(' ', ''), errors='coerce')
        df['PERCENT_CHANGE_24H'] = pd.to_numeric(df['PERCENT_CHANGE_24H'].str.replace('%', '').str.replace(',', '').str.replace(' ', ''), errors='coerce')
        df['VOLUME_24H'] = pd.to_numeric(df['VOLUME_24H'].str.replace('$', '').str.replace('B', 'E9').str.replace('M', 'E6').str.replace(',', '').str.replace(' ', ''), errors='coerce')
        df['MARKET_CAP'] = pd.to_numeric(df['MARKET_CAP'].str.replace('$', '').str.replace('B', 'E9').str.replace('M', 'E6').str.replace(',', '').str.replace(' ', ''), errors='coerce')

        df['SYSTEM_INSERTED_TIMESTAMP'] = pd.to_datetime(df['SYSTEM_INSERTED_TIMESTAMP'])
        
        # Write dataframe to CSV file in memory
        csv_buffer = pd.DataFrame.to_csv(df, index=False)
        
        # create a StringIO object from the CSV string
        csv_file = StringIO(csv_buffer)
        
        s3.put_object(Bucket=bucket, Key=f'transformation_layer/{index}.csv', Body=csv_buffer)
        index += 1
        
    delete_csv(bucket,'raw_layer/',file_names,None)    
    return index

def load_data(bucket, conn, index):
    cur = conn.cursor()
    
    access_key=os.environ['Access_key']
    access_secret=os.environ['Secret_access_key']
    
    for i in range(1, index):
        from_path = "s3://{}/transformation_layer/{}.csv".format(bucket, i)
        querry = "COPY {} FROM '{}' CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV DELIMITER ',' IGNOREHEADER 1;".format("public.top_crypto_details",from_path,access_key,access_secret)
        cur.execute(querry)
        conn.commit()
    logging.info("load successfull")
    
    delete_csv(bucket,'transformation_layer/',None,index)
    
    return

def lambda_handler(event, context):
    
    # set bucket name
    bucket = 'coinmarketcap-bucket'
    
    index = transform_data(bucket)

    conn = create_connection()
    
    load_data(bucket, conn, index)
    
    
    conn.close()
    
    if conn.closed:
        logging.info("Connection closed successfully.")
    else:
        return {
        'statusCode': 404,
        'body': 'Failed to close connection.'}
    
        
    # return the list of file names
    return {
        'statusCode': 200,
        'body': 'it worked! Data loaded in redshift'}
