import re
import time
import os
import boto3
import pandas as pd
import psycopg2
from datetime import datetime
import uuid
import json

LOG_GROUP='engDados-carga-csv'
LOG_STREAM='carga-tabelas-imagens'
DYNAMO_DB_TABLE='engDados-image-register'
S3_BUCKET='curso-eng-dados'

def log_it(message, type = 'INFO', cw_client = boto3.client('logs')):
    timestamp  = int(round(time.time() * 1000))
    token = cw_client.describe_log_streams(
        logGroupName=LOG_GROUP,
        logStreamNamePrefix=LOG_STREAM,
    )['logStreams'][0]['uploadSequenceToken']

    response = cw_client.put_log_events(
        logGroupName=LOG_GROUP,
        logStreamName=LOG_STREAM,
        logEvents=[
            {
                'timestamp': timestamp,
                'message': time.strftime('%Y-%m-%d %H:%M:%S') + '\t['+type+']\t'+ message
            }
        ],
        sequenceToken=token
    )
    print(time.strftime('%Y-%m-%d %H:%M:%S') + '\t['+type+']\t'+ message)

def list_images_from_dir(base_path: str = '.'):
    return [{'absolute':os.path.abspath(x), 'name':x.replace('.jpeg', ''), 'file':x} for x in os.listdir(base_path)]

def break_image_as_dict(image: dict):
    attributes = image['name'].split('_')
    result = {}
    for attribute in attributes:
        [name,  value] = attribute.split('=')
        result[name] = value
    return result

def relate_to_db_data(img_dict: dict):
    conn = psycopg2.connect(host='localhost', database='clinica',user='psqladmin', password='m3uCurrR$0o')
    query = '''select consultas.id
               from consultas,
                    funcionarios,
                    pacientes
               where consultas.funcionario = funcionarios.id
               and   consultas.paciente = pacientes.id
               and   funcionarios.nome = %s
               and   pacientes.nome = %s
            '''
    cur = conn.cursor()        
    cur.execute(query,(img_dict['funcionario'],img_dict['paciente']))
    appointment = cur.fetchall()
    print(appointment)
    return json.dumps(appointment)

def write_on_dynamo(item: dict):
    dynamodb = boto3.resource('dynamodb').Table(DYNAMO_DB_TABLE)
    return dynamodb.put_item(
        TableName = DYNAMO_DB_TABLE,
        Item = item
    )

def upload_to_s3(path: dict, ref_date: datetime, uuid:str):
    client = boto3.client('s3')
    response =  client.upload_file(path, S3_BUCKET, str(ref_date.year)+'/'+str(ref_date.month)+'/'+uuid)
    print(response)
    return response

file_path = r"C:\Users\hugom\OneDrive\Alura Curso Online\Engenharia de dados I\Ready Data\Imagens_Curso"
images = list_images_from_dir(file_path)
log_it('starting routine to transfer images')
for image in images:
    img_dict = break_image_as_dict(image)
    img_dict['uuid'] = str(uuid.uuid1())
    img_dict['db_relation'] = relate_to_db_data(img_dict)
    write_on_dynamo(img_dict)
    upload_to_s3(file_path+'\\'+image['file'],datetime.strptime(img_dict['data'],'%m-%d-%Y %H.%M'),img_dict['uuid'])

open(image['absolute'])


    