# AWS related functions

import re
import time

import boto3
import pandas as pd
import psycopg2

## defining variables
LOG_GROUP='engDados-carga-csv'
LOG_STREAM='carga-tabelas-excel'

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

def load_csv_files(file_path):
    log_it('starting load routine')
    file_type = file_path.split('.')[-1]
    try:
        if file_type == 'csv':
            df = pd.read_csv(file_path)
            log_it('file ' + file_path + ' was loaded successfully!\n Size: ' + str(df.shape))
        elif file_type == 'xlsx':
            df = pd.read_excel(file_path)  
            log_it('file ' + file_path + ' was loaded successfully!\n Size: ' + str(df.shape))
        else:
            log_it(file_type + ' file type is incompatible with this routine', 'ERROR')
        return df
    except FileNotFoundError:
        log_it('the file given does not exist', 'ERROR')
        raise
    except Exception as e:
        log_it('an unexpected error was given \t' + str(e), 'ERROR')
        raise
#%%
def validate_missing(field_value):
    print(field_value)
    type(field_value)
    if pd.isna(field_value):
        raise ValueError('this information in null')
    else:
        return field_value

def validate_string(regex_string, value):
    print('validating regex', value)
    print(re.findall(regex_string, value))
    if len(re.findall(regex_string, value)) != 1:
        print('value error!')
        raise ValueError('error on regex, \tvalue:'+ value +'\tpattern: '+ regex_string)
    else:
        return value
#%%
def insert_row(df_row, db_conn, db_table, row_num):
    try:
        if db_table == 'worker':
            insert_sql = ('INSERT INTO funcionarios(nome,especializacao,data_contratacao,telefone,endereco,cpf,nascimento) VALUES (%s,%s,%s,%s,%s,%s,%s)'
            print(insert_sql)
            cur = db_conn.cursor()
            cur.execute(insert_sql,(
                df_row['Nome'],
                validate_missing(df_row['Especialização']),
                validate_missing(df_row['Contratação']),
                validate_string('(\+\d{2} \d{2} \d{4}[ -]\d{4}|\+\d{2} \(\d{3}\) \d{4}[ -]\d{4}|\d{2} \d{4}[ -]\d{4}|\(\d{3}\) \d{4}[ -]\d{4})',validate_missing(df_row['Telefone'])),
                validate_missing(df_row['Endereço']),
                validate_string('(\d{3}.\d{3}.\d{3}-\d{2})',validate_missing(df_row['CPF'])),
                validate_string('(\d{2}/\d{2}/\d{4})',validate_missing(df_row['Nascimento']))
            ))
            conn.commit()
            cur.close() 
        elif db_table == 'patient':
            insert_sql =   'INSERT INTO pacientes(nome,telefone,endereco,convenio,cpf,nascimento) VALUES (%s,%s,%s,%s,%s,%s)'
            print(insert_sql)
            cur = db_conn.cursor()
            cur.execute(insert_sql, (
                df_row['Nome'],
                validate_string('(\+\d{2} \d{2} \d{4}[ -]\d{4}|\+\d{2} \(\d{3}\) \d{4}[ -]\d{4}|\d{2} \d{4}[ -]\d{4}|\(\d{3}\) \d{4}[ -]\d{4})',validate_missing(df_row['Telefone'])),
                validate_missing(df_row['Endereço']),
                validate_missing(df_row['Convênio']),
                validate_string('(\d{3}.\d{3}.\d{3}-\d{2})',validate_missing(df_row['CPF'])),
                validate_string('(\d{2}/\d{2}/\d{4})',validate_missing(df_row['Nascimento']))
            ))
            conn.commit()
            cur.close()
        elif db_table == 'appointment':
            #searching for the patient
            cur = db_conn.cursor()
            patient_search = "select id from pacientes where nome = %s"
            cur.execute(patient_search,(df_row['Paciente'],))
            patient = cur.fetchall()
            if len(patient) != 1:
                raise ValueError('data inconsistency, number of rows found:' + str(len(patient)))
            worker_search = "select id from funcionarios where nome = %s"
            cur.execute(worker_search,(df_row['Médico'],))
            worker = cur.fetchall()
            if len(worker) != 1:
                raise ValueError('data inconsistency, number of rows found:' + str(len(worker)))
            insert_sql =   'INSERT INTO consultas(paciente,funcionario,sala,agendado,data_hora,retorno) VALUES (%s,%s,%s,%s,%s,%s)'
            cur.execute(insert_sql, (
                str(patient[0][0]),
                str(worker[0][0]),
                str(validate_missing(df_row['Sala'])),
                validate_string('(Sim|Não)',validate_missing(df_row['Agendado(S/N)'])),
                validate_string('(\d{2}/\d{2}/\d{4}, \d{2}:\d{2}:\d{2})',validate_missing(df_row['Data'])),
                validate_string('(Sim|Não)',validate_missing(df_row['Retorno(S/N)']))
            ))
            conn.commit()
            cur.close()
    except ValueError as e:
        error = 'error in row_num: ' + str(row_num) + ':\t' + str(e) + '\t' + 'table:' + db_table
        log_it(error, 'ERROR')
    except psycopg2.errors.InFailedSqlTransaction as e:
        log_it('connection failed: '+str(e), 'ERROR')
        db_conn.rollback()
    except Exception as e:
        raise
        log_it(str(e), 'ERROR')
#%%

conn = psycopg2.connect(host='localhost', database='clinica',user='psqladmin', password='m3uCurrR$0o')

#Routine for workers
file_path = r"C:\Users\hugom\OneDrive\Alura Curso Online\Engenharia de dados I\Ready Data\workers.csv"
df = load_csv_files(file_path)
for index,row in df.iterrows():
    insert_row(row, conn, 'worker', index)

#Routine for patients
file_path = r"C:\Users\hugom\OneDrive\Alura Curso Online\Engenharia de dados I\Ready Data\patients-XL.xlsx"
df = load_csv_files(file_path)
for index,row in df.iterrows():
    insert_row(row, conn, 'patient', index)

#Routine for appointments
file_path = r"C:\Users\hugom\OneDrive\Alura Curso Online\Engenharia de dados I\Ready Data\appointments.csv"
df = load_csv_files(file_path)
for index,row in df.iterrows():
    insert_row(row, conn, 'appointment', index)



cur = conn.cursor()
cur.execute('select * from funcionarios')
cur.fetchall()
conn.commit()
conn.rollback()
conn.close()

# %%
file_path = r"C:\Users\hugom\OneDrive\Alura Curso Online\Engenharia de dados I\Ready Data\appointments-XL.xlsx"