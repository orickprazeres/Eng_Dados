# -*- coding: utf-8 -*-
"""
Created on Mon Jun 15 22:56:16 2020

@author: Hugo
"""
#%%
#Creating variable pools
health_plans = ['Plano AAA','Plano BBB','Plano CCC', 'Plano DDD', 'Plano EEE', 'Particular']
#https://www.educamaisbrasil.com.br/educacao/carreira/saiba-quais-sao-as-especializacoes-da-medicina-e-suas-funcoes
medical_fields = ['Acupuntura', 'Alergia e imunologia', 'Cardiologia', 'Cirurgia geral', 'Dermatologia', 'Endoscopia', 'Medicina do trabalho', 'Neurologia', 'Oftalmologia', 'Pediatria', 'Radioterapia']
#%%
number_pacient = 2000
number_worker = 2000
number_appointment = 1500
#%%
def generate_cpf():
    return str(random.randrange(100,999,1))+'.'+str(random.randrange(100,999,1))+'.'+str(random.randrange(100,999,1))+'-'+str(random.randrange(10,99,1))

def corruption_missing_data(percent, dataset, fields):
    number_of_corruptions = round(float(dataset.size)*percent)
    for run in range(0,number_of_corruptions):
        dataset[random.choice(fields)][random.randrange(0,len(dataset))] = ''
    
    return dataset

def corruption_bad_str_format(percent, dataset, fields):
    #Get string like CPF, S/N fields and Dates and changes them
    number_of_corruptions = round(float(dataset.size)*percent)
    for run in range(0,number_of_corruptions):
        column = random.choice(fields)
        row = random.randrange(0,len(dataset))
        if dataset[column][row] == 'Sim':
            dataset[column][row] = dataset[column][row].replace('Sim','sim')
        elif dataset[column][row] == 'Não':
            dataset[column][row] =  dataset[column][row].replace('Não','nao')
        else:
            dataset[column][row] = dataset[column][row].replace('.','-').replace('/','-').replace(':','-')
    
    return dataset

def corruption_bad_str_typo(percent, dataset, fields):
    #Get string like CPF, S/N fields and Dates and changes them
    number_of_corruptions = round(float(dataset.size)*percent)
    for run in range(0,number_of_corruptions):
        column = random.choice(fields)
        row = random.randrange(0,len(dataset))
        length = len(dataset[column][row])
        print('before',dataset[column][row])
        dataset[column][row] = dataset[column][row].replace(dataset[column][row][random.randrange(0,length)], chr(ord(dataset[column][row][random.randrange(0,length)]) + random.randrange(0,5)))
        print('after',dataset[column][row])
    return dataset

#%%
import faker
import pandas as pd
import random
#tables' metadata
tb_pacients_columns = ['Nome','Telefone','Endereço','Convênio','CPF', 'Nascimento']
tb_clinic_workers_columns = ['Nome', 'Especialização', 'Contratação', 'Telefone', 'Endereço', 'CPF', 'Nascimento']
tb_appointment_columns = ['Paciente', 'Médico', 'Sala', 'Agendado(S/N)', 'Data', 'Retorno(S/N)']
#%%
fake = faker.Faker(['pt_BR'])
#declaring and filling tables
data_pacients_list = [[fake.name(), fake.phone_number(), fake.address(), random.choice(health_plans), generate_cpf(), fake.date_time_between(start_date='-30y', end_date='now').strftime("%m/%d/%Y")] for pacient in range(0,number_pacient)]
data_workers_list = [[fake.name(), random.choice(medical_fields), fake.date_time_between(start_date='-10y', end_date='now').strftime("%m/%d/%Y"), fake.phone_number(), fake.address(), generate_cpf(), fake.date_time_between(start_date='-50y', end_date='-20y').strftime("%m/%d/%Y")] for worker in range(0,number_worker)]
df_patients = pd.DataFrame(data_pacients_list, columns=tb_pacients_columns)
df_workers = pd.DataFrame(data_workers_list, columns=tb_clinic_workers_columns)

data_appointments = [[random.choice(df_patients['Nome']), random.choice(df_workers['Nome']), random.randrange(1,100,1), random.choice(['Sim', 'Não']), fake.date_time_between(start_date='-30y', end_date='now').strftime("%m/%d/%Y, %H:%M:%S"),random.choice(['Sim', 'Não'])] for appointment in range (0,number_appointment)]
df_appointments = pd.DataFrame(data_appointments, columns=tb_appointment_columns)
#%%
corruption_missing_percent = 0.005
corruption_bad_str_percent = 0.008
corruption_typo_percent = 0.01
#%
print('corrupting files')
dfcorrupted_pacients = corruption_missing_data(corruption_missing_percent, df_patients, ['Telefone','Endereço','Convênio','CPF', 'Nascimento'])
dfcorrupted_pacients = corruption_bad_str_format(corruption_bad_str_percent, dfcorrupted_pacients, ['CPF', 'Nascimento'])
print('corrupted pacients')
dfcorrupted_workers = corruption_missing_data(corruption_missing_percent, df_workers, ['Especialização', 'Contratação', 'Telefone', 'Endereço', 'CPF', 'Nascimento'])
dfcorrupted_workers = corruption_bad_str_format(corruption_bad_str_percent, dfcorrupted_workers, ['Telefone', 'Endereço', 'CPF', 'Nascimento'])
print('corrupted workers')
dfcorrupted_appointments = corruption_bad_str_format(corruption_bad_str_percent, df_appointments, ['Agendado(S/N)', 'Data', 'Retorno(S/N)'])
dfcorrupted_appointments = corruption_bad_str_typo(corruption_typo_percent, dfcorrupted_appointments, ['Paciente', 'Médico'])
#%%
base_path = r'C:\Users\hugom\OneDrive\Alura Curso Online\Engenharia de dados I\Ready Data'
dfcorrupted_pacients.to_csv( base_path+'\\pacients.csv', index = False)
dfcorrupted_workers.to_csv( base_path+'\\workers.csv', index = False)
dfcorrupted_appointments.to_csv( base_path+'\\appointments.csv', index = False)

dfcorrupted_pacients.to_excel( base_path+'\\pacients-XL.xlsx', index = False)
dfcorrupted_workers.to_excel( base_path+'\\workers-XL.xlsx', index = False)
dfcorrupted_appointments.to_excel( base_path+'\\appointments-XL.xlsx', index = False)
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
df_pacient_charges = pd.read_csv(r'C:\Users\hugom\OneDrive\Alura Curso Online\Engenharia de dados I\Datasets\inpatientCharges.csv')
df_pacient_charges[' Average Covered Charges '] = df_pacient_charges[' Average Covered Charges '].str.replace('$','')
df_pacient_charges[' Average Total Payments '] = df_pacient_charges[' Average Total Payments '].str.replace('$','')
df_pacient_charges['Average Medicare Payments'] = df_pacient_charges['Average Medicare Payments'].str.replace('$','')
df_diagnosis = pd.read_csv(r'C:\Users\hugom\OneDrive\Alura Curso Online\Engenharia de dados I\Datasets\datasets_180_408_data.csv')
df_diagnosis.drop(df_diagnosis.columns[len(df_diagnosis.columns)-1], axis=1, inplace=True)
#%%
from sqlalchemy import create_engine
import io
import psycopg2
conn = psycopg2.connect(host='localhost', database='clinica',
user='admin', password='admin')

def upload_data(conn, df, table_name, columns):
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header = False, index = False)
    output.seek(0)
    try:
        cur.copy_from(output, table_name, null = "", columns = columns)
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()

upload_data(conn, df_pacient_charges, 'cobranca_paciente', ('definicao', 
                                                            'identificacao',
                                                            'nome', 
                                                            'endereco',
                                                            'cidade',
                                                            'estado',
                                                            'codigo_postal',
                                                            'regiao',
                                                            'total_cobrancas',
                                                            'media_custos_cobertos',
                                                            'media_pagamento_total',
                                                            'media_gastos_cuidados'))

upload_data(conn, df_diagnosis, 'dados_analises', ('id',
                                                    'diagnostico',
                                                    'media_raio',
                                                    'media_textura',
                                                    'media_perimetro',
                                                    'media_area',
                                                    'media_suavidade',
                                                    'media_compactacao',
                                                    'media_concavidade',
                                                    'media_concavidade_pontos',
                                                    'media_simetria',
                                                    'media_dimensao_fractal',
                                                    'se_raio',
                                                    'se_textura',
                                                    'se_perimetro',
                                                    'se_area',
                                                    'se_suavidade',
                                                    'se_compactacao',
                                                    'se_concavidade',
                                                    'se_concavidade_pontos',
                                                    'se_simetria',
                                                    'se_dimensao_fractal',
                                                    'pior_raio',
                                                    'pior_textura',
                                                    'pior_perimetro',
                                                    'pior_area',
                                                    'pior_suavidade',
                                                    'pior_compactacao',
                                                    'pior_concavidade',
                                                    'pior_concavidade_pontos',
                                                    'pior_simetria',
                                                    'pior_dimensao_fractal'))

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#%renaming image files
from os import listdir
import shutil
path = r'C:\Users\hugom\OneDrive\Alura Curso Online\Engenharia de dados I\Datasets\SAMPLES_ALL'
files = listdir(path)

destination_path = r'C:\Users\hugom\OneDrive\Alura Curso Online\Engenharia de dados I\Ready Data\Imagens_Curso'

for file in files:
    appointment = dfcorrupted_appointments.iloc[random.randrange(0,len(dfcorrupted_appointments)-1)]    
    file_name = 'paciente=' + appointment['Paciente'] + '_funcionario=' + appointment['Médico'] + '_data=' + fake.date_time_between(start_date='-3y', end_date='now').strftime("%m-%d-%Y %H.%M") + '_atributo' + str(random.randrange(1,50,1)) + '=' + fake.sentence()
    shutil.copy(path+'\\'+file,destination_path+'\\'+file_name+'jpeg')
    




# %%
