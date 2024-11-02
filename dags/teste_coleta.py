import boto3
import os


AWS_ACCESS_KEY_ID = 'AKIAZI2LDILVXCJPTPHI'
AWS_SECRET_ACCESS_KEY = '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'
AWS_REGION = 'sa-east-1' 

# Nome do bucket e o caminho do arquivo
bucket_name = 'bucketestudosengdados'
file_key = 'mes/dia/hora/13/clientes.csv'
local_file_path = 'clientes.csv' 

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

s3 = session.client('s3')

# Faz o download do arquivo
try:
    s3.download_file(bucket_name, file_key, local_file_path)
    print(f"Arquivo {local_file_path} baixado com sucesso.")
except Exception as e:
    print(f"Erro ao baixar o arquivo: {e}")

os.remove(local_file_path)
