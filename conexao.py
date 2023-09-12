# Importando as bibliotecas
import pandas as pd
import psycopg2 as ps
from sqlalchemy import create_engine
import gspread as gs
import getpass


# Arquivo JASON com as credenciais para conexão com o API do googlesheet
gc = gs.service_account(filename="D:\Documentos\Documents\Acp - PBI\chave.json")

# Abrindo a planilha do googlesheets
dados = gc.open_by_url("https://docs.google.com/spreadsheets/d/16jhJ7M97Ldf4DzIHCM698uNdzLGG0ApblhOH-hhpMFQ/edit#gid=0").\
worksheet("Página1")

# Obtendo os dados
obter_dados = dados.get_all_values()

print("Dados obtidos!")

# Obtendo os dados somente das colunas(nome)
colunas = dados.get_all_values().pop(0)

# Passando os dados obtidos para um DataFrame
df = pd.DataFrame(dados.get_all_values(), columns= colunas).drop(index=0).reset_index(drop=True)

# Convertendo os tipos de dados
df['LUCRO'] = df['LUCRO'].str.replace(',', '.').astype(float)
df['Valor_Venda'] = df['Valor_Venda'].str.replace(',', '.').astype(float)
df['Preço_Custo'] = df['Preço_Custo'].str.replace(',', '.').astype(float)
df['Data'] = pd.to_datetime(df['Data'], format="%d/%m/%Y")

print("Tipo de dados convertidos!")

# Solicite as informações de conexão ao usuário
user = input("Digite o nome de usuário: ")
password = getpass.getpass("Digite a senha: ")
host = input("Digite o host: ")
port = input("Digite a porta: ")
database = input("Digite o nome do banco de dados: ")

# Crie a conexão SQLAlchemy com base nas informações fornecidas pelo usuário
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

# Defina sua consulta SQL para obter dados do banco de dados
sql = 'SELECT * FROM vendas_acp'

# Leia os dados do banco de dados para um DataFrame
dff = pd.read_sql_query(sql, con=engine)

# Verifique se o número de linhas no DataFrame 'df' mudou em relação ao DataFrame 'dff'
if df.shape[0] > dff.shape[0]:
    # Algumas linhas foram adicionadas, atualize o banco de dados
    df.to_sql('vendas_acp', con=engine, index=False, if_exists='replace')
    print('Novas linhas adicionadas', df.shape)
elif df.shape[0] == dff.shape[0]:
    # O banco de dados está atualizado, não há alterações a serem feitas
    print('Banco Atualizado!', df.shape)

# Feche a conexão com o banco de dados
engine.dispose()