import pymysql
import schedule
import time
from datetime import datetime

# extrair e salvar
def exportar_para_sql():
    # Dados de conexão com o banco de dados remoto (origem dos dados)
    host_origem = 'seu_host_origem'
    usuario_origem = 'seu_usuario_origem'
    senha_origem = 'sua_senha_origem'
    banco_de_dados_origem = 'seu_banco_de_dados_origem'
    tabela_origem = 'sua_tabela_origem'

    # Dados de conexão com o banco de dados de destino (onde os dados serão importados)
    host_destino = 'seu_host_destino'
    usuario_destino = 'seu_usuario_destino'
    senha_destino = 'sua_senha_destino'
    banco_de_dados_destino = 'seu_banco_de_dados_destino'
    tabela_destino = 'sua_tabela_destino'

    try:
        # conecta ao banco de dados
        conn_origem = pymysql.connect(host=host_origem, user=usuario_origem, password=senha_origem, database=banco_de_dados_origem)
        cursor_origem = conn_origem.cursor()

        # Executando uma consulta SQL para selecionar dados
        cursor_origem.execute(f'SELECT * FROM {tabela_origem}')

        # Obtendo os resultados da consulta
        dados = cursor_origem.fetchall()

        # Criando um arquivo SQL para inserção dos dados
        nome_arquivo_sql = f'dados_{tabela_origem}_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.sql'
        with open(nome_arquivo_sql, 'w') as arquivo_sql:
            for registro in dados:
                valores = ', '.join(f"'{str(valor)}'" for valor in registro)
                comando_insert = f"INSERT INTO {tabela_destino} VALUES ({valores});\n"
                arquivo_sql.write(comando_insert)

        print(f"Dados exportados para '{nome_arquivo_sql}' em formato SQL")

    except pymysql.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")

    finally:
        # close conection
        if 'conn_origem' in locals() and conn_origem.open:
            conn_origem.close()

# Agendando a execução da função para um horário específico todos os dias
schedule.every().day.at("12:00").do(exportar_para_sql)  

# Verifica se tem tarefas agendadas e roda elas
while True:
    schedule.run_pending()
    time.sleep(1)
