import sqlite3
from datetime import datetime


class Criardados():
    def criardados():

        agora = datetime.now()
        datahora = agora.strftime('%d/%m/%Y %H:%M:%S')

        backup = """ CREATE TABLE IF NOT EXISTS BACKUP (
                    CODIGO_BACKUP INTEGER PRIMARY KEY AUTOINCREMENT,
                    DATAHORA_BACKUP DATETIME,
                    IP_COMPUTADOR_BACKUP VARCHAR (60) NOT NULL,
                    USUARIO_COMPUTADOR_BACKUP VARCHAR (60) NOT NULL,
                    EMAIL_ENVIADO_BACKUP VARCHAR (1) NOT NULL
                    );"""
        
        arquivos = """ CREATE TABLE IF NOT EXISTS ARQUIVOS (
                    BACKUP_CODIGO_ARQUIVOS INTEGER NOT NULL,
                    EMPRESA_ARQUIVOS VARCHAR (80) NOT NULL,
                    CNPJ_ARQUIVO VARCHAR (60) NOT NULL,
                    NOME_ARQUIVO VARCHAR (200) NOT NULL,
                    FOREIGN KEY (BACKUP_CODIGO_ARQUIVOS) REFERENCES BACKUP (CODIGO_BACKUP)
                    );"""

        try:
            connection = sqlite3.connect('dados.db')
            #print("CONEXÃO COM DADOS.DB")
            cursor = connection.cursor()
        
            try: 
                cursor.execute(backup)
                connection.commit() 
            except sqlite3.Error as e:
                print("ERRO AO AO CRIAR TABELA BACKUP! ",e)
            
            try: 
                cursor.execute(arquivos)
                connection.commit() 
            except sqlite3.Error as e:
                print("ERRO AO AO CRIAR TABELA ARQUIVOS! ",e)

        except sqlite3.Error as e:
                print("ERRO AO ABRIR CONEXÃO COM BANCO DE DADOS! ",e)

        connection.close
        #print("CONEXÃO FECHADA.")
       
Criardados.criardados()