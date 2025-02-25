import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import shutil
import os
import configparser
import ctypes 
import sqlite3
import socket
import xml.etree.ElementTree as ET
from dados import Criardados

acumulador_erro = 0
conteudo_enviado_email = []
log = []

class PPA_BACKUP:

    def main():
        arquivo_config=PPA_BACKUP.verifica_arquivo_config()
        if (arquivo_config==False):
            PPA_BACKUP.cria_config()
            PPA_BACKUP.backup()
        else:
            PPA_BACKUP.backup()


    def cria_config():
        arquivo_config = open("config.ini", "a", encoding='utf-8')
        arquivo_config.writelines("[GERAL]") 
        arquivo_config.writelines("\n")
        arquivo_config.writelines("DESTINO_BACKUP = D:/")
        arquivo_config.writelines("\n\n")
        arquivo_config.writelines("[EMAIL]\nENVIAR_EMAIL = NAO")
        arquivo_config.writelines("\n")
        arquivo_config.writelines("EMAIL_DESTINO =  ")
        arquivo_config.writelines("\n\n")
        arquivo_config.writelines("[NFE]")
        arquivo_config.writelines("\n")
        arquivo_config.writelines("EMP1 = C:/FIT/NFe/")
        arquivo_config.writelines("\n")
        arquivo_config.writelines("EMP2 = C:/FIT/NFe2/")
        arquivo_config.writelines("\n")
        arquivo_config.writelines("EMP3 = C:/FIT/NFe3/")
        arquivo_config.close


    def verifica_arquivo_config():
        if os.path.isfile('config.ini'):
            return True
        else:
            return False


    def acumulador_erros(erro):
        global acumulador_erro
        acumulador_erro=acumulador_erro+erro
        #print("QUANTIDADES DE ERROS NO BACKUP: ",acumulador_erro)
        return acumulador_erro


    def backup():
        #print("\nENTROU BACKUP!\n")
        data_completa=PPA_BACKUP.retorna_data_atual()
        ano_mes=PPA_BACKUP.retorna_anomes()
        nome_usuario = socket.gethostname()
        IP_usuario=(socket.gethostbyname(socket.gethostname()))

        # LER CONFIGURAÇÃO DO ARQUIVO: cnf.config
        config = configparser.ConfigParser()
        config.read('config.ini', encoding='utf-8')

        diretorio_backup = config.get('GERAL', 'DESTINO_BACKUP')

        enviar_email = config.get('EMAIL', 'ENVIAR_EMAIL')
        destinatario = config.get('EMAIL', 'EMAIL_DESTINO')

        emp1_local_nfe = config.get('NFE', 'EMP1')
        emp2_local_nfe = config.get('NFE', 'EMP2')
        emp3_local_nfe = config.get('NFE', 'EMP3')

        #print(diretorio_backup)

        if os.path.isdir(diretorio_backup):
            # ABRE CONEXÃO COM BANCO DE DADOS
            try:
                Criardados()
            except sqlite3.Error as e:
                PPA_BACKUP.acumulador_erros(1)
                PPA_BACKUP.mensagem(f"ERRO AO ABRIR BANCO DE DADOS: {e}")
                PPA_BACKUP.log(f"ERRO AO ABRIR BANCO DE DADOS: {e}")

            try:
                connection = sqlite3.connect('dados.db')
                cursor = connection.cursor()
                cursor.execute("INSERT INTO BACKUP (DATAHORA_BACKUP, IP_COMPUTADOR_BACKUP, USUARIO_COMPUTADOR_BACKUP, EMAIL_ENVIADO_BACKUP) VALUES ('"+data_completa+"','"+str(nome_usuario)+"','"+str(IP_usuario)+"','"+str(enviar_email)+"')")
                connection.commit()
            except sqlite3.Error as e:
                PPA_BACKUP.acumulador_erros(1)
                PPA_BACKUP.mensagem(f"ERRO AO ABRIR BANCO DE DADOS: {e}")
                PPA_BACKUP.log(f"ERRO AO ABRIR BANCO DE DADOS: {e}")
        
            diretorio_backup = str(diretorio_backup+"/BACKUP_PPA")

            PPA_BACKUP.titulo("BACKUP")
            PPA_BACKUP.mensagem("BACKUP INICIADO EM: "+diretorio_backup)
            PPA_BACKUP.log("BACKUP INICIADO EM: "+diretorio_backup)

            emp1_nfe=(emp1_local_nfe+"Transmitidas/"+ano_mes+'/NFe')
            emp2_nfe=(emp2_local_nfe+"Transmitidas/"+ano_mes+'/NFe')
            emp3_nfe=(emp3_local_nfe+"Transmitidas/"+ano_mes+'/NFe')

            emp_evento_nfe=(emp1_local_nfe+"Canceladas"+ano_mes+"/Evento/")

            PPA_BACKUP.pasta_backup(diretorio_backup)
            PPA_BACKUP.backup_banco(diretorio_backup)

            if os.path.exists(emp1_nfe):
                PPA_BACKUP.backup_emp_nfe(diretorio_backup, emp1_nfe, "NFe_EMP1")
                #PPA_BACKUP.backup_emp_eventos_nfe(diretorio_backup, emp1_nfe, emp1_evento_nfe)
            if os.path.exists(emp2_nfe):
                PPA_BACKUP.backup_emp_nfe(diretorio_backup, emp2_nfe, "NFe_EMP2")
            if os.path.exists(emp3_nfe):
                PPA_BACKUP.backup_emp_nfe(diretorio_backup, emp3_nfe, "NFe_EMP3")


            valida_destinatario=PPA_BACKUP.valida_destinatario(destinatario)
            #print(valida_destinatario)
            if (valida_destinatario==False):
                PPA_BACKUP.pular_linha()
                PPA_BACKUP.mensagem(f"O E-MAIL INFORMADO: '{destinatario}' É INVÁLIDO E O E-MAIL NÃO SERÁ ENVIADO.")
                PPA_BACKUP.log(f"O E-MAIL INFORMADO: '{destinatario}' É INVÁLIDO E O E-MAIL NÃO SERÁ ENVIADO.")
                PPA_BACKUP.mensagem("ACUMULADOR DE ERROS: "+str(quantidade_erros_backup))
                PPA_BACKUP.mensagem("BACKUP FINALIZADO.")
                PPA_BACKUP.log("BACKUP FINALIZADO.")
                log_mensagem = ",".join(str(element) for element in log) 
                log_mensagem=log_mensagem.replace(",","\n")
                PPA_BACKUP.arquivo_log(log_mensagem)
            else:
                if enviar_email=="SIM":
                    #print(destinatario)
                    quantidade_erros_backup=PPA_BACKUP.acumulador_erros(0)
                    #print(quantidade_erros_backup)
                    titulo=PPA_BACKUP.valida_titulo_email(quantidade_erros_backup)
                    PPA_BACKUP.pular_linha()
                    PPA_BACKUP.mensagem("ACUMULADOR DE ERROS: "+str(quantidade_erros_backup))
                    PPA_BACKUP.mensagem("BACKUP FINALIZADO.")
                    #print(conteudo_enviado_email)
                    corpo = ",".join(str(element) for element in conteudo_enviado_email) 
                    corpo=corpo.replace(",","\n")
                    remetente = "arquivospajola@gmail.com"
                    senha = "" #configure seu e-mail corretamente para enviar.
                    
                    log_mensagem = ",".join(str(element) for element in log) 
                    log_mensagem=log_mensagem.replace(",","\n")
                    PPA_BACKUP.arquivo_log(log_mensagem)
                    PPA_BACKUP.envia_email(titulo, corpo, remetente, destinatario, senha)
                    #os.remove("mensagem.txt")

                else:
                    quantidade_erros_backup=PPA_BACKUP.acumulador_erros(0)
                    titulo=PPA_BACKUP.valida_titulo_email(quantidade_erros_backup)
                    PPA_BACKUP.pular_linha()
                    PPA_BACKUP.mensagem("ACUMULADOR DE ERROS: "+str(quantidade_erros_backup))
                    PPA_BACKUP.mensagem("BACKUP FINALIZADO.")
                    PPA_BACKUP.log("BACKUP FINALIZADO.")
                    log_mensagem = ",".join(str(element) for element in log) 
                    log_mensagem=log_mensagem.replace(",","\n")
                    PPA_BACKUP.arquivo_log(log_mensagem)
        else: 
            ctypes.windll.user32.MessageBoxW(0,"LOCAL DE BACKUP NÃO EXISTE, INSIRA O LOCAL CORRETO NO ARQUIVO CONFIG.INI!","ERRO!", 0)

  
    def pasta_backup(diretorio_backup):
        #print("\nENTROU EM CRIA PASTA_BACKUP\n")
        try:
            os.mkdir(diretorio_backup)
            #print(f"O DIRETÓRIO '{diretorio_backup}' CRIADO COM SUCESSO!")
            PPA_BACKUP.mensagem(f"O DIRETÓRIO '{diretorio_backup}' CRIADO COM SUCESSO!")
            PPA_BACKUP.log(f"O DIRETÓRIO '{diretorio_backup}' CRIADO COM SUCESSO!")

        except FileExistsError:
            #PPA_BACKUP.acumulador_erros(1)
            #print(f"O DIRETÓRIO '{diretorio_backup}' JÁ EXISTE.")
            PPA_BACKUP.mensagem(f"O DIRETÓRIO '{diretorio_backup}' JÁ EXISTE.")
            PPA_BACKUP.log(f"O DIRETÓRIO '{diretorio_backup}' JÁ EXISTE.")
           
        except PermissionError:
            PPA_BACKUP.acumulador_erros(1)
            #print(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup}'.")
            PPA_BACKUP.mensagem(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup}'.")
            PPA_BACKUP.log(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup}'.")
        except Exception as e:
            PPA_BACKUP.acumulador_erros(1)
            #print(f"ERRO: {e}")
            PPA_BACKUP.mensagem(f"ERRO: {e}")
            PPA_BACKUP.log(f"ERRO: {e}")

            
    def backup_banco(diretorio_backup):
        PPA_BACKUP.pular_linha()
        PPA_BACKUP.titulo("BACKUP BANCO")
        PPA_BACKUP.log("BACKUP BANCO")
        #print("\nBACKUP BANCO\n")
        # CRIA PASTA BANCO PARA BACKUP
        PPA_BACKUP.mensagem("BANCO CRIANDO DIRETÓRIO PARA GUARDAR BANCO DE DADOS...")
        PPA_BACKUP.log("BANCO CRIANDO DIRETÓRIO PARA GUARDAR BANCO DE DADOS...")

        diretorio_backup_banco = (diretorio_backup+'/BANCO')
        banco_origem=('C:/FIT/Dados/BANCO.FDB')
        banco_destino=(diretorio_backup_banco+'/Banco.FDB')
    
        #print("RAIZ....: ", diretorio_backup)
        #print("BANCO...: ", diretorio_backup_banco)
        #print("ORIGEM..: ", banco_origem)
        #print("DESTINO.: ", banco_destino)

        try:
            os.mkdir(diretorio_backup_banco)
            #print(f"O DIRETÓRIO '{diretorio_backup_banco}' CRIADO COM SUCESSO!")
            #mensagem=(f"O DIRETÓRIO '{diretorio_backup_banco}' CRIADO COM SUCESSO!")
            PPA_BACKUP.mensagem(f"O DIRETÓRIO '{diretorio_backup_banco}' CRIADO COM SUCESSO!")
            PPA_BACKUP.log(f"O DIRETÓRIO '{diretorio_backup_banco}' CRIADO COM SUCESSO!")

        except FileExistsError:
            #PPA_BACKUP.acumulador_erros(1)
            #print(f"O DIRETÓRIO '{diretorio_backup_banco}' JÁ EXISTE.")
            #mensagem=(f"O DIRETÓRIO '{diretorio_backup_banco}' JÁ EXISTE.")
            PPA_BACKUP.mensagem(f"O DIRETÓRIO '{diretorio_backup_banco}' JÁ EXISTE.")
            PPA_BACKUP.log(f"O DIRETÓRIO '{diretorio_backup_banco}' JÁ EXISTE.")

        except PermissionError:
            PPA_BACKUP.acumulador_erros(1)
            #print(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup_banco}'.")
            #mensagem=(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup_banco}'.")
            PPA_BACKUP.mensagem(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup_banco}'.")
            PPA_BACKUP.log(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup_banco}'.")

        except Exception as e:
            PPA_BACKUP.acumulador_erros(1)
            #print(f"ERRO: {e}")
            #mensagem=(f"ERRO: {e}")
            PPA_BACKUP.mensagem(f"ERRO: {e}")
            PPA_BACKUP.log(f"ERRO: {e}")
        
        #COPIA BANCO:
        try:
            if (PPA_BACKUP.valida_banco()==True):
                shutil.copyfile(banco_origem, banco_destino)
                #print("BANCO DE DADOS FOI COPIADO COM SUCESSO!")
                #mensagem=("BANCO DE DADOS FOI COPIADO COM SUCESSO!")
                PPA_BACKUP.mensagem("BANCO DE DADOS FOI COPIADO COM SUCESSO!")
                PPA_BACKUP.log("BANCO DE DADOS FOI COPIADO COM SUCESSO!")
            else: 
                PPA_BACKUP.acumulador_erros(1)
                #print("NÃO FOI POSSÍVEL COPIAR BANCO DE DADOS!")
                #mensagem=("NÃO FOI POSSÍVEL COPIAR BANCO DE DADOS!")
                PPA_BACKUP.mensagem("NÃO FOI POSSÍVEL COPIAR BANCO DE DADOS!")
                PPA_BACKUP.log("NÃO FOI POSSÍVEL COPIAR BANCO DE DADOS!")

        except Exception as e:
            PPA_BACKUP.acumulador_erros(1)
            #print(f"NÃO FOI POSSÍVEL COPIAR O BANCO DE DADOS: {e}")
            #mensagem=(f"NÃO FOI POSSÍVEL COPIAR O BANCO DE DADOS: {e}")
            PPA_BACKUP.mensagem(f"NÃO FOI POSSÍVEL COPIAR O BANCO DE DADOS: {e}")
            PPA_BACKUP.log(f"NÃO FOI POSSÍVEL COPIAR O BANCO DE DADOS: {e}")


    def backup_emp_nfe(diretorio_backup, emp_nfe, empresa):

        quantidade_arquivos_movidos=0
        quantidade_arquivos_origem=0
        quantidade_arquivos_destino=0

        #print("BACKUP_EMP1_NFE")
        str_ano_mes=PPA_BACKUP.retorna_anomes()
        diretorio_backup=str(diretorio_backup)
        ano_mes=str(str_ano_mes+"/")
        ultimo_arquivo=PPA_BACKUP.retorna_ultimo_arquivo(emp_nfe)
        nome_empresa=PPA_BACKUP.retorna_nome_empresa(ultimo_arquivo,empresa)
        cnpj_empresa=PPA_BACKUP.retorna_CNPJ_empresa(ultimo_arquivo, "CNPJ NÃO ENCONTRADO!")
        diretorio_backup_nfe = (diretorio_backup+'/'+nome_empresa+'/'+ano_mes)

        PPA_BACKUP.pular_linha()
        PPA_BACKUP.titulo("BACKUP NFE: "+str(nome_empresa)+" - "+str(cnpj_empresa))
        PPA_BACKUP.log("BACKUP NFE: "+str(nome_empresa)+" - "+str(cnpj_empresa))
        PPA_BACKUP.mensagem("CRIANDO DIRETÓRIO PARA GUARDAR NOTAS FISCAIS DA EMPRESA - "+nome_empresa+' EM '+diretorio_backup_nfe)
        PPA_BACKUP.log("CRIANDO DIRETÓRIO PARA GUARDAR NOTAS FISCAIS DA EMPRESA - "+nome_empresa+' EM '+diretorio_backup_nfe)
    
        #print(diretorio_backup)
        #pasta_empresa_nfe=str("/NFe_EMP1/")
        #mensagem=("CRIANDO DIRETÓRIO PARA GUARDAR NOTAS FISCAIS DA EMPRESA - "+nome_empresa+' EM '+diretorio_backup_nfe)
        
        # CRIA PASTA NFE PARA BACKUP:
        try:
            #diretorio_backup_nfe = ("D:/BACKUP_PPA/NFe_EMP1/202501/")
            #print(type(diretorio_backup_nfe))
            #print(diretorio_backup_nfe)
        
            #diretorio_backup_nfe = (diretorio_backup+pasta_empresa_nfe+str_ano_mes)
            #diretorio_backup_nfe = (diretorio_backup+"/NFe_EMP1/")
            #diretorio_backup_nfe = str(diretorio_backup+pasta_empresa_nfe)
            os.makedirs(diretorio_backup_nfe)
            #os.mkdir(diretorio_backup_nfe)
            
            #print("DIRETORIO DE BACKUP_NFE: ",diretorio_backup_nfe)
            #print(f"O DIRETÓRIO '{diretorio_backup_nfe}' FOI CRIADO COM SUCESSO!")
            #mensagem=(f"O DIRETÓRIO '{diretorio_backup_nfe}' FOI CRIADO COM SUCESSO!")
            PPA_BACKUP.mensagem(f"O DIRETÓRIO '{diretorio_backup_nfe}' FOI CRIADO COM SUCESSO!")
            PPA_BACKUP.log(f"O DIRETÓRIO '{diretorio_backup_nfe}' FOI CRIADO COM SUCESSO!")

        except FileExistsError:
            #PPA_BACKUP.acumulador_erros(1)
            #print(f"O DIRETÓRIO '{diretorio_backup_nfe}' JÁ EXISTE.")
            #mensagem=(f"O DIRETÓRIO '{diretorio_backup_nfe}' JÁ EXISTE.")
            PPA_BACKUP.mensagem(f"O DIRETÓRIO '{diretorio_backup_nfe}' JÁ EXISTE.")
            PPA_BACKUP.log(f"O DIRETÓRIO '{diretorio_backup_nfe}' JÁ EXISTE.")

        except PermissionError:
            PPA_BACKUP.acumulador_erros(1)
            #print(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup_nfe}'.")
            #mensagem=(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup_nfe}'.")
            PPA_BACKUP.mensagem(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup_nfe}'.")
            PPA_BACKUP.log(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup_nfe}'.")

        except Exception as e:
            PPA_BACKUP.acumulador_erros(1)
            #print(f"ERRO: {e}")
            #mensagem=(f"ERRO: {e}")
            PPA_BACKUP.mensagem(f"ERRO: {e}")
            PPA_BACKUP.log(f"ERRO: {e}")

        #COPIA ARQUIVOS FISCAIS:
        
        try:
            connection = sqlite3.connect('dados.db')
            cursor = connection.cursor()

            codigo_backup=PPA_BACKUP.retorna_ultimo_backup()
            src_files = os.listdir(emp_nfe)
            dtn_files = os.listdir(diretorio_backup_nfe)

            for file in src_files:
                quantidade_arquivos_origem=quantidade_arquivos_origem+1

            for file in dtn_files:
                quantidade_arquivos_destino=quantidade_arquivos_destino+1
            
            PPA_BACKUP.mensagem("QUANTIDADE DE ARQUIVOS NA ORIGEM.: "+str(quantidade_arquivos_origem))
            PPA_BACKUP.log("QUANTIDADE DE ARQUIVOS NA ORIGEM.: "+str(quantidade_arquivos_origem))
            PPA_BACKUP.mensagem("QUANTIDADE DE ARQUIVOS NO DESTINO.: "+str(quantidade_arquivos_destino))
            PPA_BACKUP.log("QUANTIDADE DE ARQUIVOS NO DESTINO.: "+str(quantidade_arquivos_destino))
        
            for file_name in src_files:
                try:
                    if file_name not in dtn_files:
                        #print("ARQUIVO NOVO: ",file_name)
                        full_file_name = os.path.join(emp_nfe, file_name)
                        #print(str(codigo_backup)+" - "+str(file_name))
                        if os.path.isfile(full_file_name):
                                valida_arquivo_extensao=PPA_BACKUP.valida_arquivo_xml(file_name)
                                if (valida_arquivo_extensao==True):
                                    cnpj_nfe_empresa=PPA_BACKUP.retorna_CNPJ_empresa(full_file_name, "CNPJ NÃO ENCONTRADO!")
                                    chave_nfe_empresa=PPA_BACKUP.retorna_CHAVE_empresa(full_file_name, "CHAVE NÃO ENCONTRADA!")
                                    cursor.execute("INSERT INTO ARQUIVOS (BACKUP_CODIGO_ARQUIVOS, EMPRESA_ARQUIVOS, CNPJ_ARQUIVOS, CHAVE_NFE_ARQUIVOS, NOME_ARQUIVOS) VALUES ('"+(codigo_backup)+"','"+nome_empresa+"','"+str(cnpj_nfe_empresa)+"','"+str(chave_nfe_empresa)+"','"+str(file_name)+"')")
                                    shutil.copy(full_file_name, diretorio_backup_nfe)
                                    PPA_BACKUP.log("ARQUIVO MOVIDO COM SUCESSO: "+str(file_name))
                                    quantidade_arquivos_movidos=quantidade_arquivos_movidos+1
                        connection.commit()
                except Exception as e:
                    PPA_BACKUP.mensagem("NÃO FOI POSSÍVEL COPIAR NOTA FISCAL: "+str(file_name)+" ERRO: "+str(e))
                    PPA_BACKUP.log("NÃO FOI POSSÍVEL COPIAR NOTA FISCAL: "+str(file_name)+" ERRO: "+str(e))
                
            #print("NOTA FISCAL REFERENTE AO MÊS: "+str_ano_mes+" FOI COPIADO COM SUCESSO!")
            #mensagem=("NOTAS FISCAIS REFERENTE AO MÊS: "+str_ano_mes+" FORAM COPIADAS COM SUCESSO!")
            PPA_BACKUP.mensagem("QUANTIDADE ARQUIVOS MOVIDOS: "+str(quantidade_arquivos_movidos))
            PPA_BACKUP.log("QUANTIDADE ARQUIVOS MOVIDOS: "+str(quantidade_arquivos_movidos))
            PPA_BACKUP.mensagem("NOTAS FISCAIS REFERENTE AO MÊS: "+str_ano_mes+" FORAM COPIADAS COM SUCESSO!")
            PPA_BACKUP.log("NOTAS FISCAIS REFERENTE AO MÊS: "+str_ano_mes+" FORAM COPIADAS COM SUCESSO!")

        except Exception as e:
            PPA_BACKUP.acumulador_erros(1)
            #print(f"NÃO FOI POSSÍVEL COPIAR NOTA FISCAL: {e}")
            #mensagem=(f"NÃO FOI POSSÍVEL COPIAR NOTA FISCAL: {e}")
            PPA_BACKUP.mensagem(f"NÃO FOI POSSÍVEL COPIAR NOTA FISCAL: {e}")
            PPA_BACKUP.log(f"NÃO FOI POSSÍVEL COPIAR NOTA FISCAL: {e}")


    def backup_emp_eventos_nfe(diretorio_backup, emp1_nfe, emp1_evento_nfe):
        quantidade_arquivos_movidos=0
        quantidade_arquivos_origem=0
        quantidade_arquivos_destino=0

        str_ano_mes=PPA_BACKUP.retorna_anomes()
        diretorio_backup=str(diretorio_backup)
        ano_mes=str(str_ano_mes+"/")
        ultimo_arquivo=PPA_BACKUP.retorna_ultimo_arquivo(emp1_nfe)
        nome_empresa=PPA_BACKUP.retorna_nome_empresa(ultimo_arquivo,"NFe_EMP3")
        cnpj_empresa=PPA_BACKUP.retorna_CNPJ_empresa(ultimo_arquivo, "CNPJ NÃO ENCONTRADO!")
        diretorio_backup_nfe = (diretorio_backup+'/'+nome_empresa+'/'+ano_mes+'/Eventos')
        diretorio_backup_eventos_nfe_CCe=(diretorio_backup+'/'+nome_empresa+'/'+ano_mes+'/Eventos/CCe')
        diretorio_backup_eventos_nfe_cancelamento=(diretorio_backup+'/'+nome_empresa+'/'+ano_mes+'/Eventos/Cancelamento')

        PPA_BACKUP.pular_linha()
        PPA_BACKUP.titulo("BACKUP NFE EVENTOS: "+str(nome_empresa)+" - "+str(cnpj_empresa))
        PPA_BACKUP.log("BACKUP NFE EVENTOS: "+str(nome_empresa)+" - "+str(cnpj_empresa))
        PPA_BACKUP.mensagem("CRIANDO DIRETÓRIO PARA GUARDAR OS EVENTOS DAS NOTAS FISCAIS DA EMPRESA - "+nome_empresa+' EM '+diretorio_backup_nfe)
        PPA_BACKUP.log("CRIANDO DIRETÓRIO PARA GUARDAR OS EVENTOS DAS NOTAS FISCAIS DA EMPRESA - "+nome_empresa+' EM '+diretorio_backup_nfe)
        
        # CRIA PASTA NFE DE EVENTOS - CORREÇÃO PARA BACKUP:
        try:
            os.makedirs(diretorio_backup_eventos_nfe_CCe)
            PPA_BACKUP.mensagem(f"O DIRETÓRIO '{diretorio_backup_eventos_nfe_CCe}' FOI CRIADO COM SUCESSO!")
            PPA_BACKUP.log(f"O DIRETÓRIO '{diretorio_backup_eventos_nfe_CCe}' FOI CRIADO COM SUCESSO!")

        except FileExistsError:
            PPA_BACKUP.mensagem(f"O DIRETÓRIO '{diretorio_backup_eventos_nfe_CCe}' JÁ EXISTE.")
            PPA_BACKUP.log(f"O DIRETÓRIO '{diretorio_backup_eventos_nfe_CCe}' JÁ EXISTE.")

        except PermissionError:
            PPA_BACKUP.acumulador_erros(1)
            PPA_BACKUP.mensagem(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup_eventos_nfe_CCe}'.")
            PPA_BACKUP.log(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup_eventos_nfe_CCe}'.")

        except Exception as e:
            PPA_BACKUP.acumulador_erros(1)
            PPA_BACKUP.mensagem(f"ERRO: {e}")
            PPA_BACKUP.log(f"ERRO: {e}")

         # CRIA PASTA NFE DE EVENTOS - CANCELAMENTO PARA BACKUP:
        try:
            os.makedirs(diretorio_backup_eventos_nfe_cancelamento)
            PPA_BACKUP.mensagem(f"O DIRETÓRIO '{diretorio_backup_eventos_nfe_cancelamento}' FOI CRIADO COM SUCESSO!")
            PPA_BACKUP.log(f"O DIRETÓRIO '{diretorio_backup_eventos_nfe_cancelamento}' FOI CRIADO COM SUCESSO!")

        except FileExistsError:
            PPA_BACKUP.mensagem(f"O DIRETÓRIO '{diretorio_backup_eventos_nfe_cancelamento}' JÁ EXISTE.")
            PPA_BACKUP.log(f"O DIRETÓRIO '{diretorio_backup_eventos_nfe_cancelamento}' JÁ EXISTE.")

        except PermissionError:
            PPA_BACKUP.acumulador_erros(1)
            PPA_BACKUP.mensagem(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup_eventos_nfe_cancelamento}'.")
            PPA_BACKUP.log(f"PERMISSÃO NEGADA: NÃO FOI POSSÍVEL CRIAR '{diretorio_backup_eventos_nfe_cancelamento}'.")

        except Exception as e:
            PPA_BACKUP.acumulador_erros(1)
            PPA_BACKUP.mensagem(f"ERRO: {e}")
            PPA_BACKUP.log(f"ERRO: {e}")


        #COPIA ARQUIVOS FISCAIS DE EVENTOS - CCE:
        try:
            connection = sqlite3.connect('dados.db')
            cursor = connection.cursor()
            codigo_backup=PPA_BACKUP.retorna_ultimo_backup()
            src_files = os.listdir(emp1_evento_nfe)
            dtn_files = os.listdir(diretorio_backup_eventos_nfe_CCe)
            for file in src_files:
                quantidade_arquivos_origem=quantidade_arquivos_origem+1
            for file in dtn_files:
                quantidade_arquivos_destino=quantidade_arquivos_destino+1
                
            PPA_BACKUP.mensagem("QUANTIDADE DE ARQUIVOS NA ORIGEM..: "+str(quantidade_arquivos_origem))
            PPA_BACKUP.log("QUANTIDADE DE ARQUIVOS NA ORIGEM..: "+str(quantidade_arquivos_origem))
            PPA_BACKUP.mensagem("QUANTIDADE DE ARQUIVOS NO DESTINO.: "+str(quantidade_arquivos_destino))
            PPA_BACKUP.log("QUANTIDADE DE ARQUIVOS NO DESTINO.: "+str(quantidade_arquivos_destino))
        
            for file_name in src_files:
                try:
                    if file_name not in dtn_files:
                        full_file_name = os.path.join(emp1_nfe, file_name)
                        if os.path.isfile(full_file_name):
                            cnpj_nfe_empresa=PPA_BACKUP.retorna_CNPJ_empresa(full_file_name, "CNPJ NÃO ENCONTRADO!")
                            chave_nfe_empresa=PPA_BACKUP.retorna_CHAVE_empresa(full_file_name, "CHAVE NÃO ENCONTRADA!")
                            cursor.execute("INSERT INTO ARQUIVOS (BACKUP_CODIGO_ARQUIVOS, EMPRESA_ARQUIVOS, CNPJ_ARQUIVOS, CHAVE_NFE_ARQUIVOS, NOME_ARQUIVOS) VALUES ('"+(codigo_backup)+"','"+nome_empresa+"','"+str(cnpj_nfe_empresa)+"','"+str(chave_nfe_empresa)+"','"+str(file_name)+"')")
                            shutil.copy(full_file_name, diretorio_backup_nfe)
                            PPA_BACKUP.log("ARQUIVO MOVIDO COM SUCESSO: "+str(file_name))
                            quantidade_arquivos_movidos=quantidade_arquivos_movidos+1
                        connection.commit()
                except Exception as e:
                    PPA_BACKUP.mensagem("NÃO FOI POSSÍVEL COPIAR NOTA FISCAL: "+str(file_name)+" ERRO: "+str(e))
                    PPA_BACKUP.log("NÃO FOI POSSÍVEL COPIAR NOTA FISCAL: "+str(file_name)+" ERRO: "+str(e))
    
            PPA_BACKUP.mensagem("QUANTIDADE ARQUIVOS MOVIDOS: "+str(quantidade_arquivos_movidos))
            PPA_BACKUP.log("QUANTIDADE ARQUIVOS MOVIDOS: "+str(quantidade_arquivos_movidos))
            PPA_BACKUP.mensagem("NOTAS FISCAIS REFERENTE AO MÊS: "+str_ano_mes+" FORAM COPIADAS COM SUCESSO!")
            PPA_BACKUP.log("NOTAS FISCAIS REFERENTE AO MÊS: "+str_ano_mes+" FORAM COPIADAS COM SUCESSO!")

        except Exception as e:
            PPA_BACKUP.acumulador_erros(1)
            PPA_BACKUP.mensagem(f"NÃO FOI POSSÍVEL COPIAR NOTA FISCAL: {e}")
            PPA_BACKUP.log(f"NÃO FOI POSSÍVEL COPIAR NOTA FISCAL: {e}")

        
    def retorna_data_atual():
         dt = datetime.now()
         data_completa = dt.strftime("%d_%m_%Y-%H_%M")
         return str(data_completa)


    def retorna_hora_formatada():
         dt = datetime.now()
         data_completa = dt.strftime("%H:%M")
         return str(data_completa)
    

    def retorna_anomes():
         dt = datetime.now()
         anomes = dt.strftime("%Y%m")
         return anomes
    

    def valida_banco():
        banco=('C:/FIT/Dados/Banco.FDB')
        if os.path.isfile(banco):
            return True
        return False
    

    def valida_arquivo_xml(arquivo):
        arquivo=arquivo.split(".")
        extensao=arquivo[1]
        extensao=extensao.upper()
        if (extensao=="XML"):
            return True
        else: 
            return False


    def valida_destinatario(destinatario):
        if destinatario is None:
            return False
        if destinatario == "":
            return False
        if len(destinatario) < 10:
            return False
        return True
    

    def valida_titulo_email(quantidade_erros_backup):
        data_completa=PPA_BACKUP.retorna_data_atual()

        if (quantidade_erros_backup<1):
            return("BACKUP PPA - "+data_completa+" - REALIZADO COM SUCESSO!")
        else:
            return("BACKUP PPA - "+data_completa+" - REALIZADO COM AVISOS!")

      
    def retorna_ultimo_backup():
        try:
            connection = sqlite3.connect('dados.db')
            cursor = connection.cursor()
            dados = cursor.execute("SELECT CODIGO_BACKUP FROM BACKUP ORDER BY CODIGO_BACKUP DESC LIMIT 1")
            connection.close
            
            for row in dados:
                    codigo_backup=row[0]
            return str(codigo_backup)
           
        except Exception as e:
            PPA_BACKUP.acumulador_erros(1)
            PPA_BACKUP.mensagem(f"NÃO FOI POSSÍVEL RETORNAR O NÚMERO DO ÚLTIMO BACKUP: {e}")
            PPA_BACKUP.log(f"NÃO FOI POSSÍVEL RETORNAR O NÚMERO DO ÚLTIMO BACKUP: {e}")
        

    def arquivo_log(conteudo):
        data=PPA_BACKUP.retorna_data_atual()
        hora=PPA_BACKUP.retorna_anomes()
        nome_arquivo = ("backup_"+str(data)+"-"+str(hora))
        arquivo_mensagem = open(str(nome_arquivo), "a", encoding='utf-8') 
        arquivo_mensagem.writelines(conteudo)
        arquivo_mensagem.close


    def pular_linha():
        global conteudo_enviado_email
        texto_mensagem=(",")
        conteudo_enviado_email.append(texto_mensagem)


    def titulo(titulo):
        #print("TÍTULO ADICIONADO: ",titulo)
        hora_atual=PPA_BACKUP.retorna_hora_formatada()
        global conteudo_enviado_email
        texto_mensagem=(hora_atual+" ##### "+titulo+" ##### ")
        conteudo_enviado_email.append(texto_mensagem)


    def mensagem(conteudo):
        #print("CONTEUDO ADICIONADO: ",conteudo)
        hora_atual=PPA_BACKUP.retorna_hora_formatada()
        global conteudo_enviado_email
        texto_mensagem=(hora_atual+" - "+conteudo)
        conteudo_enviado_email.append(texto_mensagem)


    def log(conteudo):
        hora_atual=PPA_BACKUP.retorna_hora_formatada()
        global log
        texto_log=(hora_atual+" - "+conteudo)
        log.append(texto_log)
          

    def recupera_mensagem():
        try:
            with open('mensagem.txt','r',encoding='utf-8') as file:
                mensagem = " ".join(line.rstrip() for line in file)
                return mensagem
        except:
            PPA_BACKUP.mensagem("ERRO AO RECUPERAR LOGS DO BACKUP!")
    

    def retorna_nome_empresa(xml,nome):
        try:
            #print(xml)
            root = ET.parse(xml).getroot()
            nsNFE = {'ns':"http://www.portalfiscal.inf.br/nfe"}
            nome_empresa = root.find('ns:NFe/ns:infNFe/ns:emit/ns:xNome', nsNFE)
            cnpj_empresa = root.find('')
            if (nome_empresa)!=None:
                nome_empresa=nome_empresa.text
                nome_empresa_separado=nome_empresa.split(" ")
                primeiro_nome_empresa=nome_empresa_separado[0]
                segundo_nome_empresa=nome_empresa_separado[1]
                nome_empresa_corrigido=str(primeiro_nome_empresa+" "+segundo_nome_empresa)
                return(nome_empresa_corrigido)
            else:
                return nome
            
        except Exception as e:
            PPA_BACKUP.acumulador_erros(1)
            PPA_BACKUP.mensagem(f"NÃO FOI POSSÍVEL VERIFICAR NOME DA EMPRESA PELO XML! {e}")
            return nome
    
            
    def retorna_CNPJ_empresa(xml, sem_cnpj):
        try:
            #print(xml)
            root = ET.parse(xml).getroot()
            nsNFE = {'ns':"http://www.portalfiscal.inf.br/nfe"}
            cnpj_empresa = root.find('ns:NFe/ns:infNFe/ns:emit/ns:CNPJ', nsNFE)
            if (cnpj_empresa)!=None:
                
                return(cnpj_empresa.text)
            else:
                PPA_BACKUP.acumulador_erros(1)
                PPA_BACKUP.log("NÃO FOI POSSÍVEL VERIFICAR CNPJ DO XML: "+str(xml))
                return sem_cnpj
            
            
        except Exception as e:
            PPA_BACKUP.acumulador_erros(1)
            #PPA_BACKUP.mensagem(f"NÃO FOI POSSÍVEL VERIFICAR CNPJ DA EMPRESA PELO XML! {e}")
            PPA_BACKUP.log("ERRO AO RETORNAR CNPJ DO XML: "+str(xml))
            return 
        

    def retorna_CHAVE_empresa(xml,sem_chave):
        try:
            root = ET.parse(xml).getroot()
            nsNFE = {'ns':"http://www.portalfiscal.inf.br/nfe"}
            chave_nfe_empresa = root.find('ns:protNFe/ns:infProt/ns:chNFe', nsNFE)
            if (chave_nfe_empresa)!=None:
                
                return(chave_nfe_empresa.text)
            else:
                PPA_BACKUP.acumulador_erros(1)
                PPA_BACKUP.log("NÃO FOI POSSÍVEL VERIFICAR CHAVE DO XML: "+str(xml))
                return sem_chave
            
        except Exception as e:
            PPA_BACKUP.acumulador_erros(1)
            #PPA_BACKUP.mensagem(f"NÃO FOI POSSÍVEL VERIFICAR CHAVE DA EMPRESA PELO XML! {e}")
            PPA_BACKUP.log("ERRO AO RETORNAR CHAVE DO XML: "+str(xml))
            return
            

    def retorna_ultimo_arquivo(diretorio):
        arquivos = os.listdir(diretorio)
        diretorios = [os.path.join(diretorio, nome) for nome in arquivos]
        return max(diretorios, key=os.path.getctime)
    

    def envia_email(titulo, corpo, remetente, destinatario, senha):
        try:
            msg = MIMEText(corpo)
            msg['Subject'] = titulo
            msg['From'] = remetente
            msg['To'] = (destinatario)
            #msg['To'] = ', '.join(destinatario)

            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
                smtp_server.login(remetente, senha)
                smtp_server.sendmail(remetente, destinatario, msg.as_string())
                #print("Mensagem enviada!")
                #ctypes.windll.user32.MessageBoxW(0,"E-MAIL FOI ENVIADO COM SUCESSO!\nVERIFIQUE OS DETALHES EM SEU E-MAIL", "AVISO!", 0)
        except Exception as e:
                #print(f"Erro ao enviar email: {e}")
                ctypes.windll.user32.MessageBoxW(0,"ERRO AO ENVIAR E-MAIL","ERRO!", 0)


PPA_BACKUP.main()
