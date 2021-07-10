import pandas as pd
import numpy as np
import collections
import requests
import json
import gzip
import shutil
import pymongo
import time
import datetime
from urllib.request import urlopen
from pymongo import MongoClient
import re
from sshtunnel import SSHTunnelForwarder
import pprint
from datetime import date
import os
from urllib.parse import urlparse
import redis
# SELENIUM
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
# LOG
import sys
import logging
import logging.config


# Script para carga de dados no sistema de Licitações e Contratos referente ao Governo da Paraíba

# ================== DADOS CONTRATOS E ADITIVOS GOV  ================================
def df_aditivos_contratos_governo_pb(ANO_DA_LICITACAO):

    colunas = {
               'CtNumero':'numero_contrato',
               'NU_ProcessoLicitacao': 'numero_processo_licitatorio', 
               'CtVigenciaInicio': 'data_inicio_vigencia',
               'CtVigenciaTermino2': 'data_termino_vigencia',
               'Tempo':'DURACAO_MESES',
               'OrNome':'contratante',
               'Credor':'fornecedor',
               'ObNome':'objeto_contrato',
               'CtValorTotal':'valor_proposta',
               'CtValorTotal1':'valor_total',
         
              }
    
    colunas_df_temp = {
        
               'CODIGO_CONTRATO':'codigo_contrato',
               'NUMERO_REGISTRO_CGE': 'numero_cge', 
               'NOME_MUNICIPIO':'municipio',
               'NOME_GESTOR_CONTRATO':'nome_gestor_contrato',
               'NUMERO_PORTARIA':'numero_portaria',
               'DATA_PUBLICACAO_PORTARIA':'data_publicacao_portaria',
               'URL_CONTRATO':'url_contrato'
    }
    
    colunas_adt = {
        
            'CODIGO_CONTRATO':'codigo_contrato',
            'NUMERO_REGISTRO_CGE': 'numero_cge', 
            'NOME_CONTRATANTE': 'contratante',
            'NUMERO_PROCESSO_LICITATORIO': 'numero_processo_licitatorio',
            'OBJETO_CONTRATO':'objeto_contrato',
            'COMPLEMENTO_OBJETO_CONTRATO':'complemento_objeto_contrato',
            'NOME_CONTRATADO':'fornecedor',
            'CPFCNPJ_CONTRATADO':'cpf_cnpj_fornecedor',
            'DATA_CELEBRACAO_CONTRATO':'data_celebracao',
            'DATA_PUBLICACAO_x':'data_publicacao',
            'DATA_INICIO_VIGENCIA_x':'data_inicio_vigencia',
            'DATA_TERMINO_VIGENCIA_x':'data_termino_vigencia',
            'VALOR_ORIGINAL':'valor_proposta',
            'NOME_MUNICIPIO':'municipio',
            'NOME_GESTOR_CONTRATO':'nome_gestor_contrato',
            'NUMERO_PORTARIA':'numero_portaria',
            'DATA_PUBLICACAO_PORTARIA':'data_publicacao_portaria',
            'URL_CONTRATO':'url_contrato',
            'CODIGO_ADITIVO_CONTRATO' : 'codigo_aditivo_contrato',
            'MOTIVO_ADITIVACAO' : 'motivo_aditivacao',
            'NUMERO_ADITIVO_CONTRATO' : 'numero_aditivo_contrato',
            'DATA_INICIO_VIGENCIA_y' : 'data_inicio_vigencia_aditivo',
            'DATA_TERMINO_VIGENCIA_y' : 'data_termino_vigencia_aditivo',
            'VALOR_ADITIVO' : 'valor_aditivo',
            'OBJETO_ADITIVO' : 'objeto_ditivo',
            'DATA_CELEBRACAO_ADITIVO' : 'data_celebracao_aditivo',
            'DATA_PUBLICACAO_y' : 'data_publicacao_aditivo',
            'DATA_REPUBLICACAO' : 'data_republicacao',
            'URL_ADITIVO_CONTRATO' : 'url_aditivo_contrato'
            
          }
    
    
    data_atual = date.today()
    ano_atual = data_atual.year
    
    if(int( (int(ANO_DA_LICITACAO)) < (ano_atual-4)) | ( int(ANO_DA_LICITACAO) > ano_atual) ):
        log_obj = Logger('ANO_DA_LICITACAO')
        return False
    
    
    elif(int(ANO_DA_LICITACAO) == (ano_atual-4)):
        
        try:
            # PORTAL DA TRANSPARÊNCIA
            df_gov_contratos = pd.read_csv('./arquivos_gerados/listaContratos_gov_'+ANO_DA_LICITACAO+'.csv', delimiter=',', encoding = 'UTF-8')

            # PORTAL DE DADOS ABERTOS
            url_cont_1 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=contratos&exercicio='+str(int(ANO_DA_LICITACAO)+1)+'&tipo=csv'
            url_cont_2 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=contratos&exercicio='+ANO_DA_LICITACAO+'&tipo=csv'
            url_cont_3 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=contratos&exercicio='+str(int(ANO_DA_LICITACAO)-1)+'&tipo=csv'

            df_con_1 = pd.read_csv(url_cont_1, delimiter=';', encoding = 'ISO-8859-9')
            df_con_2 = pd.read_csv(url_cont_2, delimiter=';', encoding = 'ISO-8859-9')
            df_con_3 = pd.read_csv(url_cont_3, delimiter=';', encoding = 'ISO-8859-9')

            df_con = pd.concat([df_con_1,df_con_2,df_con_3], ignore_index=True)

            # PORTAL DE DADOS ABERTOS - ADITIVOS
            url_adt_1 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=aditivos_contrato&exercicio='+str(int(ANO_DA_LICITACAO)+1)+'&tipo=csv'
            url_adt_2 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=aditivos_contrato&exercicio='+ANO_DA_LICITACAO+'&tipo=csv'
            adt_1 = pd.read_csv(url_adt_1, delimiter=';', encoding = 'ISO-8859-9')
            adt_2 = pd.read_csv(url_adt_2, delimiter=';', encoding = 'ISO-8859-9')
            df_aditivos = adt_1.append(adt_2)

            log_obj = Logger('df_aditivos_contratos_governo_pb')
            log_obj.info("download com sucesso".format())
        
        except:
            log_obj = Logger('df_aditivos_contratos_governo_pb')
            log_obj.exception("Erro ao processar os dados do ano: "+ANO_DA_LICITACAO+"".format());
            return False;
        
    elif(int(ANO_DA_LICITACAO) == ano_atual):
        
        try:     
            # PORTAL DA TRANSPARÊNCIA
            df_gov_contratos = pd.read_csv('./arquivos_gerados/listaContratos_gov_'+ANO_DA_LICITACAO+'.csv', delimiter=',', encoding = 'UTF-8')

            # PORTAL DE DADOS ABERTOS
            url_cont_1 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=contratos&exercicio='+ANO_DA_LICITACAO+'&tipo=csv'
            url_cont_2 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=contratos&exercicio='+str(int(ANO_DA_LICITACAO)-1)+'&tipo=csv'

            df_con_1 = pd.read_csv(url_cont_1, delimiter=';', encoding = 'ISO-8859-9')
            df_con_2 = pd.read_csv(url_cont_2, delimiter=';', encoding = 'ISO-8859-9')

            df_con = pd.concat([df_con_1,df_con_2], ignore_index=True)

            # PORTAL DE DADOS ABERTOS - ADITIVOS
            url_adt_1 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=aditivos_contrato&exercicio='+ANO_DA_LICITACAO+'&tipo=csv'
            df_aditivos = pd.read_csv(url_adt_1, delimiter=';', encoding = 'ISO-8859-9')

            log_obj = Logger('df_aditivos_contratos_governo_pb')
            log_obj.info("download com sucesso".format())

        except:
            log_obj = Logger('df_aditivos_contratos_governo_pb')
            log_obj.exception("Erro ao processar os dados do ano: "+ANO_DA_LICITACAO+"".format());
            return False;
        
    elif(int(ANO_DA_LICITACAO) < ano_atual):
        
        try: 
            # PORTAL DA TRANSPARÊNCIA
            df_gov_contratos = pd.read_csv('./arquivos_gerados/listaContratos_gov_'+ANO_DA_LICITACAO+'.csv', delimiter=',', encoding = 'UTF-8')

            # PORTAL DE DADOS ABERTOS
            url_cont_1 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=contratos&exercicio='+str(int(ANO_DA_LICITACAO)+1)+'&tipo=csv'
            url_cont_2 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=contratos&exercicio='+ANO_DA_LICITACAO+'&tipo=csv'
            url_cont_3 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=contratos&exercicio='+str(int(ANO_DA_LICITACAO)-1)+'&tipo=csv'

            df_con_1 = pd.read_csv(url_cont_1, delimiter=';', encoding = 'ISO-8859-9')
            df_con_2 = pd.read_csv(url_cont_2, delimiter=';', encoding = 'ISO-8859-9')
            df_con_3 = pd.read_csv(url_cont_3, delimiter=';', encoding = 'ISO-8859-9')

            df_con = pd.concat([df_con_1,df_con_2,df_con_3], ignore_index=True)

            # PORTAL DE DADOS ABERTOS - ADITIVOS
            url_adt_1 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=aditivos_contrato&exercicio='+str(int(ANO_DA_LICITACAO)+1)+'&tipo=csv'
            url_adt_2 = 'https://dados.pb.gov.br/gettranspdimensoesexercicio?nome=aditivos_contrato&exercicio='+ANO_DA_LICITACAO+'&tipo=csv'
         
            adt_1 = pd.read_csv(url_adt_1, delimiter=';', encoding = 'ISO-8859-9')
            adt_2 = pd.read_csv(url_adt_2, delimiter=';', encoding = 'ISO-8859-9')

            df_aditivos = adt_1.append(adt_2)
            
            log_obj = Logger('df_aditivos_contratos_governo_pb')
            log_obj.info("download com sucesso".format())
            
        except:
            log_obj = Logger('df_aditivos_contratos_governo_pb')
            log_obj.exception("Erro ao processar os dados do ano: "+ANO_DA_LICITACAO+"".format());
            return False;

    df_gov_contratos = pd.merge(df_gov_contratos,df_con, how="left", left_on=["NU_ProcessoLicitacao",'CtNumero'], right_on=['NUMERO_PROCESSO_LICITATORIO','NUMERO_CONTRATO'])    
    df_gov_contratos = df_gov_contratos.rename(colunas, axis = 1)
    
    df_gov_adt = pd.merge(df_con,df_aditivos, how="inner", on='CODIGO_CONTRATO')
    df_gov_adt = df_gov_adt.rename(colunas_adt, axis = 1)
    df_gov_adt.drop(columns=['numero_cge','contratante','objeto_contrato','complemento_objeto_contrato',
                            'fornecedor','data_celebracao','data_publicacao','data_inicio_vigencia','data_termino_vigencia',
                            'OUTROS_MUNICIPIOS'], inplace=True)

    df_gov_adt['cpf_cnpj_fornecedor'] = df_gov_adt['cpf_cnpj_fornecedor'].apply(lambda x: '{0:0>14}'.format(x))
   
    df_gov_contratos = df_gov_contratos.assign(aditivos='')

    for g in df_gov_contratos.itertuples():
        
        lista_aditivos = []
        for adt in df_gov_adt.itertuples():
            if( (g.numero_processo_licitatorio == adt.numero_processo_licitatorio) ):
                
                lista_aditivos.append(df_gov_adt.loc[adt.Index].to_dict())
            
        df_gov_contratos.at[g.Index,'aditivos'] = lista_aditivos
        
    df_gov_contratos = df_gov_contratos.assign(cpf_cnpj_fornecedor='')
    df_gov_contratos.drop(columns=['Textbox7','DURACAO_MESES','NOME_CONTRATANTE','NUMERO_PROCESSO_LICITATORIO','OBJETO_CONTRATO',
                             'COMPLEMENTO_OBJETO_CONTRATO','NOME_CONTRATADO','CPFCNPJ_CONTRATADO','DATA_CELEBRACAO_CONTRATO',
                             'DATA_PUBLICACAO','DATA_INICIO_VIGENCIA','DATA_TERMINO_VIGENCIA','OUTROS_MUNICIPIOS','VALOR_ORIGINAL'], inplace=True)
    df_gov_contratos['numero_contrato'] = df_gov_contratos['numero_contrato'].str.replace('/','',regex=True)
    df_gov_contratos['NUMERO_CONTRATO'] = df_gov_contratos['NUMERO_CONTRATO'].str.replace('/','',regex=True)
    
    df_gov_contratos = df_gov_contratos.assign(entidade_governamental='Governo da Paraíba')
    df_gov_contratos = df_gov_contratos.rename(colunas_df_temp, axis = 1)
    df_gov_contratos['valor_proposta'] = df_gov_contratos['valor_proposta'].replace(np.nan, 0)
    df_gov_contratos['valor_total'] = df_gov_contratos['valor_total'].replace(np.nan, 0)
    df_gov_contratos.fillna('', inplace=True)
    df_gov_contratos = df_gov_contratos.assign(protocolo_licitacao='')
    
    df_gov_contratos.to_json('./arquivos_gerados/df_contratos_governo_com_aditivos_pb.json', orient = 'table')
    return True;
    
# ================== LOG ================================
def Logger(file_name):
    
    # criar pasta log dentro do servidor
    diretorio_log = './log/'

    formatter = logging.Formatter(fmt='%(asctime)s %(module)s,line: %(lineno)d %(levelname)8s | %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
    logging.basicConfig(filename = '%s.log' %(diretorio_log+file_name),
                        format= '%(asctime)s %(module)s,line: %(lineno)d %(levelname)8s | %(message)s',
                        datefmt='%d/%m/%Y %H:%M:%S', filemode = 'w', level = logging.DEBUG)
    log_obj = logging.getLogger()
    log_obj.setLevel(logging.DEBUG)
    
    return log_obj

# ================== DADOS TCE SAGRES ================================ 
def df_licitacao_gov_sagres(ANO_DA_LICITACAO):
    
    try:
        url = "https://dados.tce.pb.gov.br/TCE-PB-Portal-Gestor-Licitacoes_Propostas.txt.gz"
        resp = urlopen(url)

        with gzip.open(resp, 'rb') as entrada:
            with open('./arquivos_gerados/licitacao_sagres.txt', 'wb') as saida:
                shutil.copyfileobj(entrada, saida)

        df = pd.read_csv('./arquivos_gerados/licitacao_sagres.txt', delimiter='|',  dtype='unicode')

        df_licitacao_gov = df.loc[ 
                        (df['nome_tipo_jurisdicionado']=='Secretaria de Estado')
                      & (df['nome_esfera_jurisdicionado']=='Estadual')
                      &(df['ano_homologacao_licitacao']==ANO_DA_LICITACAO)
                      & (df['situacao_proposta']=='Vencedora')        
                   ]

        colunas = {'nome_proponente':'fornecedor'}
        df_licitacao_gov = df_licitacao_gov.rename(colunas, axis = 1)
        df_licitacao_gov = df_licitacao_gov.assign(entidade_governamental='Governo da Paraíba')
        df_licitacao_gov = df_licitacao_gov.assign(contratos='')
        df_licitacao_gov = df_licitacao_gov.loc[:, ~df_licitacao_gov.columns.str.contains('^Unnamed')]

        df_licitacao_gov.to_csv('./arquivos_gerados/licitacao_sagres_'+ANO_DA_LICITACAO+'.csv')
        return True;

    except:
        log_obj = Logger('df_licitacao_gov_sagres')
        log_obj.exception("Erro ao processar os dados do ano: "+ANO_DA_LICITACAO+"".format());
        return False;

# ================== DOWNLOAD CONTRATOS DO PORTAL DE TRANSPARÊNCIA GOV ================================
# https://transparencia.pb.gov.br/compras/contratos
def download_contratos_transparencia_gov(ANOS):
    try:
        for i in range (len(ANOS)):
    
            path = "./arquivos_gerados";

            tempo_padrao = 10; # segundos

            options = webdriver.ChromeOptions()
            options.add_argument("headless")
            options.add_experimental_option("prefs", {
              "download.default_directory": "./arquivos_gerados",
              "download.prompt_for_download": False,
              "download.directory_upgrade": True,
              "safebrowsing.enabled": True
            })


            driver = webdriver.Chrome('./navegador/chromedriver', options=options)
            driver.get('https://transparencia.pb.gov.br/compras/contratos')
            time.sleep(tempo_padrao)

            driver.switch_to.frame('iFrameResizer0')

            # definir o ano
            selectAno = Select(driver.find_element_by_name('RPTRender$ctl08$ctl03$ddValue'))
            selectAno.select_by_value(ANOS[i])

            # esperar para carregar o select de ano
            time.sleep(tempo_padrao)

            # definir como data de publicação
            selectDataPublicacao = Select(driver.find_element_by_name("RPTRender$ctl08$ctl07$ddValue"))
            selectDataPublicacao.select_by_value('2')

            time.sleep(tempo_padrao)
            # renderizar os resultados da seleção escolhida
            driver.find_element_by_id('RPTRender_ctl08_ctl00').click()
            time.sleep(tempo_padrao)

            # download do csv
            botao_csv = driver.find_element_by_xpath("//div[@id='RPTRender_ctl09_ctl04_ctl00_Menu']/div[6]/a")
            driver.execute_script("arguments[0].click();", botao_csv)

            # tempo para download
            time.sleep(30)

            os.rename(path + '/ListaContratos.csv', path + '/listaContratos_gov_'+ANOS[i]+'.csv')

            driver.quit()
            
            log_obj = Logger('download_contratos_transparencia_gov')
            log_obj.info("download com sucesso".format())
            log_obj.handlers.clear()
            
        return True;

    except:
        log_obj = Logger('download_contratos_transparencia_gov')
        log_obj.exception("Erro ao processar os dados do ano: "+ANOS[i]+"".format());

        log_obj.handlers.clear()
        return False;
    
def carga_mongoDB(  my_df, collection_name,
                          database_name = 'licitacoesecontratos',
                          server = 'localhost',
                          mongodb_port = 27017):
    

    client = MongoClient('localhost',int(mongodb_port))
    db = client[database_name]
    collection = db[collection_name]

    collection.delete_many({})
    my_dict = my_df.to_dict('records')
    collection.insert_many(my_dict)
    
    client.close()
    
def main(ANOS):
 
    resultado_transp_contratos = True;
    resultado_licitacao_tce_sagres = True;
    resultado_contratos_aditivos = True;

    #ROTINA PARA TENTATIVAS DE DOWNLOAD DE CONTRATOS DA TRANSPARÊNCIA GOV 
    quantidade_tentativas = 3
    while (quantidade_tentativas > 0): 
        resultado_transp_contratos = download_contratos_transparencia_gov(ANOS);
        if(resultado_transp_contratos):
            break;
        quantidade_tentativas -= 1;
        time.sleep(10) # tempo de espera para próxima execução

    if not(resultado_transp_contratos):
        return False;

    df_carga = pd.DataFrame()
    for ANO_DA_LICITACAO in ANOS:

        resultado_licitacao_tce_sagres = df_licitacao_gov_sagres(ANO_DA_LICITACAO);
        if not (resultado_licitacao_tce_sagres):
            return False;

        df_sagres = pd.read_csv('./arquivos_gerados/licitacao_sagres_'+ANO_DA_LICITACAO+'.csv', delimiter=',',  dtype='unicode')
        df_sagres.rename(columns={'cpf_cnpj_proponente': 'cpf_cnpj_fornecedor'}, inplace=True)
        df_sagres['numero_licitacao'] = df_sagres['numero_licitacao'].apply(lambda x: str(x).replace("/",""))
        df_sagres['valor_proposta'] = df_sagres['valor_proposta'].apply(lambda x: float(x))
        df_sagres['nome_municipio'] = df_sagres['nome_municipio'].replace(np.nan, '')
        df_sagres['valor_estimado_licitacao'] = df_sagres['valor_estimado_licitacao'].replace(np.nan, 0)
        df_sagres['valor_estimado_licitacao'] = df_sagres['valor_estimado_licitacao'].apply(lambda x: float(x))
        df_sagres['valor_licitado_licitacao'] = df_sagres['valor_licitado_licitacao'].replace(np.nan, 0)
        df_sagres['valor_licitado_licitacao'] = df_sagres['valor_licitado_licitacao'].apply(lambda x: float(x))
        df_sagres['objeto_licitacao'] = df_sagres['objeto_licitacao'].str.upper()
        df_sagres.fillna('', inplace=True)

        resultado_contratos_aditivos = df_aditivos_contratos_governo_pb(ANO_DA_LICITACAO);
        if not (resultado_contratos_aditivos):
            return False;

        df_gov = pd.read_json('./arquivos_gerados/df_contratos_governo_com_aditivos_pb.json', orient='table')
        df_gov['valor_proposta'] = df_gov['valor_proposta'].apply(lambda x: float(str(x).replace(".","").replace(",",".")))
        df_gov['valor_total'] = df_gov['valor_total'].apply(lambda x: float(str(x).replace(".","").replace(",",".")))

        # Associar os contratos com as licitações
        data = {}
        lista_contratos = []
        n_protocolo_licitacao = ''
        s_n_protocolo_licitacao = ''
        colunas_contratos = ['numero_contrato','numero_processo_licitatorio','data_inicio_vigencia','data_termino_vigencia',
                             'contratante','fornecedor','objeto_contrato','valor_proposta','valor_total','codigo_contrato',
                             'numero_cge','NUMERO_CONTRATO','municipio','nome_gestor_contrato','numero_portaria',
                             'data_publicacao_portaria','url_contrato','aditivos','cpf_cnpj_fornecedor','entidade_governamental','protocolo_licitacao']
        contratos_com_licitacao = pd.DataFrame(columns=colunas_contratos)
        df_contratos_sem_licitacao = []
        for s in df_sagres.itertuples():

            s_n_protocolo_licitacao = re.sub('[^0-9]', '',  s.protocolo_licitacao)
            if not (s_n_protocolo_licitacao == n_protocolo_licitacao):
                 lista_contratos = []

            cd_unidade_gestora_tce = re.sub('[^0-9]', '',  s.cd_ugestora)


            # Para cada licitação procure todos os contratos
            for g in df_gov.itertuples():

                cpf_cnpj_fornecedor = df_gov.loc[g.Index,'fornecedor'][0:18]
                cpf_cnpj_fornecedor = re.sub('[^0-9]', '', cpf_cnpj_fornecedor)
                cd_unidade_gestora_contrato = re.sub('[^0-9]', '',  g.contratante)

                if(len(cpf_cnpj_fornecedor) == 11):
                    cpf_cnpj_fornecedor = "***"+cpf_cnpj_fornecedor[3:6]+cpf_cnpj_fornecedor[6:9]+"**"

                if( ((s.cpf_cnpj_fornecedor == cpf_cnpj_fornecedor) & (s.valor_proposta == g.valor_proposta) & (cd_unidade_gestora_tce == cd_unidade_gestora_contrato) & (g.NUMERO_CONTRATO == '') ) |
                    ((s.cpf_cnpj_fornecedor == cpf_cnpj_fornecedor) & (g.numero_contrato == g.NUMERO_CONTRATO) & (s.valor_proposta == g.valor_proposta)) 
                  ):

                    df_gov.at[g.Index,'protocolo_licitacao'] = s_n_protocolo_licitacao
                    df_gov.at[g.Index,'cpf_cnpj_fornecedor'] = cpf_cnpj_fornecedor
                    df_gov.at[g.Index,'valor_proposta'] = s.valor_proposta
                    df_gov.at[g.Index,'fornecedor'] = s.fornecedor
                    

                    # adiciona o contrato encontrado em uma lista para gravar no dataframe
                    lista_contratos.append(df_gov.loc[g.Index].to_dict())

                    # dicionário de dados para checar as licitações sem contratos
                    data[s_n_protocolo_licitacao] = s_n_protocolo_licitacao

                    # adicionar os contratos encontrados em outro dataframe
                    contratos_com_licitacao.loc[g.Index] = df_gov.loc[g.Index].to_dict()


            if not (s_n_protocolo_licitacao in data):

                # colunas do dataframe quando a licitação não posssui contratos
                df_temp = df_sagres.loc[s.Index:,[
                    'fornecedor','cpf_cnpj_fornecedor','valor_proposta']]

                # adiciona o dados do tce encontrado em uma lista para gravar no dataframe
                lista_contratos.append(df_temp.loc[s.Index].to_dict())
                
            df_sagres.at[s.Index,'contratos'] = lista_contratos    
            n_protocolo_licitacao = s_n_protocolo_licitacao


        # DataFrame de contratos sem licitação
        df_contratos_sem_licitacao = df_gov.drop(contratos_com_licitacao.index)

        df_sagres = df_sagres.drop(columns=[
                                             'nome_tipo_jurisdicionado',
                                             'nome_tipo_administracao_jurisdicionado',
                                             'nome_esfera_jurisdicionado',
                                             'situacao_fracassada_licitacao',
                                             'fornecedor',
                                             'cpf_cnpj_fornecedor',
                                             'valor_proposta',
                                             'situacao_proposta'])

        df_carga_temp = df_sagres.drop_duplicates(subset=['numero_licitacao','nome_modalidade_licitacao'], keep='last')
        df_carga_temp = df_carga_temp.loc[:, ~df_carga_temp.columns.str.contains('^Unnamed')]

        if(len(df_carga.index) != 0):
            df_carga = pd.concat([df_carga,df_carga_temp])
            result = df_carga.drop_duplicates(subset=['numero_licitacao','nome_modalidade_licitacao'], keep='last')
            result = result.loc[:, ~result.columns.str.contains('^Unnamed: 0')]
            df_carga = result.copy()
            df_carga_temp = pd.DataFrame()
        else:
            df_carga = df_carga_temp.copy()
            df_carga_temp = pd.DataFrame()


    carga_mongoDB(df_carga,'licitacoes')
    return True;



    
if __name__ == '__main__':

    ANOS = ['2017','2018','2019','2020','2021']
    main(ANOS)
    

        
