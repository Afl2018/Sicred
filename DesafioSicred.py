# Databricks notebook source
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Criação da Base
# MAGIC %sql
# MAGIC create database if not exists DWSICRED

# COMMAND ----------

# DBTITLE 1,Deletes das Tabelas
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS DWSICRED.movimento

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS DWSICRED.cartao 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS DWSICRED.conta

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS DWSICRED.associado

# COMMAND ----------

# DBTITLE 1,Limpar Diretório para reprocessar
dbutils.fs.rm('dbfs:/user/hive/warehouse/dwsicred.db/associado', True)
dbutils.fs.rm('dbfs:/user/hive/warehouse/dwsicred.db/conta', True)
dbutils.fs.rm('dbfs:/user/hive/warehouse/dwsicred.db/cartao', True)
dbutils.fs.rm('dbfs:/user/hive/warehouse/dwsicred.db/movimento', True)

# COMMAND ----------

# DBTITLE 1,Criação das Tabelas
# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE DWSICRED.associado(
# MAGIC id INT
# MAGIC ,nome VARCHAR(50)
# MAGIC ,sobrenome VARCHAR(100)
# MAGIC ,idade INT
# MAGIC ,email VARCHAR(100) 
# MAGIC );
# MAGIC 
# MAGIC CREATE TABLE DWSICRED.conta(
# MAGIC id INT
# MAGIC ,tipo_conta VARCHAR(50)
# MAGIC ,data_criacao TIMESTAMP
# MAGIC ,id_associado INT
# MAGIC );
# MAGIC 
# MAGIC CREATE TABLE DWSICRED.cartao (
# MAGIC id INT NOT NULL
# MAGIC ,num_cartao INT NOT NULL
# MAGIC ,nom_impressao VARCHAR(100)
# MAGIC ,id_conta INT
# MAGIC ,id_associado INT
# MAGIC 
# MAGIC );
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE DWSICRED.movimento(
# MAGIC id INT NOT NULL
# MAGIC ,vlr_transacao DECIMAL(10,2) NOT NULL
# MAGIC ,des_transacao VARCHAR(255)
# MAGIC ,data_movimento TIMESTAMP
# MAGIC ,id_cartao INT 
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Inserção nas Tabelas 
# MAGIC %sql
# MAGIC INSERT INTO DWSICRED.associado (id, nome, sobrenome,idade,email)
# MAGIC VALUES (1,'JOSUE','DOS SANTOS SILVA ORLANDO',35,'JOSUE@GMAIL.COM')
# MAGIC 		,(2,'DANIEL',' SILVA ',35,'DANIELSILVA@GMAIL.COM')
# MAGIC 		,(3,'ROBERTO','CARVALHO',35,'ROBERTOCARVALHO@GMAIL.COM')
# MAGIC 		,(4,'DIEGO','RODRIGO',35,'DIEGORODRIGO@GMAIL.COM');
# MAGIC 
# MAGIC 
# MAGIC INSERT INTO DWSICRED.conta (id, tipo_conta, id_associado,data_criacao)
# MAGIC VALUES (1,'CORRENTE',5,  '2022-05-03T00:00:00.000-0300')
# MAGIC 		,(2,'SALARIO',4, '2021-04-01T00:00:00.000-0300' )
# MAGIC 		,(3,'POUPANCA',1,'2022-09-05T00:00:00.000-0300')
# MAGIC 		,(4,'SALARIO',4, '2021-06-07T00:00:00.000-0300' );
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC INSERT INTO DWSICRED.cartao(id, num_cartao, nom_impressao,id_conta,id_associado)
# MAGIC VALUES (1,'147258369',541,1,2)
# MAGIC 		,(2,'963852741',459,3,3)
# MAGIC 		,(3,'789456123',473,4,2)
# MAGIC 		,(4,'987654321',471,2,4);
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC INSERT INTO DWSICRED.movimento(id, vlr_transacao, des_transacao,data_movimento,id_cartao)
# MAGIC VALUES (1,'3400.99','DOC','2022-05-03T00:00:00.000-0300',1)
# MAGIC 		,(2,'5901.99','TED','2021-04-01T00:00:00.000-0300',3)
# MAGIC 		,(3,'1080.00','DEPOSITO','2022-09-05T00:00:00.000-0300',2)
# MAGIC 		,(4,'1140.44','SAQUE','2021-06-07T00:00:00.000-0300',4);

# COMMAND ----------

cmd  = "select " 
cmd  += "ass.nome       as Nome "
cmd  += ",ass.sobrenome as SobreNome"
cmd  += ",ass.idade     as Idade"
cmd  += ",ass.email     as Email"
cmd  += ",cast(mov.vlr_transacao as float) as ValorTransacao "
cmd  += ",mov.des_transacao                as DescricaoTransacao "
cmd  += ",mov.data_movimento               as DataMovimento "
cmd  += ",cart.num_cartao                  as NumeroCartao "
cmd  += ",cart.nom_impressao               as NumeroImpressao "
cmd  += ",cont.tipo_conta                  as TipoConta "
cmd  += ",cast(cont.data_criacao as date)  as DataCriacao "
cmd  += "from DWSICRED.cartao cart inner join DWSICRED.associado ass on ass.id = cart.id "
cmd  += "                          inner join DWSICRED.conta cont   on cont.id = cart.id "
cmd  += "                          inner join DWSICRED.movimento mov   on mov.id = cart.id "
df = spark.sql(cmd)
df.createOrReplaceTempView('DWSICRED')

# COMMAND ----------

pandas_df = df.toPandas()

# COMMAND ----------

pandas_df.to_csv('C://Users//josue.orlando/OneDrive - DOr Consultoria//Documentos//Comprovante de rendimento//sicre.csv')
