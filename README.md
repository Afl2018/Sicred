<h1 align="center"> Desafio técnico engenharia de dados </h1>

## Desafio
modelar a estrutura descrita no desafio em um banco de dados, após isso criar um componente capaz de ler os dados das tabelas criadas e escrever e 
um único arquivo flat, em um diretório local, ou seja, todas as tabelas devem ser agregadas para formar 
uma visão única


## Porque optou por determinado design ? 
Optei por criar um cluster na plataforma de análise de dados otimizada do Databricks, porque poderia criar o cluster em Spark e processa-los com     grande desempenho
  <div align="center">
  <img src="https://user-images.githubusercontent.com/36649082/167301950-95d4bd98-591f-45d0-b4a6-bd08808a9bf4.PNG" width="700px" />
  </div>

## O que faria se tivesse mais tempo para concluir o desafio ?
Iria criar um serviço no AZURE, criar um banco de banco de Dados em Sql e também um notebook no DataBricks para processar em Spark e construir pipelines no Datafactory para fazer a orquestração do meu projeto. 
Tentaria também criar um esteira de processamento para Provisionar  



## dificuldades que encontrou no desenvolvimento ? 
Minhas dificuldades foi que já tinha utilizado os USD200 que Azure disponibilizar para executar projetos Free, ai não conseguir criar os pipelines criar os banco de dados AzureSQL, DataFactory, Databricks e Azure  Blob   Storage.
Então criei um Notebook no Databricks para construção do desafio, Criando uma Base DWSICRED, construí a tabelas executando script em Sql e aplicando ETL em SparkSql transformei um DataFrame em Pandas

