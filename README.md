# Pipeline de Dados - Olist Databse 
<b>Alerta:</b> Os dados utilizados nesse projeto estão disponíveis no keggle neste link: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce . <br><br>
# Construção de um pipeline de dados
Neste projeto quis experimentar o processo de criação de um pipeline de dados que vai desde a extração dos dados na camada de integração de um sistema OLTP até a disponibilização deste dado no modelo estrela dentro de um Modern Data Warehouse. Neste projeto Utilizei as seguintes ferramentas: s3, airbyte, airflow, google cloud storage, python, google data transfer e Big Query. 
<br><br>

## 1. Sobre a Olist
O Ecommerce Olist é uma plataforma de comércio eletrônico brasileira que permite que vendedores ofereçam produtos em diversos marketplaces, simplificando a gestão de pedidos e fornecendo visibilidade em toda a cadeia de suprimentos.
<br><br>

### 1.1. Sobre o Projeto
O projeto tem como objetivo a transformação dos dados da estrutura OLTP do sistema de origem em um modelo estrela, utilizando um pipeline de dados otimizado. Esses dados serão então disponibilizados em um Modern Data Warehouse para permitir que as áreas de negócios os consumam de maneira analítica, fornecendo insights valiosos.

Estrutura Inicial:
<p align='center'>
    <img src = 'images/modelo_oltp.png'>
</p>

Estrutura Desejada:
<p align='center'>
    <img src = 'images/modelo_estrela.png'>
</p>

Obs: Cada uma das tabelas é representada por um banco de dados no diagrama. No entanto, é importante observar que cada objeto rna verdade é uma tabela, não em um banco de dados. O uso do banco no diagrama foi somente para representar a interação de cada tabela (chaves e relacionamentos).
<br><br>

### 1.2. Problema de Negócio
A equipe de TI implementou com sucesso uma rotina automatizada para extrair diariamente dados de um banco de dados relacional e disponibilizá-los como arquivos CSV em um bucket no Amazon S3. Minha responsabilidade como Engenheiro de Dados é criar o pipeline de dados e realizar a ingestão desses dados no BigQuery.
<br><br>

## 2. Arquitetura do Pipeline
<br><br>

<p align='center'>
    <img src = 'images/arquitetura.png'>
</p>
<br>

### 2.1. Ingestão de Dados - Airbyte
A ferramenta escolhida para fazer a ingestão de dados do Amazon S3 para o Google Cloud Storage foi o Airbyte, conforme imagem abaixo.

<p align='center'>
    <img src = 'images/airbyte_connection.png'>
</p>

O processo de implantação e configuração do Airbyte é notavelmente simples. Basta selecionar uma fonte de dados e um destino, e em seguida, configurar a conexão, incluindo a definição da periodicidade para a carga de dados. 

<br><br>

