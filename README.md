# Airflow com Python, Spark, MySQL, Postgres e Docker

Este projeto tem aintenção de mostrar como algumas tecnologias funcionam orquestradas

## Estrutura

 Neste projeto nos temos duas `dags`, sendo uma em construção, sendo a completa conta com:

- `start`: Um operador python que cria um log em banco de dadoos com informações de quando foi realizado o início.
- `PythonOperator`: Um operador para rodar algumas rotinas python
- `python_job`: Este operado lança para o spark-master um job.
- `MySQL_operator`: operador utilizado para validação de funcionamento do banco
- `end`:  Um operador python que cria um log em banco de dadoos com informações de fim do ciclo do airflow.



## Requisitos

Before setting up the project, ensure you have the following:

- Docker
- Imagem do Airflow, Spark, MySQL e Postgres
- Configurar volumes no docker compose para o Spark Jobs e para as Dags


### Notas:
Para configurar a comunicação entre o Airflow e Spark e os SGBDs, deve-se configurar as suas respectivas configurações dentro da UI do Airflow. Para os jobs spark, essa configuração não é necessária, podendo assim serem realizadas via código

## Crétditos
A estrutura basicia de pastas e os codigos java e scala são de direitos intelectuais de Yusuf Guanyou, sendo este um fork do projeto base.
