# EqualsAssessment

Project description
# creating a star schema design for datawarehouse and perfoming ETLs from a document database and a relational database

Project structure
# ddl/
  Creating postgres tables.py      #scripts to create all the tables for datawarehouse star schema design
  Creating Time Dimension.py       #script to create time_dim
  Creating Transaction Data Mongo DB        #script to create transactional database schema in mongo db
  Creating Transactional Data Sql Server.py      #script to create transactional database schema in sql server
    Creating Transactional DPostgresql.py      #script to create transactional database schema in postgresql
  Creating Transaction Type Dimension.py    #script to create dim_transaction_type 

#data_generation/
  Inserting Dimensional Data for Transaction Type Postgresql.py    #run script for data generation to simulate real world data
  Inserting Dimensional Data Postgresql.py    #script to generate fake data for dimensions in postgresql
  Inserting Transaction Data SQL Server.py    #script to generate transactionala data for sql server
  Inserting Transactional Data Mongo DB.py    #script to generate transactional data for mongo db

#etl/
  ETL from MongoDB to Postgresql.py      #script for whole ETL process
  ETL from SQL Server to Postgresql.py   #script for whole ETL process 

#fact_tables/
  Fact Transactional Data.py    #script to run transactional data from stagging table to fact table
  Fact Visa Transactions.py    #script to run visa transaction data from stagging table to fact table

#utils/
  Schemas Effectives.sql    # sql script that shows the effectiveness of a star schema


#Pre-requisites
to run this project ensure that the following are installed


- Python 3.x
- Apache Spark
- PostgreSQL
- Mongo Db
- Sql Server
- libraries: `pandas`, `pyspark`, `sqlalchemy`
  
