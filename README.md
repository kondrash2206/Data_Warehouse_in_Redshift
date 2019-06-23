# Data_Warehouse_in_Redshift
Creating a Data Warehouse using data from AWS S3 and staging in Redshift 

# Description
In this project I have created an ETL Pipeline to fill an AWS Redshift based Data Warehouse for an imaginary music streaming app Sparkify.
First, I have transferred data from Udacity public AWS S3 bucket "udacity-dend" containing multiple .json files into AWS Redshift using Python. Json files were obtained from ["Million Song Dataset"](http://millionsongdataset.com/) and ["Music streaming app event simulator"](https://github.com/Interana/eventsim). 

As a result two staging tables were created and filled: **staging_songs** (from song dataset .jsons) and **staging_events** (from event simulator jsons). Then I used these tables to distribute the data into a star chema tables. Fact Table: **songplays**; Dimension Tables: **users**,**songs**, **artists**, **time**. As a result a Redshift-based Data Warehouse was created that 
is of huge benefit for an imaginary music streaming app "Sparkify" due to following reasons:
* Provides information about user activities over times of day and user location which makes possible to correctly arrange resources
* Provides information about user song and band preferences which allow to develop a recommendation system (rank based recomendations, user-user collaborative filtering e.t.c)
* Provides information about user subscriptions. The behaviour of users that cancelled the subscription can be analysed and this knowldege can be used to identify users that might canceln their membership in the future. Such users could recieve some benefits (discounts, gifts etc) which prevents the cancelation of their membership. 

# Database Schema 
The star schema below allows to answer the above stated questions:
![](https://github.com/kondrash2206/Data_Warehouse_in_Redshift/blob/master/shema.png)
(* "D" - distribution key, "S" - sortkey)

# ETL Pipeline
ETL Pipeline gets the data from 2 datasources and fills 4 Dimension Tables (**Songs**, **Artists**, **Time**, **User**). The **Songplays** Table is filled also from **Songs** and **Artists** Tables.
![](https://github.com/kondrash2206/Data_Warehouse_in_Redshift/blob/master/table_sources.png)


# Files
* **sql_queries.py** -collection of all SQL Querries used in table manipulation and etl
* **create_tables.py** - file that connects to a Redshift, drops all existing tables and creates new tables:  2 staging tables, 1 fact table and 4 dimension tables
* **etl.py** - ETL (Extract Transfer Load) Pipeline that fills the previously created staging tables with data from json files. Fact and dimension tables are filled from staging tables.
**dwh.cfg** - configuration file, containing the information needed to connect a Redshift cluster as well as access public S3 bucket. 

# Installations
In order to run this project following python libraries are needed: psycopg2, pandas. To run it first start Redshift Cluster, add all necessary information into "dwh.cfg" file, start "create_tables.py", this creates defined database and tables. And then run "etl.py" that fills the tables with data. 

### Acknowledgements
This project is a part of Udacity "Data Engineering" Nanodegree
