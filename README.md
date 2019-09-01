
# Capstone Project – Udacity Data Engineering Nanodegree

## Summary


In the project I decided to work with a set of data related to fuel prices in Brazil over time. The original data can be found in the Brazilian government's [open data](http://dados.gov.br/)  program.


## Data


The dataset consists of json / CSV files divided into three themes:

- Events: Composed by the data corresponding to the purchase and sale price of fuels divided by region, state, municipality, resale, date of collection, product type and distributor.

- States: Composed of economic and population data of each state.

- Region: Composto por dados referentes a área em Km de cada região.

## Scope
The project makes use of three different tools to fulfill different purposes:

Storing Original Data: Original data is stored in an S3 bucket.

Transformation, Storage, and Query: Original data is first transported and stored in stage tables in Redshift and later transformed and made available in dimension and fact tables forming a Star Schema.

Below you can check the data dictionary and the data model.

![capstone_star](img/capstone_star.jpg)

Data flow management: For this task I use airflow.  Below you can see a DAG for our data flow.

These tools are widely adopted by the industry, with airflow being an open source tool.

![capstone_airflow](img/capstone_airflow.jpg)

## Questions
****The data was increased by 100x:****

****Answer**** 
We can easily resize our cluster to make the best use of the computing and storage options that Redshift provides.

 ****The pipelines would be run on a daily basis by 7 am every day.****
 
****Answer****
 We can achieve the goal by using the airflow task scheduler.
    
****The database needed to be accessed by 100+ people.****
****Answer****
  Redshift was designed to be highly scalable. We can deliver fast query responses for particular scenarios.

## Prerequisites

The Code is written in Python 3.6.3 . If you don't have Python installed you can find it [here]. If you are using a lower version of Python you can upgrade using the pip package, ensuring you have the latest version of pip.

To install pip run in the command Line:
```sh
$ python -m ensurepip -- default-pip
```
To upgrade pip:
```sh
$ python -m pip install -- upgrade pip setuptools wheel
```
To upgrade Python:
```ssh
$ pip install python -- upgrade
```
Additional Package: Psycopg2. You can donwload them using pip Psycopg2:
```ssh
$ pip install psycopg2
```
You also need a Redshift Cluster up and running. You can read the [documentation](https://docs.aws.amazon.com/pt_br/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html) and configure your own cluster.





[//]: #

   [here]: <https://www.python.org/downloads/>