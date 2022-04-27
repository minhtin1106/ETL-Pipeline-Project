# ETL-Pipeline-Project
This project demonstrates the idea of constructing the ETL pipeline to fetch data from multiple sources, performing transformation on them before loading to the datawarehouse

__Introduction__:<br>

It is very likely that as a Data Analyst, before being able to conduct any classy analyses to find insights from the data,  we would have a need to pool data we need from multiple sources. Given raw data whose format varies, some preprocessing and transformation are required.<br> 
Taking into account that maybe in the future, the cleaned and well structured data of this form would be needed again and we do not want to start everything from scratch, it would be great that at this point, we load the data to the database and write some generic scripts showing how to handle the whole pipeline of extracting, transforming and loading data of this kind. By doing that, we save a ton of efforts next time when we need to collect and transform the same type of data.<br>
That is the motivation for me to do this work: To try construct the ETL pipeline in Python


__Tasks__:<br>
+ Extract data from multiple sources (i.e: load CSV file from local computer, use API to fetch json data from website )
+ Transform data (i.e: drop null values, select relevant columns, apply aggregate functions on the data)
+ Load data to the PostgreSQL database
+ Apply principles of object-oriented programming to create scripts that are configurable, loosely-coupled, and scalable

__Results and Impacts__:<br>

The ETL pipeline could help save a great deal of time by automating the tasks of collecting, preprocessing and loading data


For more details of the work, see my notebook.




 

