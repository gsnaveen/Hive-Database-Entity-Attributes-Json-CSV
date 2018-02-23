# Python-Hive-Database-Entity-Attributes-Json-CSV

A very convienent way of extracting Hive database, Entity and Attributes and making it searchable for the users. I had to make the schema available to users for them to be able to serach entities and attributes in hive schema. Keeping this usecase in mind I wrote a python script which connects to hive server, list down all the database and iteratively go through all the enteties to extracts attributes (all by default). This script uses [dataTables](https://datatables.net/) to display this data and makes it easy to [search](https://datatables.net/examples/api/multi_filter.html).

###### For all
```python
hive_sql_db = "SHOW DATABASES like '*'"
hive_sql_tabs_like = " LIKE '*'"
```
###### For specific schema or entities
```python
hive_sql_db = "SHOW DATABASES like 'mySchema*'"
hive_sql_tabs_like = " LIKE 'myEntity*'"
```
###### It produces 2 outputs
  HiveSchema.json used by datatable javascript and HiveSchema.csv which can be distributed in a email.
  
  ![](https://github.com/gsnaveen/Python-Hive-Database-Entity-Attributes-Json-CSV/blob/master/HiveSchemaSearch.png)
