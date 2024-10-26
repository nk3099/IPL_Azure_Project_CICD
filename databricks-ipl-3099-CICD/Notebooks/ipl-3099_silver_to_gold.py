# Databricks notebook source
#dbutils.fs.ls('/mnt/silver/')

# COMMAND ----------

#dbutils.fs.ls('/mnt/gold/')

# COMMAND ----------

#input_path = '/mnt/silver/'

# COMMAND ----------

#df = spark.read.format('delta').load(input_path)
#df.head(2)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,regexp_replace

# COMMAND ----------

#get list of column names
#column_names = df.columns

#for old_col_name in column_names:
    #convert column name from ColumnName to Column_Name fromat
    #new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i-1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")

    #change the column name using with ColumnRenamed and regexp_replace
    #df=df.withColumnRenamed(old_col_name,new_col_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #Doing Transformation for All Tables (Changing Column Names)

# COMMAND ----------

table_name=[]

for i in dbutils.fs.ls('/mnt/silver/'):
    table_name.append(i.name.split('/')[0])  #directory names in Silver container will get added as table_name


# COMMAND ----------

table_name

# COMMAND ----------

from pyspark.sql.functions import date_format, from_utc_timestamp
from pyspark.sql.types import TimestampType

for name in table_name:
    path = '/mnt/silver/'+name
    print(f"path{name}"+"="+path)
    df = spark.read.format('delta').load(path)
        
    column_names = df.columns

    for old_col_name in column_names:
       #convert column name from ColumnName to Column_Name fromat
       new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i-1].isupper() else char.lower() for i, char in enumerate(old_col_name)]).lstrip("_")
      
       #change the column name using with ColumnRenamed and regexp_replace
       df=df.withColumnRenamed(old_col_name,new_col_name)

    
    output_path='/mnt/gold/'+name+'/'
    df.write.format('delta').mode('overwrite').save(output_path)

# COMMAND ----------

display(df) #would print the last item in list (ie. last table)

# COMMAND ----------

# def to_snake_case(old_col_name):
#     new_col_name = []
    
#     for i, char in enumerate(old_col_name):
#         # Check if the character is uppercase and the previous character is not uppercase
#         if char.isupper() and (i == 0 or not old_col_name[i-1].isupper()):
#             if i != 0:
#                 new_col_name.append('_')  # Add underscore before the uppercase character
#             new_col_name.append(char.lower())  # Convert uppercase to lowercase
#         else:
#             new_col_name.append(char)  # Just append the character as is
    
#     # Convert the list back to a string
#     return "".join(new_col_name)


