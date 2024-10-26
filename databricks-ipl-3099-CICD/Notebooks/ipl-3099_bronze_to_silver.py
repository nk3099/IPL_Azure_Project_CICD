# Databricks notebook source
#dbutils.fs.ls('mnt/bronze/dbo')

# COMMAND ----------

#input_path='/mnt/bronze/dbo/Player/Player.parquet'

# COMMAND ----------

#df=spark.read.format('parquet').load(input_path)
#df.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #Spark Code

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,IntegerType,StringType,BooleanType,DateType,DecimalType
from pyspark.sql.functions import col,when,sum,avg,row_number
from pyspark.sql.window import Window

spark=SparkSession.builder.appName("ipl_neeraj").getOrCreate()

# COMMAND ----------

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

     

# COMMAND ----------

# MAGIC %md 
# MAGIC # Tables Schema

# COMMAND ----------


team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

#or team_schema = "team_sk integer, team_id integer, team_name string"


# COMMAND ----------

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

# COMMAND ----------

ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True), #True means can have NULL values
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

# COMMAND ----------

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])


# COMMAND ----------

# MAGIC %md
# MAGIC #Doing Transformations for All Tables

# COMMAND ----------

schemas_dict = {
    'Player': player_schema,
    'Player_match': player_match_schema,
    'Ball_By_Ball': ball_by_ball_schema,
    'Match': match_schema,
    'Team': team_schema
}

# COMMAND ----------

table_name=[]

for i in dbutils.fs.ls('/mnt/bronze/dbo/'):
    table_name.append(i.name.split('/')[0])  #directory names in Bronze container will get added as table_name

# COMMAND ----------

table_name

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver Transformation

# COMMAND ----------

from pyspark.sql.functions import date_format, from_utc_timestamp
from pyspark.sql.types import TimestampType

for i in table_name:
    path = f'/mnt/bronze/dbo/{i}/{i}.parquet'
    #schema = schemas_dict.get(i)
    
    # Read the Parquet file using the schema if available
    #if schema:
    df1 = spark.read \
            .option("mergeSchema", "true") \
            .format('parquet') \
            .load(path)
            #.schema(schema) \
    #else:
        #print(f"No schema found for {i}")
        #df1 = spark.read.parquet(path)
    
    # Print the schema to verify types
    #df1.printSchema()
    
    columns = df1.columns
    print(f"Columns for {i}: {columns}")

    # Process date columns
    for col in columns:
        if "Date" in col or "date" in col:
            if col in df1.columns:
                try:
                    # Cast the column to TimestampType (if possible)
                    df = df1.withColumn(col, date_format(from_utc_timestamp(df1[col].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))
                except Exception as e:
                    print(f"Error processing column {col}: {e}")
            else:
                print(f"Column {col} not found in DataFrame")

    # Writing the data to the Delta table
    output_path = f'/mnt/silver/{i}/'
    df.write.format('delta').mode('overwrite').save(output_path)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW silver.dbo.PlayerView AS
# MAGIC SELECT * FROM silver.dbo.Player 
# MAGIC LIMIT 3;
