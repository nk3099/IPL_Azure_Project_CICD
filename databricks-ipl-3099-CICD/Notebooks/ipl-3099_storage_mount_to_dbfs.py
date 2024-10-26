# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake Storage to DBFS
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using (Storage) Account Key

# COMMAND ----------

configs = {
  "fs.azure.account.key.ipl3099datalakegen2.blob.core.windows.net": dbutils.secrets.get(scope="ipl_project_scope", key="iplAccountKeySecret")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "wasbs://bronze@ipl3099datalakegen2.blob.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)


"""
can use 'updateMoun't to change the 'source' and can use same mount_point:

  dbutils.fs.updateMount(
    source="wasbs://bronze@ipl3099datalakegen2.blob.core.windows.net/dbo",
    mount_point="/mnt/bronze"
    extra_configs = configs)

"""

# COMMAND ----------

#to check if mount point exists:

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze/dbo/')

# COMMAND ----------

# MAGIC %md
# MAGIC silver and gold mount_points
# MAGIC

# COMMAND ----------

configs = {
  "fs.azure.account.key.ipl3099datalakegen2.blob.core.windows.net": dbutils.secrets.get(scope="ipl_project_scope", key="iplAccountKeySecret")
}

dbutils.fs.mount(
  source = "wasbs://silver@ipl3099datalakegen2.blob.core.windows.net/",
  mount_point = "/mnt/silver",
  extra_configs = configs)

# COMMAND ----------

configs = {
  "fs.azure.account.key.ipl3099datalakegen2.blob.core.windows.net": dbutils.secrets.get(scope="ipl_project_scope", key="iplAccountKeySecret")
}

dbutils.fs.mount(
  source = "wasbs://gold@ipl3099datalakegen2.blob.core.windows.net/",
  mount_point = "/mnt/gold",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Service Principal (SP)

# COMMAND ----------

#storageaccount: ipl3099datalakegen2
#oauth2.client : 'client' means Service Principal

spark.conf.set("fs.azure.account.auth.type.ipl3099datalakegen2.dfs.core.windows.net", "OAuth") 

spark.conf.set("fs.azure.account.oauth.provider.type.ipl3099datalakegen2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

# Application (client) ID / Service Prinicipal Id
spark.conf.set("fs.azure.account.oauth2.client.id.ipl3099datalakegen2.dfs.core.windows.net", "e9480eba-9747-4537-bbf4-6b57c9f4a9d3") 

spark.conf.set("fs.azure.account.oauth2.client.secret.ipl3099datalakegen2.dfs.core.windows.net", dbutils.secrets.get(scope="ipl_project_scope",key="iplServicePrincipalSecret"))

#Directory (tenant) ID of Service Principal
spark.conf.set("fs.azure.account.oauth2.client.endpoint.ipl3099datalakegen2.dfs.core.windows.net", "https://login.microsoftonline.com/f4690ab4-a188-40aa-8bd8-b06bbd1cef70/oauth2/token") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Shared Access Signature (SAS)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.ipl3099datalakegen2.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.ipl3099datalakegen2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.ipl3099datalakegen2.dfs.core.windows.net",dbutils.secrets.get(scope="ipl_project_scope",key="iplSASsecret"))


# COMMAND ----------

dbutils.fs.ls('/mnt/bronze/dbo/')

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://bronze@ipl3099datalakegen2.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = {"fs.azure.sas.fixed.token.ipl3099datalakegen2.dfs.core.windows.net":dbutils.secrets.get(scope="ipl_project_scope",key="iplSASsecret")}
  )

  #Databricks Runtime 3.5x, which is quite old (and likely quite limited in terms of newer protocols), it does not support the abfss protocol or potentially even abfs for ADLS Gen2. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Credential Passthrough 
# MAGIC
# MAGIC => when creating databricks cluster -> enable "Credential Passthrough" in Advanced options

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

dbutils.fs.mount(
  source = "abfss://bronze@ipl3099datalakegen2.dfs.core.windows.net/",
  mount_point = "/mnt/bronze>",
  extra_configs = configs)

#Azure Data Lake Storage credential passthrough --> Available on Azure Databricks premium
