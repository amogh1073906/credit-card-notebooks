# Databricks notebook source
# mounting adls gen2 directory to store the generated results
try:
    dbutils.fs.mount(
    source = "wasbs://creditdemo@simulationdevicedata.blob.core.windows.net",
    mount_point = "/mnt/creditdemo",
    extra_configs = {"fs.azure.account.key.simulationdevicedata.blob.core.windows.net": "Bd7hAg/1GZ5B824iPKpdWtDxkVlELJ71I90kkYZcckYR3zRilKf8Jk9dCGmpx/SZhEJbbCS1bWop1ThPDtwqFg=="})
except:
    pass

# COMMAND ----------

dbutils.notebook.exit("Utils Ran Successfully")
