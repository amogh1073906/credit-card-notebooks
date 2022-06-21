# Databricks notebook source
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.functions import translate	
from pyspark.sql.functions import col, substring

spark=SparkSession.builder.getOrCreate()

# COMMAND ----------

# Read credit_card CSV file into dataframe
# file_path is files path
#cred is dataframe
file_path="/FileStore/tables/Bank_credit_card_data.csv"

credit_card_data=spark.read.csv(file_path,header=True,inferSchema=True)

# COMMAND ----------

display(credit_card_data)

# COMMAND ----------

credit_card_data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Setup for Delta Lake

# COMMAND ----------

dbutils.fs.mkdirs("/creditd/")
working_dir= 'dbfs:/creditd'
output_path = f"{working_dir}/output"    

outputPathBronze = f"{output_path}/bronze"
outputPathSilver = f"{output_path}/silver"
outputPathGold   = f"{output_path}/gold"

dbutils.fs.rm(outputPathBronze,True)
dbutils.fs.rm(outputPathSilver,True)
dbutils.fs.rm(outputPathGold,True)

print(output_path)
print(outputPathBronze)
print(outputPathSilver)
print(outputPathGold)

# COMMAND ----------

#dbutils.fs.rm(outputPathGold,True)


# COMMAND ----------

# function to ingest dataframe to delta(bronze/silver/gold)
def writeToDelta(sourceDataframe, deltaPath):
    (sourceDataframe.write.format('delta').mode('overwrite').save(deltaPath))

# COMMAND ----------

# MAGIC %md
# MAGIC Ingesting raw data and save it into a Delta Lake table (Bronze)

# COMMAND ----------

# Ingesting raw data to bronze layer of delta lake.
writeToDelta(credit_card_data,outputPathBronze)

# COMMAND ----------

# Register SQL table in the database
spark.sql("DROP TABLE IF EXISTS bronze_credit_card_data")
spark.sql(f"CREATE TABLE bronze_credit_card_data USING delta LOCATION '{outputPathBronze}'") 

# COMMAND ----------

# Optimizing the table
display(spark.sql("OPTIMIZE bronze_credit_card_data"))

# COMMAND ----------

# Read the bronze table
credit_card_data_bronze = spark.read.format("delta").load(outputPathBronze)
display(credit_card_data_bronze)

# COMMAND ----------

#credit_card_data_bronze=(credit_card_data_bronze.with)

# Modifying and cleaning the bronze table.

#credit_card_data_bronze=credit_card_data_bronze.withColumn("Surname", translate(col("Surname"), "?,'", "").alias("Surname"))
credit_card_data_bronze=credit_card_data_bronze.withColumn("Surname", translate(col("Surname"), "?","").alias("Surname"))


display(credit_card_data_bronze)

# COMMAND ----------

from pyspark.sql.functions import udf
age_range = udf(lambda Age: '10s' if Age < 20 else 
                       '20s' if (Age >= 20 and Age < 30) else
                       '30s' if (Age >= 30 and Age < 40) else
                       '40s' if (Age >= 40 and Age < 50) else
                       '50s' if (Age >= 50 and Age < 60) else
                       '60s' if (Age >= 60 and Age < 70) else
                       '70s' if (Age >= 70 and Age < 80) else
                       '80s' if (Age >= 80 and Age < 90) else
                       '90s' if (Age >= 90 and Age < 100) else
                       '100+' if (Age >= 100) else '')
 
credit_card_data_bronze = credit_card_data_bronze.withColumn('Age_Group', age_range(credit_card_data_bronze.Age))
display(credit_card_data_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Save our cleaned and conformed table as a Silver table in Delta Lake

# COMMAND ----------

# After cleaning(if any) our data, we save result as silver table
writeToDelta(credit_card_data_bronze, outputPathSilver)

# COMMAND ----------

# Register SQL table in the database
spark.sql("DROP TABLE IF EXISTS silver_credit_card_data")
spark.sql(f"CREATE TABLE silver_credit_card_data USING delta LOCATION '{outputPathSilver}'") 

# COMMAND ----------

# Optimizing the table
display(spark.sql("OPTIMIZE silver_credit_card_data"))

# COMMAND ----------

# Read the silver table
credit_card_data_silver= spark.read.format("delta").load(outputPathSilver)
display(credit_card_data_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC KPIs/queries

# COMMAND ----------


#1.	Customer details with Maximum and Minimum credit score
def detMaxMinCreditScore(cc_data):
    row1 = cc_data.agg({"creditScore": "max"}).collect()[0]
    maxCreditScore=(row1["max(creditScore)"])
    row2 = cc_data.agg({"creditScore": "min"}).collect()[0]
    minCreditScore=(row2["min(creditScore)"])
    df_KPI1=cc_data.filter(cc_data.CreditScore==maxCreditScore)
    df_KPI2=cc_data.filter(cc_data.CreditScore==minCreditScore)
    df_KPI=df_KPI1.union(df_KPI2)
    #display(df_KPI)
    return(df_KPI)

#detMaxCreditScore(credit_card_data_silver)







# COMMAND ----------

#2.	Select the customer details between age 20-60

def ageBetween(cc_data):
    ageBetween_df= cc_data.filter(cc_data.Age.between(20, 60))
    return(ageBetween_df)
#kpi_2=ageBetween(df)




# COMMAND ----------

#3.	Number of male & female customers
def countOfMaleAndFemale(cc_data):
    countOfMaleAndFemale_df=(cc_data.groupBy('Gender')).count().withColumnRenamed("count","number_of_FM_cust")
    #display(countOfMaleAndFemale_df)
    return(countOfMaleAndFemale_df)
   
#kpi_3=countOfMaleAndFemale(credit_card_data_silver)




# COMMAND ----------

#4.	Number of customers with same surname

def withSameSurname(cc_data):
    withSameSurname_df=(cc_data.groupBy('Surname')).count().withColumnRenamed("count","no_of_cust_same_surname")
    #display(withSameSurname_df)
    return(withSameSurname_df)
#kpi_4=withSameSurname(credit_card_data_silver)




# COMMAND ----------

#5.	Number of customers from each country

from pyspark.sql.functions import *
def customersCountryWise(cc_data):
    #df_KPI=(cc_data.filter(cc_data.RowNumber.count).groupBy("Geography"))
    df_KPI=(cc_data.groupBy('Geography')).count().withColumnRenamed("count","number_of_customers")
    #display(df_KPI)
    return df_KPI
    
#customersCountryWise(credit_card_data_silver)

# COMMAND ----------

#6.	Average of Age, Balance, Estimated Salary
from pyspark.sql.functions import avg
def avgAge_Balance_Estimated_Salary(cc_data):
    #df_KPI=cc_data.agg({'Age':'avg','Balance':'avg','EstimatedSalary':'avg'})
    #df_KPI=cc_data.agg({'Age':'avg','Balance':'avg','EstimatedSalary':'avg'})
    #df_KPI=cc_data.select(avg('age').alias('avg'))
    df_KPI=cc_data.select(avg('age').alias('avg'),avg('Balance').alias('Bal'),avg('EstimatedSalary').alias('EstSal'))
    #display(df_KPI)
    return df_KPI



    
#avgAge_Balance_Estimated_Salary(credit_card_data_silver)


# COMMAND ----------

#7 Count of Gender who have balance more than 2000
def countOfGenderWithBalanceMoreThan2k(cc_data):
    df_KPI=((cc_data.filter(cc_data.Balance>2000)).groupBy('Gender')).count().withColumnRenamed("count","count_having_bal_gt_2k")
    display(df_KPI)
    #df_KPI.printSchema()
    return df_KPI
    
display(countOfGenderWithBalanceMoreThan2k(credit_card_data_silver))


# COMMAND ----------

#8.	Details of customers who have balance more than 2000
def customerDetailsHavingBalanceMoreThan2k(cc_data):
    df_KPI=cc_data.filter(cc_data.Balance>2000)
    #display(df_KPI)
    return df_KPI
    
#customerDetailsHavingBalanceMoreThan2k(credit_card_data_silver)

# COMMAND ----------

#9.	Customers who have tenure between 8-10 and are active members with balance more than 0
def activemem(cc_data):
    df_tenture = cc_data.filter((cc_data.Tenure >= 8) & (cc_data.IsActiveMember == 1) & (cc_data.Balance > 0))
    return df_tenture
    
#val = activemem(df)
#display(val)

# COMMAND ----------

#10.	Customers having more than average balance in their account.

def avgbal(cc_data):
    BalAverage = str(cc_data.select(avg("Balance")).collect()[0][0])
    df_avgbalance = cc_data.filter(cc_data.Balance>BalAverage)
    return df_avgbalance
#val1 = avgbal(df)
#display(val1)


# COMMAND ----------

#11.	 Customers having more than average credit score.
def avgcrscr(cc_data):
    Average = str(cc_data.select(avg("CreditScore")).collect()[0][0])
    df_avgscore = cc_data.filter(cc_data.CreditScore>Average)
    return df_avgscore
#val2 = avgcrscr(df)
#display(val2)


# COMMAND ----------

#12.	  Customers who have the highest and lowest Tenure with the bank.

def minMaxTenure(cc_data):
    # df=bnk1.select(min("Tenure")).show()
    # df=bnk1.select(max("Tenure")).show()
    cst=cc_data.select(min("Tenure").alias('minTen'), max("Tenure").alias('maxTen'))
    #display(cst)
    return cst
#minMaxTenure(credit_card_data_silver)

# COMMAND ----------

#13.	 Customers with highest and lowest number of products enrolled.

def minMaxNumOfProducts(cc_data):
    # df=bnk1.select(max("NumOfProducts")).show()
    # df=bnk1.select(min("NumOfProducts")).show()
    cst=cc_data.select(min('NumOfProducts').alias("cust_lowest_prodct"),max('NumOfProducts').alias("cust_highest_prodct"))
    display(cst)
    return cst
minMaxNumOfProducts(credit_card_data_silver)


# COMMAND ----------

#14.	 Number of customers that exited, has a credit card or is a active member
def customerExited(cc_data):
    df=cc_data.filter((cc_data.Exited == 1)&((cc_data.HasCrCard == 1) | (cc_data.IsActiveMember == 1)))
    return df
#customerExited(bnk1)


# COMMAND ----------

#15.	 Total Number of Customers with Zero Balance
def totCustWithZeroBal(cc_data):
    #df=(cc_data.filter(cc_data.Balance == 0)).count().alias("zero_bal")
    #df=cc_data.select(count('RowNumber') having cc_data.Balance == 0 groupBy('Balance'))
    df=(cc_data.select('Balance').where(cc_data.Balance==0).groupBy('Balance')).count().withColumnRenamed("count","number_of_custs_with_bal_0")
    #.withColumnRenamed("count","no_of_cust_with_bal_zero")
    #display(df)
    return df
#totCustWithZeroBal(credit_card_data_silver)
#select grpby bal a=and having 


# COMMAND ----------

#16.	 Details of Customers with Zero Balance who have not exited the plan
def custMoreThan2k(cc_data):
    df=cc_data.filter((cc_data.Balance==0) & (cc_data.Exited==0))
    return df
#custMoreThan2k(bnk1)


# COMMAND ----------

#17.	 Customers that have more than average Credit Score, Tenure, Balance & doesn't have a Credit Card
def avgOfCreditTenBal(cc_data):
    avg_credit_score = cc_data.select(avg(col("CreditScore"))).collect()[0][0]
    # display(avg_credit_score)
    avg_tenure = int(cc_data.select(avg(col("Tenure"))).collect()[0][0])
    # display(avg_tenure)
    avg_balance = cc_data.select(avg(col("Balance"))).collect()[0][0]
    # display(avg_balance)
    resultDf = cc_data.filter( (cc_data.CreditScore>avg_credit_score) & (cc_data.Tenure>avg_tenure) & (cc_data.Balance>avg_balance) & (cc_data.HasCrCard=="false"))
    return resultDf
#avgOfCreditTenBal(bnk1)

#display(function17(silver))


# COMMAND ----------

#18. Average Credit_Scores of Customers based on age groups
def avg_credit_score(age_df):
    avg_cs = age_df.groupBy("Age_Group").agg(avg("CreditScore").alias("Average_Credit_Score")).orderBy("Age_Group")
    cs= avg_cs.select("Age_Group", round(col('Average_Credit_Score'),2).alias("Average_Credit_Score"))
    return cs

# COMMAND ----------

#19.Distribution of customers exited among age groups
def exited_customers(age_df):
    exited_cust = age_df.filter("Exited == 1").orderBy("Age_Group")
    return exited_cust

# COMMAND ----------

#20. Number of Customers owning Credit Card under each age group
def own_credit_card(age_df):
    has_credit_card = age_df.where("HasCrCard == 1").groupBy("Age_Group").count().orderBy("Age_Group")
    return has_credit_card

# COMMAND ----------

#21. Number of Customers that are active based upon their age groups
def cust_active_grp(age_df):
    active_grp = age_df.where ("IsActiveMember == 1").groupBy("Age_Group").count().orderBy("Age_Group")
    return active_grp

# COMMAND ----------

#22. Average Balances of Customers based on age groups
def avg_cust_balance(age_df):
    avg_balance = age_df.groupBy("Age_Group").agg(avg("Balance").alias("Average_Balance")).orderBy("Age_Group")
    av=avg_balance.select("Age_Group", round(col('Average_Balance'),2).alias("Average_Balance"))
    return av

# COMMAND ----------

#23. Average Tenure periods of Customers based on age groups
def cust_avg_tenure(age_df):
    avg_tenure = age_df.groupBy("Age_Group").agg(avg("Tenure").alias("Average_Tenure")).orderBy("Age_Group")
    cvt = avg_tenure.select("Age_Group", round(col('Average_Tenure'),2).alias("Average_Tenure"))
    return cvt

# COMMAND ----------

#24. Average Estimated Salaries of Customers based on age groups 
def cust_est_salary(age_df):
    est_salary = age_df.groupBy("Age_Group").agg(avg("EstimatedSalary").alias("Avg_Est_Salary")).orderBy("Age_Group")
    es=est_salary.select("Age_Group", round(col('Avg_Est_Salary'),2).alias("Avg_Est_Salary"))
    return es


# COMMAND ----------

#25. Details of customers who have enrolled in more than two bank products
def cust_bank_prod(age_df):
    bank_prod = age_df.filter(age_df.NumOfProducts>2)
    return bank_prod

# COMMAND ----------

#26. Number of Male and Female customer who are Active Member
def cust_active_count(age_df):
    male_cust = age_df.filter((age_df.IsActiveMember  == 1) & (age_df.Gender  == "Male")).groupBy("Gender").count()
    female_cust = age_df.filter((age_df.IsActiveMember  == 1) & (age_df.Gender  == "Female")).groupBy("Gender").count()
    res = male_cust.union(female_cust)
    return res


# COMMAND ----------

#27. Number of Customers from Countries that have more than 5 years of Tenure
def cust_tenure(age_df):
    ctn = age_df.where ("Tenure > 5").groupBy("Geography").count()
    return ctn

# COMMAND ----------

#28. List the customers who are below 40 and are active member
def cust_active_mem(age_df):
    cam = age_df.filter((age_df.Age < 40) & (age_df.IsActiveMember == 1)).orderBy('RowNumber')
    return cam

# COMMAND ----------

#29
def cust_zero_balance(age_df):
    ccb = age_df.where ("Balance == 0").groupBy("Geography").count()
    return ccb

# COMMAND ----------

#30
def max_min_balance(age_df):
    maxi=age_df.agg({"Balance" : 'max'}).collect()[0][0]
    mx = age_df.filter(age_df.Balance == maxi)
    mini=age_df.agg({"Balance" : 'min'}).collect()[0][0]
    m = age_df.filter(age_df.Balance == mini)
    res= mx.unionByName(m)
    return res
   

# COMMAND ----------

#31 Estimated High Income Customers vs Low Income Customers
from pyspark.sql.types import FloatType
def high_low_es(age_df):
    
    max_sal = age_df.agg({"EstimatedSalary": 'max'}).collect()[0][0]
    min_sal= age_df.agg({"EstimatedSalary": 'min'}).collect()[0][0]
    res= max_sal/min_sal
    df=spark.createDataFrame([res],FloatType())
    return df


# COMMAND ----------

#32 Customers with Max Tenure, Max CreditScore, and does not have a Card
def max_tenure_cs_nocard(age_df):
    max_Ten = age_df.agg({"Tenure": 'max'}).collect()[0][0]
    max_Cred = age_df.agg({"CreditScore": 'max'}).collect()[0][0]
    res = age_df.filter((age_df.HasCrCard == 0) & (age_df.Tenure == max_Ten) & (age_df.CreditScore == max_Cred))
    return res
    

# COMMAND ----------

#33
def max_tenure_cs_hascard(age_df):
    max_Ten = age_df.agg({"Tenure": 'max'}).collect()[0][0]
    max_Cred = age_df.agg({"CreditScore": 'max'}).collect()[0][0]
    res = age_df.filter((age_df.HasCrCard == 1) & (age_df.Tenure == max_Ten) & (age_df.CreditScore == max_Cred))
    return res

# COMMAND ----------

#34 The youngest and oldest Customer having a Credit Card
def youngest_oldest_cust(age_df):
    min_Age = age_df.agg({"Age": 'min'}).collect()[0][0]
    max_Age = age_df.agg({"Age": 'max'}).collect()[0][0]
    Custcred_minage = age_df.where((age_df.HasCrCard == 1) & (age_df.Age == min_Age))
    Custcred_maxage = age_df.where((age_df.HasCrCard == 1) & (age_df.Age == max_Age))
    res = Custcred_minage.union(Custcred_maxage)
    return res

# COMMAND ----------

credit_card_data_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Generating KPI results

# COMMAND ----------

kpi_1=detMaxMinCreditScore(credit_card_data_silver)
kpi_2=ageBetween(credit_card_data_silver)
kpi_3=countOfMaleAndFemale(credit_card_data_silver)
kpi_4=withSameSurname(credit_card_data_silver)
kpi_5 = customersCountryWise(credit_card_data_silver)
kpi_6 = avgAge_Balance_Estimated_Salary(credit_card_data_silver)
kpi_7 = countOfGenderWithBalanceMoreThan2k(credit_card_data_silver)
kpi_8 = customerDetailsHavingBalanceMoreThan2k(credit_card_data_silver)
kpi_9 = activemem(credit_card_data_silver)
kpi_10= avgbal(credit_card_data_silver)
kpi_11=avgcrscr(credit_card_data_silver)
kpi_12=minMaxTenure(credit_card_data_silver)
kpi_13=minMaxNumOfProducts(credit_card_data_silver)
kpi_14=customerExited(credit_card_data_silver)
kpi_15=totCustWithZeroBal(credit_card_data_silver)
kpi_16=custMoreThan2k(credit_card_data_silver)
kpi_17=avgOfCreditTenBal(credit_card_data_silver)
kpi_18=avg_credit_score(credit_card_data_silver)
kpi_19= exited_customers(credit_card_data_silver)
kpi_20=own_credit_card(credit_card_data_silver)
kpi_21=cust_active_grp(credit_card_data_silver)
kpi_22= avg_cust_balance(credit_card_data_silver)
kpi_23=cust_avg_tenure(credit_card_data_silver)
kpi_24=cust_est_salary(credit_card_data_silver)
kpi_25=cust_bank_prod(credit_card_data_silver)
kpi_26=cust_active_count(credit_card_data_silver)
kpi_27=cust_tenure(credit_card_data_silver)
kpi_28=cust_active_mem(credit_card_data_silver)
kpi_29=cust_zero_balance(credit_card_data_silver)
kpi_30=max_min_balance(credit_card_data_silver)
kpi_31=high_low_es(credit_card_data_silver)
kpi_32=max_tenure_cs_nocard(credit_card_data_silver)
kpi_33=max_tenure_cs_hascard(credit_card_data_silver)
kpi_34=youngest_oldest_cust(credit_card_data_silver)






# COMMAND ----------

# MAGIC %md
# MAGIC Saving the specific business use cases i.e. KPIs as Gold table for efficient visualization

# COMMAND ----------

#ALTER TABLE table_name SET TBLPROPERTIES ('delta.minReaderVersion' = '2','delta.minWriterVersion' = '5','delta.columnMapping.mode' = 'name')

kpiList = [kpi_1,kpi_2,kpi_3,kpi_4,kpi_5,kpi_6,kpi_7,kpi_8,kpi_9,kpi_10,kpi_11,kpi_12,kpi_13,kpi_14,kpi_15,kpi_16,kpi_17,kpi_18,kpi_19,kpi_20,kpi_21,kpi_22,kpi_23,kpi_24,kpi_25,kpi_26,kpi_27,kpi_28,kpi_29,kpi_30,kpi_31,kpi_32,kpi_33,kpi_34]
index=1
for kpi in kpiList:
                #print(kpiList[index])
                kpiOutputPathGold=outputPathGold+"/kpi"+str(index)
                table_name="gold_kpi_"+str(index)
                print(table_name)
                writeToDelta(kpi,kpiOutputPathGold)
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                spark.sql(f"CREATE TABLE {table_name} USING delta LOCATION '{kpiOutputPathGold}'")
                spark.sql(f"OPTIMIZE {table_name}")
                index+=1;
#ALTER TABLE table_name SET TBLPROPERTIES ('delta.minReaderVersion' = '2','delta.minWriterVersion' = '5','delta.columnMapping.mode' = 'name')
    

# COMMAND ----------

def delete_mounted_dir(dirname):
    files=dbutils.fs.ls(dirname)
    for f in files:
        if f.isDir():
            delete_mounted_dir(f.path)
        dbutils.fs.rm(f.path, True)

# COMMAND ----------

delete_mounted_dir("/mnt/creditdemo/output")

# COMMAND ----------

dbutils.fs.cp(outputPathGold, "/mnt/creditdemo/output", True)

# COMMAND ----------

# MAGIC %md
# MAGIC Exiting the notebook

# COMMAND ----------

dbutils.notebook.exit("KPI Notebook Ran Successfully") 
