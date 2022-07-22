# Databricks notebook source
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/input/")
dbutils.fs.mkdirs("/FileStore/tables/output/")

# COMMAND ----------

input_path = "/FileStore/tables/input/combined_subject_vs_lab_with_sdoh_copy_fillna2.parquet"
output_path = "/FileStore/tables/output/"
feature_score_path = "/FileStore/tables/input/feature_score.csv"

# COMMAND ----------

spark.conf.set('spark.sql.caseSensitive', True)
df = spark.read.format("delta").parquet(input_path)
df = df.drop("unnamed:_0")
display(df)

# COMMAND ----------

feature_score = spark.read.format("csv").option("header","true").load(feature_score_path)
display(feature_score)

# COMMAND ----------

# MAGIC %md
# MAGIC Feature Store Analysis

# COMMAND ----------

df_chf = df.groupBy("chf").count()
display(df_chf)

# COMMAND ----------

from databricks import feature_store
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
import functools

def chf(data,feature_scores):
    selected_feature = ["patient_id"]
    first_row = feature_scores.first()
    for feature in feature_scores.columns:
        if float(first_row[feature]) >= 0.02:
            selected_feature.append(feature)
    selected_data = data.select(selected_feature)
    print(selected_data.count())
    selected_data = selected_data.dropDuplicates(['patient_id'])
    print(selected_data.count())
    return selected_data
    
chf_features = chf(df,feature_score)
display(chf_features)
"""
def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

#Select the features needed in the machine learning process
def feature_selection(df):
    df_chf_0 = df.filter(df.chf == "0.0").limit(100)
    df_chf_1 = df.filter(df.chf == "1.0").limit(100)
    df_chf_2 = df.filter(df.chf == "2.0").limit(100)
    df_chf_3 = df.filter(df.chf == "3.0").limit(100)
    df_chf_4 = df.filter(df.chf == "4.0").limit(100)
    df_chf_5 = df.filter(df.chf == "5.0").limit(100)
    df_chf_6 = df.filter(df.chf == "6.0").limit(100)
    df_chf_7 = df.filter(df.chf == "7.0").limit(100)
    df_chf_8 = df.filter(df.chf == "8.0").limit(100)
    df_chf_9 = df.filter(df.chf == "9.0").limit(100)
    df_chf_10 = df.filter(df.chf == "10.0").limit(100)
    df_chf_11 = df.filter(df.chf == "11.0").limit(100)
    df_chf_12 = df.filter(df.chf == "12.0").limit(100)
    df_chf_13 = df.filter(df.chf == "13.0").limit(100)
    
    df_chf_total = df_chf_0.union(df_chf_1).union(df_chf_2).union(df_chf_3).union(df_chf_4) \
                    .union(df_chf_5).union(df_chf_6).union(df_chf_7).union(df_chf_8).union(df_chf_9) \
                    .union(df_chf_10).union(df_chf_11).union(df_chf_12).union(df_chf_13)
    
    print("done")
    #df_chf_total = unionAll([df_chf_0,df_chf_1,df_chf_2,df_chf_3,df_chf_4,df_chf_5,df_chf_6,df_chf_7,df_chf_8,df_chf_9,df_chf_10,df_chf_11,df_chf_12,df_chf_13])
    #return df_chf_total
    return [df_chf_0,df_chf_1,df_chf_2,df_chf_3,df_chf_4,df_chf_5,df_chf_6,df_chf_7,df_chf_8,df_chf_9,df_chf_10,df_chf_11,df_chf_12,df_chf_13]
    

#features = feature_selection(df)
#print(len(features))
#print(features.count())
#display(features)
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a database to store the feature tables
# MAGIC CREATE DATABASE IF NOT EXISTS feature_store_database;

# COMMAND ----------

# Feature Store client
fs = feature_store.FeatureStoreClient()

fs.create_table(
    name="feature_store_database.chf_features",
    primary_keys=["patient_id"],
    df=chf_features,
    partition_columns = "chf",
    description="CHF features table",
)

# COMMAND ----------

fs.write_table(
  name="feature_store_database.chf_features",
  df=chf_features,
  mode="overwrite",
)

# COMMAND ----------

# MAGIC %md
# MAGIC Training a model

# COMMAND ----------

#Create Feature Lookup
from databricks.feature_store import FeatureLookup
import mlflow

features_table = "feature_store_database.chf_features"

feature_lookups = [
    FeatureLookup( 
      table_name = features_table,
      #feature_names = "ischemic",
      lookup_key = ["patient_id"],
    ),
]

# COMMAND ----------

# Create Training Dataset
mlflow.end_run()
mlflow.start_run() 


training_set = fs.create_training_set(
  df,
  feature_lookups = feature_lookups,
  label = "chf",
  exclude_columns = ["patient_id"]
)

training_df = training_set.load_df()

#training_df = chf_features.drop("patient_id")

# COMMAND ----------

#Train the model using SKLearn
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.feature_selection import SelectKBest, f_classif
import pandas as pd

data = training_df.toPandas()
train,test = train_test_split(data,test_size=0.3, random_state=123)
X_train = train.drop(["chf"], axis=1)
X_test = test.drop(["chf"], axis=1)
y_train = train.chf.astype('float').astype("int")
y_test = test.chf.astype('float').astype("int")

model = LogisticRegression(multi_class="multinomial",solver="lbfgs",max_iter=10000).fit(X_train, y_train)
score = model.score(X_test, y_test)
print(score)


"""
def generator(datal,n):
    result = []
    for i in range(14):
        if i != n:
            result.append(datal[i].head(8))
    dat = pd.concat(result,axis=0)
    dat.loc[dat["chf"] != None, 'chf'] = float(i) + 1.0
    return dat


datas = []
for feature in features:
    data = feature.toPandas()
    datas.append(data)
    
training_data = []
for i in range(14):
    data1 = datas[i]
    data2 = generator(datas,i)
    data = pd.concat([data1,data2])
    training_data.append(data)
    #print(data1.shape)
    #print(data2.shape)
    X = data.drop(["chf"],axis=1)
    print(X.shape)
    y = data.chf.astype("float").astype("int")
    X_new = SelectKBest(f_classif, k=200).fit_transform(X, y)
    print(X.shape)
    

feature_0 = features[0]
data1 = feature_0.toPandas()

feature_1 = features[1]
data2 = feature_1.toPandas()
data = pd.concat([data1, data2], axis=0)
print(data.shape)



#data = features.toPandas()
train,test = train_test_split(data,test_size=0.3, random_state=42)

X_train = train.drop(["chf"], axis=1)
X_test = test.drop(["chf"], axis=1)
y_train = train.chf
y_test = test.chf
print(X_train.shape,y_train.shape)
print(X_test.shape,y_test.shape)
"""

# COMMAND ----------

fs = feature_store.FeatureStoreClient()
fs.log_model(
    model,
    artifact_path="model_packaged",
    flavor=mlflow.sklearn,
    training_set=training_df,
    registered_model_name="chf_prediction_logistic_regression_model"
)

# COMMAND ----------


