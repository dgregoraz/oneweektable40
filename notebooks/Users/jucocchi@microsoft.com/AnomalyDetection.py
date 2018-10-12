# Databricks notebook source
# TODO: Get input parameters
input_file = "results/faulty/csv/all/part-00000-tid-6869852783684632327-eafbffa3-a0b8-44f5-abc2-a62954c431c0-47-c000.csv"

mount_point = "/mnt/adls/"
path = mount_point + input_file

# COMMAND ----------

# Import data
df = spark.read.csv(path, header=True, inferSchema=True)

df.head()

# COMMAND ----------

import matplotlib.pyplot as plt

plt.clf()
plt.plot(df.select('humidity').take(5000), df.select('pressure').take(5000), '.')
plt.xlabel('temp')
plt.ylabel('hum')
display()


# COMMAND ----------

display(df)

# COMMAND ----------

# Get the min and max values for each feature
minHumidity = df.groupby().min("Humidity").take(1)[0]["min(Humidity)"]
maxHumidity = df.groupby().max("Humidity").take(1)[0]["max(Humidity)"]
minPressure = df.groupby().min("Pressure").take(1)[0]["min(Pressure)"]
maxPressure = df.groupby().max("Pressure").take(1)[0]["max(Pressure)"]
minTemp = df.groupby().min("Temperature").take(1)[0]["min(Temperature)"]
maxTemp = df.groupby().max("Temperature").take(1)[0]["max(Temperature)"]

# COMMAND ----------

# Calculate difference
diffHum = maxHumidity - minHumidity
diffPress = maxPressure - minPressure
diffTemp = maxTemp - minTemp

# Calculate normalized columns
df = df.withColumn("normHum", ((df["humidity"] - minHumidity)/diffHum))
df = df.withColumn("normPress", ((df["pressure"] - minPressure)/diffPress))
df = df.withColumn("normTemp", ((df["temperature"] - minTemp)/diffTemp))

display(df)

# COMMAND ----------

# Select only the columns we will use for our model
df = df.select("normHum", "normPress", "normTemp")

# COMMAND ----------

import numpy as np
from sklearn.decomposition import PCA
from sklearn.covariance import EllipticEnvelope
from sklearn.preprocessing import StandardScaler
from sklearn.datasets import make_moons, make_blobs

n_samples = 100000
sample_data = np.array(df.take(n_samples))
n_features = 3

clf = EllipticEnvelope()
clf.fit(sample_data)

# COMMAND ----------

# number of outlier values
pred = clf.predict(sample_data)
sum(pred == -1)

# COMMAND ----------

# continuous output of decision_function
decision = clf.decision_function(sample_data)
decision.min(), decision.max()

# COMMAND ----------

7