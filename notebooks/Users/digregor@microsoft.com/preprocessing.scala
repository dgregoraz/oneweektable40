// Databricks notebook source
print("this is version 1.1")


// COMMAND ----------

val veryNestedDF = Seq("Diego", "Daniel").toDF()

//veryNestedDF.show()
HelloWorld.withGreeting(veryNestedDF)