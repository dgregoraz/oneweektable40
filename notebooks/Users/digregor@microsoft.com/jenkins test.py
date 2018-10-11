# Databricks notebook source
dbutils.widgets.text("testParam", "")
print(dbutils.widgets.get("testParam"))