# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM silver.gamersclub.fs_assinatura
# MAGIC ORDER BY idPlayer, dtRef
