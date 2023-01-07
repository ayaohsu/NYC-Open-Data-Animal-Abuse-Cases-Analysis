# Analyze NYC 311 Requests Regarding the Yearly Jewish Kaporos Ritual

- [Introduction](#introduction)
- [Architecture](#architecture)
- [Data Model](#data-model)
- [Testing](#testing)
- [Dashboard](#dashboard)
- [Usage](#usage)

## Introduction

This is my personal project to get some insights of the complaints during the Jewish ritual of kaporos in NYC and also to familiarize myself with the usage of data engineering tools.

[Kaporo (ro Kapparot)](https://en.wikipedia.org/wiki/Kapparot) is a traditional practice by some orthodox Jewish people to use chickens as a symbol of atonement. The chickens are slaughtered after the ritual in the streets. It is estimated that at least 60,000 chickens are slaughtered only in Brooklyn every year [source](https://www.adoptakaporossurvivor.com/whatiskaporos). This is not only a cruelty to animals, but also a potential cause of public health crisis as the dead birds, blood, body parts are all on the slaughter sites, in the public streets. For more information about kaporos, please read this [post](https://www.adoptakaporossurvivor.com/whatiskaporos).



If you have any questions or comments, please feel free to reach me at ayao780607@gmail.com!

## Architecture


## Data Model

## Testing

## Dashboard

## Usage

spark-submit \
  --jars /usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-redshift.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar \
  src/main.py \
  1>output.log \
  2>spark.log
