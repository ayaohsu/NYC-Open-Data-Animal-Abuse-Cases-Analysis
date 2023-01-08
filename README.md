# NYC 311 Requests Analysis for the Kaporos Ritual

- [Introduction](#introduction)
- [Architecture](#architecture)
- [Data Model](#data-model)
- [Testing](#testing)
- [Results](#results)

## Introduction

[Kaporo](https://en.wikipedia.org/wiki/Kapparot) is a traditional practice by some orthodox Jewish people to use chickens as a symbol of atonement, or to transfer one's sins to. The chickens are slaughtered after the ritual in the streets. It is estimated that at least 60,000 chickens are slaughtered only in Brooklyn every year. This is not only a cruelty to animals, but also a potential cause of public health crisis as the dead birds, blood, body parts are all on the slaughter sites, on public streets. For more information about kaporos, please read this [post](https://www.adoptakaporossurvivor.com/whatiskaporos).

In this project, I digged into 311 service requests from [NYC Open Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9) and tried to understand more about the kaporos-related complaints and discover the major sites where the ritual happens. To do that, I built a data pipeline in AWS ecosystem and put up a QuickSight dashboard to visualize the results.

If you would like to get the access to the dashboard, or if you have any questions or comments, please feel free to reach me at ayao780607@gmail.com!

Click [here](https://us-east-1.quicksight.aws.amazon.com/sn/accounts/607143918644/dashboards/ce780170-26a3-4382-8461-1db5d34ae445) to view the dashboard (permission required).


## Architecture

![NYC Open Data -> S3 -> EMR & Spark -> Redshift -> QuickSight](figures/311-analysis-architecture.png)

- TODO why do we choose this stack

## Data Model

Dimension Tables: `dim_complaint_type`, `dim_date`, `dim_location_type`
Fact Table: `fact_service_request`

I chose a star schema to model various aspects of the requests. The date dimension table has other attributes such as year/month/day of the year so it is easier for end users to query against those fields (such as count of animal torture cases in a particular month for each year).

![data model diagram](figures/311-analysis-architecture-data-model.png)

## Testing

**End-to-end system testing** is in place to ensure the pipeline does not break when adding new features. This is done by mocking the sample data when reading from NYC Open Data API, running the pipeline, and verify in the data warehouse to see if the output is expected. The test can be carried out by running `pytest` manually.

I also wrote some **data quality testing** to make sure the transformed data meets the business constraints and rules, for example, if the request dates of all the requests meet our query to the data source.

## Results
