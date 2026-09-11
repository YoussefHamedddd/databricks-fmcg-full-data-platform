# Enterprise Data Integration & Analytics Platform

## Project Overview

This project demonstrates an enterprise data integration scenario where a Parent Company acquires a Child Company and needs to integrate the Child Company's data into the existing Parent data platform.

The main objective is to process the Child Company's operational data, transform it into a business-ready structure, and merge it into the Parent Company's existing Gold layer so both companies can be analyzed through a unified analytics platform.

The project focuses primarily on `fact_orders`, because orders are the main transactional dataset that continuously receives new records.

## Business Case

The Parent Company already has an established data platform and an existing Gold data model.

After acquiring the Child Company, the Child's operational data needs to be integrated with the Parent Company's existing analytical model.

The business requirement is:

> Integrate the newly acquired Child Company's data into the existing Parent analytical platform and update the Parent Gold data when new Child order data arrives.

The Child Company provides orders, customers, products, and pricing data. The data is initially available in AWS S3 and processed through Databricks before being integrated into the Parent Gold layer.

The final result is a unified analytical environment consumed through dashboards and AI-assisted analytics.

## Solution Architecture

![Data Platform Architecture](docs/images/data_platform_architecture.png)

The overall flow is:

```text
Child OLTP
    |
    v
AWS S3 - Landing
    |
    +----------------------+
    |                      |
 Full Load          Incremental Load
    |                      |
    v                      v
Bronze Orders       Bronze Staging Orders
    |                      |
   MERGE                 APPEND
    |                      |
    v                      v
Silver Orders       Silver Staging Orders
    |                      |
    +----------+-----------+
               |
               v
        Child Gold Orders
               |
              MERGE
               |
               v
        Parent Gold Orders
               |
        +------+------+
        |             |
      Views         Genie
        |             |
        +------+------+
               |
               v
           Dashboard
