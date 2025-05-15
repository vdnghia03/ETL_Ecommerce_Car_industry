# Ecommerce_ETL_Analysis


## Introduction

### Project Requirements
Project nhằm xây dựng End-to-End data pipeline theo mô hình ETL (Extract - Transform - Load) kết hợp thực hiện việc phân tích và trực quan hóa dữ liệu trên Dashboard

Project phân tích dựa trên bộ dữ liệu về giả lập về data của một hệ thống bán các loại gồm có data về customer, data product và data thông tin bán hàng sales detail, đưa ra cái nhìn tổng quan về hoạt động của cửa hàng.

## Overview

### Data 

- Nguồn raw data ban đầu:
![ETL](/Docs/Integration_Model.png)

- Flow xây dựng data warehouse từ nguồn
![ETL](/Docs/Data_Flow.png)

### ETL Platform
![ETL](/Docs/ETL_ERM_Full.png)

Trong đó:
- **E**: Extract process, quá trình này lấy data từ **MySQL** để xử lý và tạm lưu trử ở **MinIO.**
- **T**: Transform process, thực hiện xử lý, sàn lọc và truy vấn các thông tin có giá trị từ raw data với **MinIO** là object storage với quy trình **Medalion Architechture** để xây dựng **Data warehouse**
- **L**: Loading process, đẩy data sạch vào **PostgreSQL** và cũng dùng đó là Data warehouse. Sau đó ta sử dụng **dbt** để test data quality và thực hiện các transform cần thiết để được dữ liệu cuối cùng cho phân tích và báo báo.

### Technologies Used
- **Data Orchestration**: [Dagster](https://dagster.io/)
- **Containerized Application Development**: [Docker](https://www.docker.com/)
- **Database**: [MySQL](https://www.mysql.com/), [PostgreSQL](https://www.postgresql.org/)
- **Object Storage**: [MinIO](https://min.io/)
- **Data Dashboard and Analysis**: [Metabase](https://www.metabase.com/)
- **Data Quality**: [dbt](https://www.getdbt.com/)

## Prerequisite
1. [Docker](https://www.docker.com/)
2. [Git](https://git-scm.com/)


## Building Data Warehouse - Main Propose

- Medalion Architechture

![ETL](/Docs/data_warehouse.png)


## Analytics

### Data Models

![ETL](/Docs/Data_Model_dwh.png)

### Data Explore and Datalytic

![ETL](/Docs/Screenshot%20from%202025-05-15%2017-43-08.png)

## Future Update

- Hoàn thành các Dashboard trên Metabase, hiện tại chỉ mới Exploration Data Analysis.
- Sử dụng **Apache Spark** cho việc transform xây dựng các layer bronze, silver and gold với bộ dữ liệu lớn hơn.
- Tạo mô hình gợi ý đề xuất các sản phẩm sử dụng Machine Learning, xây dựng giao diện trực quang bằng streamlit cho việc sử dụng mô hình machine learning.
- Dữ liệu Trong Mysql là giả lập dữ liệu từ một cửa hàng doanh nghiệp, xem xét tìm hiểu sử dụng các API lấy dữ liệu thay vì giả lập, kết hợp streaming bằng các công cụ như **Apache Kafka, ...**

## Conclusion
Project hướng dẫn xây dựng mô hình ETL cơ bản đơn giản.