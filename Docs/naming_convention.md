# **Naming Conventions**

---

This document outlines the naming convention used for schema, tables, views, columns and other object in the data warehouse.

## **Table of Contents**

---

## **General Principles**

---

- Naming conventions: Use snake_case, with lowercase letters and underscore( _ ) to separate words.
- Language: Use English for all name.
- Avoid Reserved Words: Do not use SQL reserved words as object names.

## **Table Naming Conventions**

---

### **Bronze Rules**

- All names must start with source system name, and table names must match their original names without renaming.
- **`<sourcesystem>_<entity>`:**
    - `<sourcesystem>`: Name of source system (e.g, `crm`, `emp`,….)
    - `<entity>`: Extract table name from source system
    - Example: `crm_customer_info` → Customer information from the CRM system.

### **Silver Rules**

- All names must start with source system name, and table names must match their original names without renaming.
- **`<sourcesystem>_<entity>`:**
    - `<sourcesystem>`: Name of source system (e.g, `crm`, `emp`,….)
    - `<entity>`: Extract table name from source system
    - Example: `crm_customer_info` → Customer information from the CRM system.

### **Gold Rules**

- All name must use meaning full, business aligned name for tables, starting with the category prefix.
- **`<category>_<entity>:`**
    - `<category>`: Describes the role of table, such as `dim`  (dimension) or `fact` (fact table)
    - `<entity>` : Descriptive name of table, aligned with the business domain (e.g., `customers, products, sales,` ….)
    - Examples:
        - dim_customers: Dimension table for customer data.
        - fact_sales: Fact table containing sales transactions.

### **Glossary of Category Patterns**

| **Pattern** | **Meaning** | **Example(s)** |
| --- | --- | --- |
| `dim_` | Dimension table | `dim_customer`, `dim_product` |
| `fact_` | Fact table | `fact_sales` |
| `report_` | Report table | `report_customers`, `report_sales_monthly` |

## **Column Naming Conventions**

---

### **Surrogate Keys**

- All primary keys in dimension tables must use the suffix `_key`.
- **`<table_name>_<key>` :**
    - `<table_name>`: Refers to the name of the tables or entity the keys belong to.
    - `_key`: A suffix indicating that this column is a surrogate key
    - Example: `customer_key` → surrogate key in the `dim_customer` table

### **Technical Columns**

- A technical columns must start with the prefix `dwh_` , follow by description name indicating the column’s purpose.
- **`dwh_<column_name>`**
    - `dwh` : Prefix exclusively for system-generated metadata.
    - `<column_name>` : Descriptive name indicating the columns’s purpose.
    - Example: `dwh_load_date` → system generating column used to store the date when the record was loaded.

## **Stored Procedure**

---

- All stored procedures use for loading data must follow the naming pattern:
- **`load_<layer>`**
    - `<layer>`: Represents the layer being loaded, such as `bronze`, `silver`, or `gold`.
    - Example:
        - `load_bronze` → Stored procedure for loading data into the Bronze layer.
        - `load_silver` → Stored procedure for loading data into the Silver layer.