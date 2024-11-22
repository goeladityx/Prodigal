# Mutual Funds Data Pipeline and Querying System

This project implements a robust data pipeline for scraping mutual fund data, storing it in a MySQL database, and providing various querying options for analysis. It is designed to handle exceptions, ensure data consistency, and optimize performance for data ingestion and querying.

---

## **Features**

1. **Data Scraping**:
   - Scrapes mutual fund data from an external source for a specified date range.
   - Supports both historical and regular (incremental) data ingestion into a MySQL database.

2. **Database Schema**:
   - Creates a `mutual_funds` table with the following structure:
     - `scheme_code` (VARCHAR): Unique identifier for the mutual fund.
     - `scheme_name` (VARCHAR): Name of the mutual fund.
     - `isin_div_payout_growth` (VARCHAR): ISIN for dividend payout/growth.
     - `isin_div_reinvestment` (VARCHAR): ISIN for dividend reinvestment.
     - `net_asset_value` (DECIMAL): Net asset value of the fund.
     - `repurchase_price` (DECIMAL): Repurchase price of the fund.
     - `sale_price` (DECIMAL): Sale price of the fund.
     - `nav_date` (DATE): Date of the NAV record.
   - Index on `scheme_code` and `nav_date` for efficient querying.

3. **Exception Handling**:
   - Skips rows with invalid or duplicate data.
   - Logs skipped rows for debugging.
   - Gracefully handles database-related errors.

4. **Query Options**:
   - Retrieve NAV trends for a specific mutual fund.
   - Compare NAVs across multiple mutual funds within a date range.
   - Run custom SQL queries.

5. **Performance Optimizations**:
   - Batch insertions reduce database overhead.
   - Indexing for faster lookups.
   - Skips duplicate rows by checking the database before insertion.

---

## **Setup Instructions**

### **1. Prerequisites**
- Python 3.x
- MySQL server
- Required Python packages (install using `requirements.txt`):
  ```bash
  pip install -r requirements.txt

### **2. Add Your Database Credentials**
Before running the script, update the `establish_connection` function with your database credentials:

```python
connection = pymysql.connect(
    charset="utf8mb4",
    connect_timeout=timeout,
    cursorclass=pymysql.cursors.DictCursor,
    db="your_database",
    host="your_host",
    password="your_password",
    port=your_port,
    user="your_user",
    write_timeout=timeout,
)
```
### **2. After this run the main file**
```python main.py
