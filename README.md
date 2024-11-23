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
### **3. After this run the main file**
```python
python main.py
```

---

## **What Happens in the Main Function?**

1. **Table Clearance**
   - Deletes all existing data in the `mutual_funds` table for a fresh start.

2. **Ingest Historical Data**
   - Adds historical data for a predefined date range (e.g., `01-Oct-2024` to `03-Oct-2024`).

3. **Ingest Regular Data**
   - Automates incremental data ingestion for the last two days, ensuring the database remains up-to-date.

4. **Interactive Query Options**
   - Allows users to test and retrieve data using pre-defined and custom queries.

5. **Database Connection Closure**
   - Ensures the database connection is closed properly after execution.

---

## **Exception Handling**

1. **Invalid Data**
   - Rows with invalid `net_asset_value`, `repurchase_price`, or `sale_price` are skipped during insertion.
   - Logs skipped rows for debugging.

2. **Duplicate Rows**
   - Checks for existing rows with the same `scheme_code` and `nav_date` before insertion.
   - Skips duplicate rows to maintain data integrity.

3. **Database Errors**
   - Handles `pymysql` exceptions gracefully and ensures partial insertions are rolled back.
  
---

## **Performance Measures**

1. **Batch Insertions**
   - Ensures rows are inserted in batches to minimize database overhead.

2. **Indexing**
   - Indexes on `scheme_code` and `nav_date` improve query performance.

3. **Incremental Updates**
   - Regular ingestion adds only the latest data, reducing the need to reprocess the entire dataset.

---

## **Query Options**

The script provides the following query options for testing:

### **1. Retrieve NAV Trends**
- Retrieves the NAV history for a specific mutual fund.
- **Example Query**:
    ```sql
    SELECT nav_date, net_asset_value 
    FROM mutual_funds 
    WHERE scheme_code = '139617';
    ```

### **2. Compare NAVs Across Funds**
- Compares NAVs for multiple funds within a specific date range.
- **Example Query**:
    ```sql
    SELECT scheme_code, scheme_name, nav_date, net_asset_value 
    FROM mutual_funds 
    WHERE nav_date BETWEEN '2024-11-01' AND '2024-11-07';
    ```

### **3. Run Custom SQL Queries**
- Allows users to execute any SQL query directly.
- **Example Query**:
    ```sql
    SELECT COUNT(*) FROM mutual_funds;
    ```

---

## **Visual Summary**

### **Key Features**

| Feature               | Description                                           |
|-----------------------|-------------------------------------------------------|
| **Data Scraping**     | Fetches data for a specified date range.              |
| **Historical & Incremental** | Handles both historical and regular updates.         |
| **Interactive Querying** | Provides predefined and custom query options.          |

---

### **Performance Measures**

| Measure               | Description                                           |
|-----------------------|-------------------------------------------------------|
| **Batch Insertions**  | Minimizes database overhead.                          |
| **Indexing**          | Improves lookup and query performance.                |
| **Duplicate Handling**| Skips rows with the same `scheme_code` and `nav_date`.|

---

## Sample Queries Output Format

Query Options:
1. Retrieve NAV trends for a single mutual fund
2. Compare NAVs across multiple mutual funds within a date range
3. Run a custom SQL query
4. Exit
Enter your choice: 1
Enter the Scheme Code: 139619
NAV Trends:
{'nav_date': datetime.date(2024, 10, 1), 'net_asset_value': Decimal('10.0000')}
{'nav_date': datetime.date(2024, 10, 3), 'net_asset_value': Decimal('10.0000')}
{'nav_date': datetime.date(2024, 11, 21), 'net_asset_value': Decimal('10.0000')}

Query Options:
1. Retrieve NAV trends for a single mutual fund
2. Compare NAVs across multiple mutual funds within a date range
3. Run a custom SQL query
4. Exit
Enter your choice: 3
Enter your custom SQL query: SELECT COUNT(*) AS total_entries FROM mutual_funds;
Query Results:
{'total_entries': 24786}

Query Options:
1. Retrieve NAV trends for a single mutual fund
2. Compare NAVs across multiple mutual funds within a date range
3. Run a custom SQL query
4. Exit
Enter your choice: 4
Exiting query options.
Connection closed. Pipeline execution complete.
