# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 15:25:11 2024

@author: adity
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pymysql

headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246", 
           "Authorization": "Token 6ec9e524b28e1a37a7384d809feb20414d87652f"}

def establish_connection():
    
    print("Establishing database connection...")
    timeout = 30
    connection = pymysql.connect(
        charset="utf8mb4",
        connect_timeout=timeout,
        cursorclass=pymysql.cursors.DictCursor,
        db="defaultdb",
        host="mysql-1642b338-adityamayor-2b2c.b.aivencloud.com",
        password="AVNS_5mADJNEK32hcZyFcK12",
        read_timeout=timeout,
        port=17851,
        user="avnadmin",
        write_timeout=timeout,
    )
    print("Database connection established.")
    return connection

def create_schema(connection):
    print("Creating database schema...")
    cursor = connection.cursor()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS mutual_funds (
        id INT AUTO_INCREMENT PRIMARY KEY,
        scheme_code VARCHAR(50) NOT NULL,
        scheme_name VARCHAR(255) NOT NULL,
        isin_div_payout_growth VARCHAR(20),
        isin_div_reinvestment VARCHAR(20),
        net_asset_value DECIMAL(10, 4),
        repurchase_price DECIMAL(10, 4),
        sale_price DECIMAL(10, 4),
        nav_date DATE,
        INDEX idx_scheme_code_date (scheme_code, nav_date)
    ) ENGINE=InnoDB;
    """
    cursor.execute(create_table_query)
    print("Schema created successfully!")

# =============================================================================
# def retry_failed_rows(failed_rows, connection):
# 
#     cursor = connection.cursor()
#     insert_query = """
#         INSERT INTO mutual_funds (
#             scheme_code, scheme_name, isin_div_payout_growth, 
#             isin_div_reinvestment, net_asset_value, 
#             repurchase_price, sale_price, nav_date
#         )
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
#     """
#     
#     for row in failed_rows:
#         try:
#             row[4] = float(row[4]) if row[4] not in ('', None) else None  # net_asset_value
#             row[5] = float(row[5]) if row[5] not in ('', None) else None  # repurchase_price
#             row[6] = float(row[6]) if row[6] not in ('', None) else None  # sale_price
#             
#             cursor.execute(insert_query, row)
#         
#         except Exception as e:
#             continue
#     
#     connection.commit()
# =============================================================================

def add_rows(data_list, connection):
    
    cursor = connection.cursor()
    
    # Exception handling
    for i in range(0,2):
        if data_list[i] in ('', None) :
            return
    
    for i in range(2,4):
        if data_list[i] in ('', None) :
            data_list[i] = 'NA'
    
    for i in range(4,7):
        if data_list[i] in ('', None) :
            data_list[i] = 0.0
    
    data_list[7] = datetime.strptime(data_list[7], "%d-%b-%Y").strftime("%Y-%m-%d")
    
    insert_query = """
        INSERT INTO mutual_funds (
            scheme_code, scheme_name, isin_div_payout_growth, 
            isin_div_reinvestment, net_asset_value, 
            repurchase_price, sale_price, nav_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    
    try:
        cursor.execute(insert_query, data_list)
        connection.commit()
        
    except Exception as e:
        print(e)
    
def delete_row(condition, connection):
    cursor = connection.cursor()
    delete_query = f"DELETE FROM mutual_funds WHERE {condition};"
    cursor.execute(delete_query)
    connection.commit()
    
def clear_table(connection):
    print("Clearing the table...")
    cursor = connection.cursor()
    clear_query = "TRUNCATE TABLE mutual_funds;"
    cursor.execute(clear_query)
    connection.commit()
    print("Table cleared.")
    
def find_and_delete_duplicates(connection):
    print("Checking for duplicate rows...")
    cursor = connection.cursor()
    
    delete_duplicates_query = """
        DELETE mf1
        FROM mutual_funds mf1
        INNER JOIN mutual_funds mf2
        ON mf1.scheme_code = mf2.scheme_code 
           AND mf1.scheme_name = mf2.scheme_name
           AND mf1.id > mf2.id;
    """
    cursor.execute(delete_duplicates_query)
    connection.commit()
    print("Duplicate rows deleted.")
    
# =============================================================================
# As I don't have that much infromation regarding Mutual Funds. Here are some 
# testing Queries as mentioned in the doc
# =============================================================================

# Query 1: Retrieve NAV trends for a single mutual fund
def get_nav_trends(scheme_code):
    connection = establish_connection()
    cursor = connection.cursor()
    query = """
        SELECT nav_date, net_asset_value
        FROM mutual_funds
        WHERE scheme_code = %s
        ORDER BY nav_date;
    """
    cursor.execute(query, (scheme_code,))
    return cursor.fetchall()

# Query 2: Compare NAVs across multiple mutual funds within a date range
def compare_navs(start_date, end_date):
    connection = establish_connection()
    cursor = connection.cursor()
    query = """
        SELECT scheme_code, scheme_name, nav_date, net_asset_value
        FROM mutual_funds
        WHERE nav_date BETWEEN %s AND %s
        ORDER BY nav_date, scheme_code;
    """
    cursor.execute(query, (start_date, end_date))
    return cursor.fetchall()

# Query 3: Any random query
def random_query(query):
    connection = establish_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def data_sraper(start_date, end_date):
    
# =============================================================================
#     Not sure which data to be ingested for a full load of historical data so 
#     for the time being we will ingest data from 01-Nov-2024 to 10-Nov-2024 here:
#     as historical data
# =============================================================================
    print(f"Starting data scraper for range: {start_date} to {end_date}")
    connection = establish_connection()

    start_date_obj = datetime.strptime(start_date, "%d-%b-%Y")
    end_date_obj = datetime.strptime(end_date, "%d-%b-%Y")
    current_start = start_date_obj
    while current_start < end_date_obj:
        # final_list = []
        current_end = current_start + timedelta(days=2)
        
        if current_end > end_date_obj:
            current_end = end_date_obj
            break
        
        
        URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=" + current_start.strftime("%d-%b-%Y") + "&todt=" + current_end.strftime("%d-%b-%Y")
        r = requests.post(url=URL, headers=headers)
        soup = BeautifulSoup(r.content, 'html5lib')
        data = soup.find('body').text.split('\n')
        
        switch = 0
        for entry in data:
            items_list = entry.split(';')
            
# =============================================================================
#             For the time being I am adding all the entries but here we should 
#             have checks to stop the duplicate entries from being added or 
#             checks to stop the schema definition from entring the main schema
# =============================================================================

            if len(items_list) > 1 and switch == 1:
                add_rows(items_list, connection)
                # final_list.append(items_list)
                
            elif len(items_list) > 1 and switch == 0:
                switch = 1
        
        # add_rows(final_list, connection)
        current_start = current_end
        
def main():
    connection = establish_connection()

# =============================================================================
#     Here I would like to clear the table each and every time so that you can
#     evaluate properly
# =============================================================================
    clear_table(connection)

    historical_start_date = "01-Oct-2024"
    historical_end_date = "03-Oct-2024"
    data_sraper(historical_start_date, historical_end_date)
    find_and_delete_duplicates(connection)
    print("Historical data ingestion complete.")

    # Pipeline to add data daily
    def ingest_regular_data():
        today = datetime.today()
        regular_start_date = (today - timedelta(days=1)).strftime("%d-%b-%Y")
        regular_end_date = today.strftime("%d-%b-%Y")
        data_sraper(regular_start_date, regular_end_date)
        find_and_delete_duplicates(connection)
        print("Regular data ingestion complete.")

    # Step 5: Query Options
    def query_options():
        while True:
            print("\nQuery Options:")
            print("1. Retrieve NAV trends for a single mutual fund")
            print("2. Compare NAVs across multiple mutual funds within a date range")
            print("3. Run a custom SQL query")
            print("4. Exit")
            
            choice = input("Enter your choice: ")
            
            if choice == "1":
                scheme_code = input("Enter the Scheme Code: ")
                results = get_nav_trends(scheme_code)
                print("NAV Trends:")
                for row in results:
                    print(row)
            elif choice == "2":
                start_date = input("Enter the start date (DD-MMM-YYYY): ")
                end_date = input("Enter the end date (DD-MMM-YYYY): ")
                results = compare_navs(datetime.strptime
                                       (start_date, "%d-%b-%Y").
                                       strftime("%Y-%m-%d"), 
                                       datetime.strptime
                                       (end_date, "%d-%b-%Y").
                                       strftime("%Y-%m-%d"))
                print("NAV Comparison:")
                for row in results:
                    print(row)
            elif choice == "3":
                query = input("Enter your custom SQL query: ")
                results = random_query(query)
                print("Query Results:")
                for row in results:
                    print(row)
            elif choice == "4":
                print("Exiting query options.")
                break
            else:
                print("Invalid choice. Please try again.")

    # Step 6: Execute the regular ingestion and query options
    print("\nExecuting regular ingestion pipeline and query options...")
    ingest_regular_data()
    query_options()

    # Close the connection
    connection.close()
    print("Connection closed. Pipeline execution complete.")

# Run the main function
if __name__ == "__main__":
# =============================================================================
#     Before running it again, you can directly query the DB for different 
#     kind of data already been added
# =============================================================================
    main()