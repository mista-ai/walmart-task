import pandas as pd
import sqlite3

# File paths
spreadsheet_0_path = 'data/shipping_data_0.csv'
spreadsheet_1_path = 'data/shipping_data_1.csv'
spreadsheet_2_path = 'data/shipping_data_2.csv'
database_path = 'shipment_database.db'

# Load the spreadsheets
df0 = pd.read_csv(spreadsheet_0_path)
df0.rename(columns={'product': 'name'}, inplace=True)
df1 = pd.read_csv(spreadsheet_1_path)
df2 = pd.read_csv(spreadsheet_2_path)
df0_db = df0.drop_duplicates(subset='name').copy()

columns_to_keep = ['name']
columns_to_drop = [col for col in df0_db.columns if col not in columns_to_keep]
df0_db.drop(columns=columns_to_drop, inplace=True)
df0_db['id'] = df0_db.index

# Connect to the SQLite database
conn = sqlite3.connect(database_path)

# Create a temporary table and insert the data
df0_db.to_sql('temp_product', conn, if_exists='replace', index=False)

# Use an INSERT OR IGNORE to transfer data from the temporary table to the target table
conn.execute('''
    INSERT OR IGNORE INTO product (id, name)
    SELECT id, name FROM temp_product;
''')

# Drop the temporary table
conn.execute('DROP TABLE IF EXISTS temp_product;')

conn.commit()

df0_db = pd.read_sql('SELECT * FROM product', conn)
df0_db.rename(columns={'id': 'product_id'}, inplace=True)

df1.rename(columns={'product': 'name'}, inplace=True)
merged_df1_df2 = pd.merge(df1, df2, on='shipment_identifier')

final_merged_df = pd.merge(merged_df1_df2, df0,
                           on=['name', 'origin_warehouse', 'destination_store', 'driver_identifier', 'on_time'])
final_merged_df.rename(
    columns={'origin_warehouse': 'origin', 'destination_store': 'destination', 'product_quantity': 'quantity'},
    inplace=True)
final_merged_df = pd.merge(final_merged_df, df0_db)
columns_to_keep = ['product_id', 'quantity', 'origin', 'destination']
columns_to_drop = [col for col in final_merged_df.columns if col not in columns_to_keep]

# Create a temporary table and insert the data
final_merged_df.to_sql('temp_shipment', conn, if_exists='replace', index=False)

# Use an INSERT OR IGNORE to transfer data from the temporary table to the target table
conn.execute('''
    INSERT OR IGNORE INTO shipment (product_id, quantity, origin, destination)
    SELECT product_id, quantity, origin, destination FROM temp_shipment;
''')

# Drop the temporary table
conn.execute('DROP TABLE IF EXISTS temp_shipment;')

conn.commit()

# Close the database connection
conn.close()
