import config as conf
import snowflake.connector as sno_con

# Connecting to snowflake using the credentials mentioned in the config file
conn = sno_con.connect(**conf.snowflake_config)
cur = conn.cursor()

# Let's consider the below is the source data
source_data = [
    {'id': 1, 'name': 'John', 'address': 'Chatrabagi', 'start_date': '2023-08-28'},
    {'id': 2, 'name': 'Bob', 'address': 'Kujarbagi', 'start_date': '2022-08-28'}
]

# Loading these records into Snowflake table
# for src_rec in source_data:
#    placeholders = ', '.join(['%s'] * len(src_rec))
#    insert_query = f"INSERT INTO ELT_DB.ELT_SCHEMA.EMPLOYEE(ID, NAME, ADDRESS, START_DATE) VALUES ({placeholders})"
#    value = tuple(src_rec.values())
#    cur.execute(insert_query,value)
# Checking for Existing record
business_key = 'id'
for record in source_data:
    result = cur.execute(f"SELECT * FROM ELT_DB.ELT_SCHEMA.EMPLOYEE WHERE {business_key} = %s", (record[business_key],))
    existing_record = result.fetchone()
    # print(existing_record)

    # If Business_key is present in the table
    if existing_record:
        cur.execute(f"UPDATE ELT_DB.ELT_SCHEMA.EMPLOYEE SET END_DATE = CURRENT_DATE WHERE {business_key} = %s AND END_DATE IS NULL",(record[business_key],))
        cur.execute(
            f"INSERT INTO ELT_DB.ELT_SCHEMA.EMPLOYEE ({business_key}, name, address, start_date, end_date) VALUES (%s, %s, %s, %s, %s)",
            (record[business_key], record['name'], record['address'], record['start_date'], None)
        )
    else:
        # Insert new record
        cur.execute(
            f"INSERT INTO ELT_DB.ELT_SCHEMA.EMPLOYEE ({business_key}, name, address, start_date, end_date) VALUES (%s, %s, %s, %s, %s)",
            (record[business_key], record['name'], record['address'], record['start_date'], None)
        )
