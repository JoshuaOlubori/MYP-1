# %%
import pandas as pd
import duckdb

# %%
conn = duckdb.connect()

discharges_create_table_query = """
CREATE TABLE hospital_discharges AS SELECT * FROM read_csv('datasets/hospital_discharges.csv');
"""

facility_create_table_query = """
CREATE TABLE facility AS SELECT * FROM read_csv('datasets/facility.csv');
"""

ccs_diagnosis_create_table_query = """
CREATE TABLE ccs_diagnosis AS SELECT * FROM read_csv('datasets/ccs_diagnosis.csv');
"""

ccs_procedure_create_table_query = """
CREATE TABLE ccs_procedure AS SELECT * FROM read_csv('datasets/ccs_procedure.csv');
"""
queries = [discharges_create_table_query, facility_create_table_query, 
           ccs_diagnosis_create_table_query,ccs_procedure_create_table_query]
#%%
try:
    for query in queries:
        conn.execute(query)
    print("tables created succesfully")
except Exception as e:
    print(e)

# %%
conn.execute("""
CREATE OR REPLACE TABLE results AS 
select * 
from hospital_discharges as hd
inner join
ccs_diagnosis as cdc 
on hd.ccs_diagnosis_code = cdc.ccs_diagnosis_code
""")

#%%
# pandas transformation code here on full joined table
# %%
# writing to local drive
conn.execute("COPY (FROM results) TO 'results.csv' (FORMAT 'csv')")
# %%
