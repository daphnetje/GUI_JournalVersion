import pandas as pd
import sqlite3


# Load each sheet into a DataFrame (modify sheet names if necessary)
df_discriminatory_patterns = pd.read_excel("data\\recidivism_discriminatory_patterns.xlsx")
df_validation = pd.read_excel("data\\recidivism_val_data.xlsx")
df_test = pd.read_excel("data\\recidivism_test_data.xlsx")


conn = sqlite3.connect("bias_detection.db")
cursor = conn.cursor()


cursor.execute("DROP TABLE IF EXISTS recidivism_discriminatory_patterns;")
cursor.execute("DROP TABLE IF EXISTS recidivism_validation_data;")
cursor.execute("DROP TABLE IF EXISTS recidivism_test_data;")
conn.commit()

df_discriminatory_patterns.to_sql("recidivism_discriminatory_patterns", conn, if_exists="append", index=False)
df_validation.to_sql("recidivism_validation_data", conn, if_exists="append", index=False)
df_test.to_sql("recidivism_test_data", conn, if_exists="append", index=True, index_label="id")

conn.commit()
conn.close()



# # Create new tables
# cursor.execute("""
# CREATE TABLE adult_discriminatory_patterns (
#     id string primary key,
#     pd_itemset TEXT,
#     rule_base TEXT,
#     rule_conclusion TEXT,
#     support REAL,
#     confidence REAL,
#     slift REAL,
#     p_value_slift REAL
# );
# """)
#
# cursor.execute("""
# CREATE TABLE adult_validation_data (
#     id integer primary key,
#     age TEXT,
#     marital_status TEXT,
#     education TEXT,
#     workinghours TEXT,
#     workclass TEXT,
#     occupation TEXT,
#     race TEXT,
#     sex TEXT,
#     income TEXT,
#     pred_probability REAL
# );
# """)
#
# cursor.execute("""
# CREATE TABLE adult_test_data (
#     id integer primary key autoincrement,
#     age TEXT,
#     marital_status TEXT,
#     education TEXT,
#     workinghours TEXT,
#     workclass TEXT,
#     occupation TEXT,
#     race TEXT,
#     sex TEXT,
#     income TEXT,
#     pred_probability REAL,
#     uncertainty_score REAL,
#     relevant_rule_id TEXT,
#     max_slift REAL,
#     sit_test_score REAL,
#     closest_favoured TEXT,
#     closest_discriminated TEXT,
#     selector TEXT,
#     GLU_score REAL
#
# );
# """)
#
# conn.commit()
