import pandas as pd
import sqlite3


# Load each sheet into a DataFrame (modify sheet names if necessary)
# df_discriminatory_patterns = pd.read_excel("rules.xlsx")
# df_discriminatory_patterns = df_discriminatory_patterns[df_discriminatory_patterns['support'] >= 0.01]
df_discriminatory_patterns = pd.read_excel("NEWreject_rules.xlsx")
#df_discriminatory_patterns = df_discriminatory_patterns[df_discriminatory_patterns['slift'] >= 0.50]
df_validation = pd.read_excel("NEWval_data_with_pred.xlsx")
df_test = pd.read_excel("NEWtest_data_with_preds.xlsx")


conn = sqlite3.connect("bias_detection.db")
cursor = conn.cursor()


cursor.execute("DROP TABLE IF EXISTS adult_dataset;")
cursor.execute("DROP TABLE IF EXISTS discriminatory_patterns;")
cursor.execute("DROP TABLE IF EXISTS validation_data;")
cursor.execute("DROP TABLE IF EXISTS test_data;")
conn.commit()

# Create new tables
cursor.execute("""
CREATE TABLE discriminatory_patterns (
    id integer primary key autoincrement,
    pd_itemset TEXT,
    rule_base TEXT,
    rule_conclusion TEXT,
    support REAL,
    confidence REAL,
    slift REAL,
    p_value_slift REAL
);
""")

cursor.execute("""
CREATE TABLE validation_data (
    id integer primary key autoincrement,
    age TEXT,
    marital_status TEXT,
    education TEXT,
    workinghours TEXT,
    workclass TEXT,
    occupation TEXT,
    race TEXT,
    sex TEXT,
    income TEXT,
    pred_probability REAL
);
""")

cursor.execute("""
CREATE TABLE test_data (
    id integer primary key autoincrement,
    age TEXT,
    marital_status TEXT,
    education TEXT,
    workinghours TEXT,
    workclass TEXT,
    occupation TEXT,
    race TEXT,
    sex TEXT,
    income TEXT,
    pred_probability REAL
);
""")

conn.commit()


df_discriminatory_patterns.to_sql("discriminatory_patterns", conn, if_exists="append", index=False)
df_validation.to_sql("validation_data", conn, if_exists="append", index=False)
df_test.to_sql("test_data", conn, if_exists="append", index=False)

conn.commit()
conn.close()