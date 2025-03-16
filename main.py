import anthropic
import pandas as pd
import os
from dotenv import load_env
import sqlite3

load_env()

#get the api key
api_key = os.getenv('API_KEY')

#initializing the claude client
client = anthropic.Anthropic(api_key=api_key)

#create the tables from the given dataframe dicts
def create_sqlite_db(dataframes_dict, db_path="data.db"):
    conn = sqlite3.connect(db_path)
    for table_name, df in dataframes_dict.items():
        df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

#we need to get the schema of the table for dynamic fuctionality of the RAG

def get_table_schema(table_name, db_path="data.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    schema = f"CREATE TABLE {table_name} (\n"
    for col in columns:
        schema += f"    {col[1]} {col[2]},\n"
    schema = schema.rstrip(",\n") + "\n);"
    conn.close()
    return schema

#now we need to do the process query
def process_query_sql(user_query, dataframes_dict, db_path='data.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    schemas = "\n".join([get_table_schema(table_name) for table_name in dataframes_dict])

    system_prompt = f"""
    You are an assistant that translates natural langauge questions into SQL queries.
    Use the following table scemas to generate the SQL Queries :
    {schemas}
    Return only the SQL query, do not include any extra text.
"""
    sql_query_generation = client.messages.create(
        model="claude-3-7-sonnet-20250219",
        max_tokens=1000,
        system=system_prompt,
        messages=[{'role':'user','content':user_query}]

    )
    sql_query = sql_query_generation.content[0].text.strip()
    print(f"Generated SQL Query: {sql_query}")

    try:
        cursor.execute(sql_query)
        results = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        df_results = pd.DataFrame(results, columns=columns)
        print(f"SQL Query Results: \n{df_results}")
    except sqlite3.Error as e:
        return f"Error executing SQL query: {e}"
    results_text = df_results.to_string()

    conn.close()

    response = client.messages.create(
        model="claude-3-haiku-20240307",
        max_tokens=1000,
        system="You are a helpful assistant that answers questions based on the provided SQL query results.",
        messages=[{'role':'user', "content": f"Query Results:\n{results_text}\n\nQuestion: {user_query}"}]
    )

    return response.content[0].text

if __name__ == "__main__":
    tables = {
        'table1': pd.read_csv('healthcare_10.csv'),
        "table2": pd.read_csv('healthcare_10-20.csv')
    }
    #this RAG can handle both the horizontal and vertical divided data.

    #It's can understand the schema of the table and can generate the SQL query based on the schema of the table.
    create_sqlite_db(tables) 

    result = process_query_sql("How many people had cancer?", tables)
    print(result)
