import os
import psycopg2
import pandas as pd
# import snowflake.connector
from sqlalchemy import create_engine
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Get Pinecone API key from environment
PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
# if not PINECONE_API_KEY:
#     raise ValueError("PINECONE_API_KEY not found in environment variables")

POSTGRES_ENGINE = os.getenv('POSTGRES_ENGINE')
# if not POSTGRES_ENGINE:
#     raise ValueError("POSTGRES_ENGINE not found in environment variables")

TWITTER_BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN_NDS')
# if not TWITTER_BEARER_TOKEN:
#     raise ValueError("TWITTER_BEARER_TOKEN not found in environment variables")

TWITTER_KOL_COLS = ['id', 'name', 'username', 'description', 'followers_count', 'associated_project_id', 'account_type', 'tracking']
PROJECT_COLS = ['name', 'parent_project_id', 'description', 'ecosystem', 'tags']

def execute_pg_query(query):
	print('execute_pg_query')
	print(query)
	conn = psycopg2.connect(POSTGRES_ENGINE)
	cursor = conn.cursor()
	cursor.execute(query)
	conn.commit()
	conn.close()

# def load_data_from_snowflake(query):
# 	ctx = snowflake.connector.connect(
# 		user=usr,
# 		password=pwd,
# 		account='vna27887.us-east-1'
# 	)

# 	df = ctx.cursor().execute(query)
# 	df = pd.DataFrame.from_records(iter(df), columns=[x[0] for x in df.description])
# 	df.columns = [x.lower() for x in df.columns]
# 	return df

def load_data_from_pg(query):
	print('load_data_from_pg')
	print(query)
	conn = psycopg2.connect(POSTGRES_ENGINE)
	cursor = conn.cursor()
	cursor.execute(query)
	df = pd.DataFrame(cursor.fetchall(), columns=[x[0] for x in cursor.description])
	df.columns = [x.lower() for x in df.columns]
	return df

def upload_data_to_pg(df, table, if_exists="append"):
	engine = create_engine(POSTGRES_ENGINE)
	df.to_sql(table, engine, if_exists=if_exists, index=False)
