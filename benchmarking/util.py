import psycopg2
import json
from sqlalchemy import create_engine, text as sql_text

import pandas as pd
import numpy as np
import matplotlib as mpl

def hello():
	print("Hello world")

def build_query_listings_join_reviews(start_date, end_date):
	q1 = """
SELECT DISTINCT l.id, l.name
FROM listings l, reviews r
WHERE l.id = r. listing_id AND r.date >= '"""
	q2 = """' and r.date <= """
	q3 = """'
ORDER BY l.id;"""
	return q1 + start_date + q2 + end_date + q3

def build_query_listings_join_reviews_datetime(start_date, end_date):
	q1 = """
SELECT DISTINCT l.id, l.name
FROM listings l, reviews r
WHERE l.id = r. listing_id AND r.datetime >= '"""
	q2 = """' and r.datetime <= """
	q3 = """'
ORDER BY l.id;"""
	return q1 + start_date + q2 + end_date + q3


def time_diff(time1, time2):
    return (time2-time1).total_seconds()


def add_drop_index(db_eng, command, col, table):
	# e.g. "drop the id index on the listings table"
	index_name = col + "_in_" + table
	print("\nIndex name:" + index_name)
	q = """"""
	if(command == 'add'):
		q1 = """BEGIN TRANSACTION;\nCREATE INDEX IF NOT EXISTS """
		q2 = """\nON """
		q3 = """;\n END TRANSACTION;\n"""
		q = q1 + index_name + q2 + table + "(" + col + ")" +q3

	elif(command == 'drop'):
		q1 = """BEGIN TRANSACTION;\nDROP INDEX IF EXISTS """
		q2 = """;\n END TRANSACTION;\n"""
		q = q1 + index_name + q2

	else:
		print("\nUnknown command: " + command)
		return

	print("QUERY TO EXECUTE:\n" + q)

	with db_eng.connect() as conn:
		conn.execute(sql_text(q))

		# now get all of the indexes and return it
		q_show_all_indexes = """select * from pg_indexes\n where tablename = '""" + table + """';"""

		result = conn.execute(sql_text(q_show_all_indexes))

		return result.all()

# fetches filename (which should be a json file) and returns a 
#       dict corresponding to the contents of filename
def fetch_perf_data(filename):
    f = open('perf_data/' + filename)
    return json.load(f)

# writes the dictionary in dict as a json file into filename
def write_perf_data(dict, filename):
    with open('perf_data/' + filename, 'w') as fp:
        json.dump(dict, fp)

def build_index_description_key(all_index, spec):
    description_key = ""
    for index in all_index:
        if index in spec:
            description_key += "__"
            description_key += str(index[0]) + "_in_" + str(index[1])
    
    description_key += "__"
    return description_key


def newfunc():
	print("test")