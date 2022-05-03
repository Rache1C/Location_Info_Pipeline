import sql_queries
import tmdb

#Extract data from API and send to csv (stored locally)
tmdb.get_gust_data()
tmdb.get_direction_data()
tmdb.create_data_set()

#save data to MySql Database
#Database is created locally in MySQL Workbench
#Table is created if not already exists
sql_queries.insert_records()
