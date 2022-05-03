import sql_queries
import extract

# Extract data from API and send to csv (stored locally)
# pass latitude and longitude of choice, this should have two decimal places.
extract.get_gust_data('50.57', '-2.45')
extract.get_direction_data('50.57', '-2.45')
extract.create_data_set()
extract.tidy_up()

# Save data to MySql Database
# Database is created locally in MySQL Workbench
# Table is created to refresh for current data
sql_queries.insert_records()
