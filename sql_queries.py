import mysql.connector
import csv
import config
from config import USER, PASSWORD, HOST

#db connection test
class DbConnectionError(Exception):
    pass

#db connection (add your own credentials to the config file and create database called 'Location_Info'
def _connect_to_db(db_name):
   cnx = mysql.connector.connect(
        host=config.HOST,
        user=config.USER,
        password=config.PASSWORD,
        port=3306,
        database='Location_Info', autocommit=True
    )
   return cnx

# Insert data into MySql Database
def insert_records():
    db_connection = []
    #check connection
    try:
        db_name = 'Location_info'  # update as required
        db_connection = _connect_to_db(db_name)
        cur = db_connection.cursor()
        print("Connected to DB: %s" % db_name)
        #drop existing table
        cur.execute('''DROP TABLE IF EXISTS `Location_info`.`wind_predict`;''')
        print('Creating table....')
        #create table for data dumping
        cur.execute('''
            CREATE TABLE `Location_info`.`wind_predict` (
                `id` INT NOT NULL,
                `Date` VARCHAR(45) NULL,
                `Wind_Gusts_Kts` VARCHAR(45) NULL,
                `Wind_Dir_Deg_T` VARCHAR(45) NULL,
                PRIMARY KEY (`id`));
            ''')
        print('Table created successfuly!')
        #read csv file into MySQL table
        csv_data = csv.reader(open('wind_predict.csv'))
        next(csv_data)
        for row in csv_data:
            cur.execute("INSERT INTO Location_info.wind_predict VALUES(%s, %s, %s, %s)", row)
        cur.close()
        print("Data passed successfully!")
        cur.close()

    except Exception:
        raise DbConnectionError("Failed to read data from DB")

    finally:
        if db_connection:
            db_connection.close()
            print("DB connection is closed")  # always close connection after opening it


#used in testing
#insert_records()

