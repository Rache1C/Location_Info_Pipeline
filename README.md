Project Description
This is a pipeline which gets data on wind gusts (kts) and wind direction (ºT) for a chosen location (latitude and longitude) for the next 48 hours. 

Prerequisites

Before you continue, ensure you have met the following requirements:

* You have installed the latest version of Python3.
* You have a Python IDE with compiler/means by which to execute Python files.
* You have access to a MySQL database. (MySQL Workbench was used for this project).
* You have the following packages installed: mysqlconnector, pandas, csv and requests.

Running the Pipeline

* Clone this repo, currently private, please request access.
* Add your database credentials to the config.py file.
* Get API key for https://openweathermap.org/api/one-call-api and add this to config.py file for "weather_api_key" variable.
* Run the etl.py file to invoke all required functions.
