import functions_framework
import json
import requests
import time, datetime, calendar
from google.cloud import storage
from google.cloud.sql.connector import Connector, IPTypes
import sqlalchemy, pymysql

# CONSTANTS
APP_ID = 'd16b1167069c24328f77797175f4d365'
LOCATION = 'Melbourne,AU'
WEATHER_DATA_URL = f'http://api.openweathermap.org/data/2.5/weather?q={LOCATION},de&units=metric&APPID={APP_ID}'
FILE_NAME = f'weather_current_date_{calendar.timegm(datetime.datetime.now().timetuple())}.csv'

# Cloud SQL Connection
connection_name = "amiable-vent-425413-a7:us-central1:demo-seasonal-sales-analysis"
public_ip = "34.28.50.62"
port = 3306
db_name = "demo-seasonal-sales-analysis"
db_user = "admin"
db_password = "admin1234"
driver_name = 'mysql+pymysql'
query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})

# TEST API CALL
# http://api.openweathermap.org/data/2.5/weather?q=Melbourne,AU,de&units=metric&APPID=d16b1167069c24328f77797175f4d365


def get_data():
    """get weather data from openweathermap"""
    response = requests.get(WEATHER_DATA_URL)
    data = json.loads(response.text)

    temp = data['main']['temp']
    pressure = data['main']['pressure']
    temp_min = data['main']['temp_min']
    temp_max = data['main']['temp_max']
    humidity = data['main']['humidity']
    wind_speed = data['wind']['speed']
    try:
        wind_gust = data['wind']['gust']
    except KeyError:
        wind_gust = None
    wind_deg = data['wind']['deg']
    clouds = data['clouds']['all']
    try:
        rain = data['rain']['3h']
    except KeyError:
        rain = None
    try:
        snow = data['snow']['3h']
    except KeyError:
        snow = None
    weather_id = data['weather'][0]['id']
    sunrise = data['sys']['sunrise']
    sunset = data['sys']['sunset']

    return [temp, pressure, temp_min, temp_max, humidity, wind_speed, wind_gust, wind_deg, clouds, rain, snow,
            weather_id, sunrise, sunset]



def save_data_to_bucket(data):
    blob = BUCKET.blob(FILE_NAME)
    blob.upload_from_string(data=json.dumps(data), content_type='application/json')
    return 'JSON file (FILE_NAME) successfully uploaded to ' + bucket_name


# def save_data_to_db(data):
#     con = sqlite3.connect("PATH_TO_SQLITE_FILE")
#     con.isolation_level = None
#     cur = con.cursor()
#     query = '''INSERT INTO open_weather_data VALUES (null, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
#     cur.execute(query, tuple([datetime.datetime.now().isoformat()] + data[:-2]))
#     con.commit()
#     con.close()

# initialize Cloud SQL Connector
connector = Connector()

# SQLAlchemy database connection creator function
def getconn():
    conn = connector.connect(
        "34.28.50.62", # Cloud SQL Instance Connection Name
        "pymysql",
        user="admin",
        password="admin1234",
        db="demo-seasonal-sales-analysis",
        ip_type=IPTypes.PUBLIC # IPTypes.PRIVATE for private IP
    )
    return conn

# create SQLAlchemy connection pool
pool = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
)

# interact with Cloud SQL database using connection pool
with pool.connect() as db_conn:
    # query database
    result = db_conn.execute("SELECT * from products_by_date").fetchall()
    print(result)
    # Do something with the results
    for row in result:
        print(row)

# close Cloud SQL Connector
connector.close()


def save_data_to_db(request):   #You can change this to match your personal details
    stmt = sqlalchemy.text('''INSERT INTO open_weather_data VALUES (null, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''')

    db = sqlalchemy.create_engine(url = db_url)

    db = sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL(
            drivername=driver_name,
            username=db_user,
            password=db_password,
            database=db_name,
            query=query_string,
            host=public_ip,
            port=port
        ),
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800
    )
    try:
        with db.connect() as conn:
            conn.execute(stmt)
            print("Insert successful")
    except Exception as e:
        print ("Some exception occured" + e)
        return 'Error: {}'.format(str(e))
    return 'ok' 


@functions_framework.http
def fetch_weather_data_api(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    data = get_data()
    time.sleep(10)
    now = calendar.timegm(datetime.datetime.now().timetuple())
    if data[-2] < now < data[-1]:
        save_data_to_db(data)


# Involke
# functions-framework --target fetch_weather_data_api
