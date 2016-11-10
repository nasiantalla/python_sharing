
import requests
import pandas as pd
import time
import psycopg2

file = '/Users/athanasiantalla/Desktop/zipcode_all.csv'
reader = pd.read_csv(file, sep=';', encoding='utf-8-sig',dtype=str)

zipcodes = reader["zipcode"].astype(str)
base_url = "https://api.lieferheld.de/restaurants/?zipcode={zipcode}&fields=id&limit=4000"
headers = {'Authentication': 'LH api-key=VXOEO5ezqBJIz0L6UAp46OjWSvGg7TA0DzdafBQ2Cf8g0Rbuu9FvOS9vE9BWmCMUzt63cPG4slTKR7apDF4WLnyy6xF1qmsxE6B80019ya0B292OgJmvK5rGhsxUVD96, token=2e113a99a9776fd78f6260d4e77f0555e1cd1985707c660762b45eb87ee9ff6e'}
con = psycopg2.connect("dbname= user= password= host= port=")
cur = con.cursor()
cur.execute('CREATE TABLE delivery_zipcodes_api (zipcode text, restaurant_id text)')
cur.execute('TRUNCATE TABLE delivery_zipcodes_api')
con.commit()
for zipcode in zipcodes:
    url = base_url.format(zipcode=zipcode)
    r = requests.get(url,
                     headers=headers)
    for r_info in r.json()["data"]:
        restaurant_id = r_info["id"]
        zipcode = zipcode
        cur.execute("INSERT INTO delivery_zipcodes_api (zipcode, restaurant_id) VALUES (%s,%s)", (zipcode,restaurant_id))
        con.commit()
    time.sleep(0.5)

