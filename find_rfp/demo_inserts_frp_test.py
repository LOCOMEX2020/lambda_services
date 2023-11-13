# from pymongo import MongoClient
from faker import Faker
import psycopg2
fake = Faker()
from datetime import datetime
import pandas as pd

def getDB():
    conn = psycopg2.connect(
    host="devdblocomex.c2zteosxyngj.us-east-1.rds.amazonaws.com",
    database="devdblocomex",
    user="postgres",
    password="pSq#Mn&Bqk7YHCKR",
    port="5432")
    return conn

if __name__ == '__main__':
    db = getDB()
    # df = pd.read_csv('/home/osmany/work/locomex/find-frp/find_rfp/rfp.csv')
    # # convert nan to empty string
    # df = df.fillna('')
    # # make sure due_date and end_date are in datetime format if not set None
    # df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce')
    # df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
    # df['due_date'] = (df['due_date']
    #                       .astype(str)
    #                       .replace({'NaT': None}
    #                      ))
    # df['end_date'] = (df['end_date']
    #                 .astype(str)
    #                 .replace({'NaT': None}
    #                 ))
    # # let's split latlong into latitude and longitude
    # df['latitude'] = df['latlon'].str.split(',').str[0]
    # df['longitude'] = df['latlon'].str.split(',').str[1]
    # # rename values to value
    # df = df.rename(columns={'values': 'value'})
    batch_insert = []
    # # let's insert sql query on batch insert using cursor.executemany
    # cursor = db.cursor()
    # for index, row in df.iterrows():
    #     cursor.execute("""INSERT INTO public.rfp_rfp (title, contact_name, contact_email, contact_phone, rfp_url, due_date, end_date,
    #     description, address, city, state, zipcode, naics, industries, value, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
    #     %s, %s, %s, %s, %s, %s, %s, %s)""", (row['title'], row['contact_name'], row['contact_email'], row['contact_phone'], row['rfp_url'],
    #     row['due_date'], row['end_date'], row['description'], row['address'], row['city'], row['state'], row['zipcode'], row['naics'],
    #     row['industries'], row['value'], row['latitude'], row['longitude']))
    # db.commit()
    # cursor.close()                                                                                               
    
    row_to_insert = 30000
    # create a list of 1000000 rows
    for i in range(row_to_insert):
        # create a fake data
        title = fake.text(max_nb_chars=100)
        contact_name = fake.name()
        contact_email = fake.email()
        contact_phone = fake.phone_number()
        rfp_url = fake.url()
        due_date = fake.date_time_between(start_date='-1y', end_date='+1y')
        end_date = fake.date_time_between(start_date='-1y', end_date='+1y')
        description = fake.text(max_nb_chars=5000)
        address = fake.street_address()
        city = fake.city()
        # we add state but setting the state abreviation
        state = fake.state_abbr()
        zipcode = fake.zipcode()
        naics = fake.numerify(text='#######')
        industries = fake.text(max_nb_chars=100)
        value = fake.numerify(text='#######')
        longitude = fake.longitude()
        latitude = fake.latitude()
        # create a sql query
        sql = """INSERT INTO public.rfp_rfp (title, contact_name, contact_email, contact_phone, rfp_url, due_date, end_date,
        description, address, city, state, zipcode, naics, industries, value, latitude, longitude) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}',
        '{}', '{}', '{}', '{}', '{}', '{}', {}, {});""".format(title, contact_name, contact_email, contact_phone, rfp_url,
        due_date, end_date, description, address, city, state, zipcode, naics, industries, value, latitude, longitude)
        # append to the list
        batch_insert.append(sql)
        # print current i added and missing
        print('i: {}'.format(i))
        print('missing: {}'.format(row_to_insert - i))
        
    # now we have to insert the data into the database 100 at a time
    # we have to split the list into 100 items
    # https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
    batch_insert = [batch_insert[i:i + 100] for i in range(0, len(batch_insert), 100)]
    batch_inserted = 0
    for batch in batch_insert:
        # now we have to insert the entire bath
        # https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
        cur = db.cursor()
        cur.execute("\n".join(batch))
        db.commit()
        cur.close()
        # print progress and missing batches
        batch_inserted += 1
        print('batch_inserted: {}'.format(batch_inserted))
        print('missing batches: {}'.format(len(batch_insert) - batch_inserted))

    # run a count to make sure we have the same amount of rows
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) FROM public.rfp_rfp;")
    rows = cur.fetchall()
    print(rows)