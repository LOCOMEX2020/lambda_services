from math import sqrt
import json
import psycopg2
import pandas as pd
import jwt

notAuthorized = 'Unauthorized'

def getDB(environment):
    if environment == 'dev':
        conn = psycopg2.connect(
        host="devdblocomex.c2zteosxyngj.us-east-1.rds.amazonaws.com",
        database="devdblocomex",
        user="postgres",
        password="pSq#Mn&Bqk7YHCKR",
        port="5432")
        return conn
    conn = psycopg2.connect(
        host="locomex.c2zteosxyngj.us-east-1.rds.amazonaws.com",
        database="locomex_db",
        user="locomex_db_user",
        password="GQZgN#7PRt&e9z4e",
        port="5432")
    return conn

def response_400(message):
    return {
        "statusCode": 400,
        "body": json.dumps({
            "message": message,
        }),
    }

def auth(event):
    token = event.get("headers", {}).get("Authorization", "")
    if token == "":
        return 'Unauthorized'
    try:
        # remove Bearer from token
        token = token.split(" ")[1]
        decoded = jwt.decode(token, 'h*bFHAzPPz.dnqv_Ut9RHGaiVD92.Bd6A', algorithms=['HS256'])
        environment = decoded.get("environment", "")
        if environment != "dev" and environment != "prod":
            return notAuthorized
        return environment
    except:
        print("Error decoding token or token expired {}".format(token))
    return notAuthorized

def get_rfp_by_id(environment, event, context):
    # return event
    rfp_id = event.get("pathParameters", {}).get("rfp_id", "")
    if rfp_id == "":
        return response_400('rfp_id is required')
    body = event.get("body", "{}")
    if type(body) == str:
        body = json.loads(body)
    supplier_naics_code_list = body.get("supplier_naics", [])
    lat_lon = str(body.get("lat_lon", "")) # latitude and longitude
    supplier_state = str(body.get("supplier_state", ""))
    if supplier_state == '':
        return response_400('supplier_state is required')
    search_by_distance = False
    if lat_lon != '':
        search_by_distance = True
    db = getDB(environment)
    cur = db.cursor()
    cur.execute("SELECT * FROM public.rfp_rfp WHERE id = %s;", (rfp_id,))
    rfp = cur.fetchone()
    if rfp is None:
        return response_400('rfp_id does not exist')
    # let's fetch columns names and create a dataframe
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'rfp_rfp';")
    columns = cur.fetchall()
    columns = [x[0] for x in columns]
    df = pd.DataFrame([rfp], columns=columns)
    # let's convert due_date and end_date to string
    df['due_date'] = df['due_date'].astype(str)
    df['end_date'] = df['end_date'].astype(str)
    # let's latitude and longitude to string
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df['win_rate'] = "Low"
    for index, row in df.iterrows():
        if row['naics'] in supplier_naics_code_list and row['state'] == supplier_state:
            df.at[index, 'win_rate'] = "High"
        elif row['naics'] in supplier_naics_code_list or row['state'] == supplier_state:
            df.at[index, 'win_rate'] = "Medium"
    # let's calculate distance
    if search_by_distance:
        latitude = float(lat_lon.split(',')[0])
        longitude = float(lat_lon.split(',')[1])
        # let's get distance 100 miles to degrees
        # 1 degree = 69 miles
        # 100 miles = 1.449275362318841 degrees
        # distance = 1.449275362318841
        df['distance'] = sqrt((latitude - df['latitude'])**2 + (longitude - df['longitude'])**2)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "rfp": df.to_dict(orient='records'),
        }),
    }

def lambda_handler(event, context):
    environment = auth(event)
    if environment == notAuthorized:
        return response_400(notAuthorized)
    
    method = event.get("httpMethod", "")
    if method == "PATCH":
        return get_rfp_by_id(environment, event, context)
    body = event.get("body", "{}")
    if type(body) == str:
        body = json.loads(body)
    # supplier state is a field required
    supplier_state = str(body.get("supplier_state", ""))
    if supplier_state == '':
        return response_400('supplier_state is required')
    state = str(body.get("state", ""))
    lat_lon = str(body.get("lat_lon", "")) # latitude and longitude
    # state is a field required
    if state == '' and lat_lon == '':
        state = supplier_state
    search_by_distance = False
    if lat_lon != '':
        search_by_distance = True
    naics_code = str(body.get("industry", ""))
    sort = str(body.get("sort", ""))
    # validate sort has to be due_date or win_rate
    if sort != "" and (sort != 'due_date' and sort != 'win_rate'):
        return response_400('sort has to be due_date or win_rate')
    supplier_naics_code_list = body.get("supplier_naics", [])
    db = getDB(environment)
    # get rfp by state
    # we have to set limit to avoid overhead
    limit_to_fetch = 30000
    cur = db.cursor()
    if search_by_distance:
        latitude = lat_lon.split(',')[0]
        longitude = lat_lon.split(',')[1]
        # let's get distance 100 miles to degrees
        # 1 degree = 69 miles
        # 100 miles = 1.449275362318841 degrees
        distance = 1.449275362318841
        if naics_code != "":
            query = """
                    SELECT * FROM (
                    SELECT *, sqrt((latitude - %s)^2 + (longitude - %s)^2) AS distance
                    FROM public.rfp_rfp) rfp
                    WHERE rfp.naics = %s AND rfp.distance <= %s
                    ORDER BY rfp.distance DESC
                    LIMIT %s; 
                """
            cur.execute(query, (latitude, longitude, naics_code, distance, limit_to_fetch))
        else:
            query = """
                    SELECT * FROM (
                    SELECT *, sqrt((latitude - %s)^2 + (longitude - %s)^2) AS distance
                    FROM public.rfp_rfp) rfp
                    WHERE rfp.distance <= %s
                    ORDER BY rfp.distance DESC
                    LIMIT %s; 
                """
            cur.execute(query, (latitude, longitude, distance, limit_to_fetch))
    else:
        if naics_code != "":
            cur.execute("SELECT * FROM public.rfp_rfp WHERE state = %s AND naics = %s LIMIT %s;", (state, naics_code, limit_to_fetch,))
        else:
            cur.execute("SELECT * FROM public.rfp_rfp WHERE state = %s LIMIT %s;", (state, limit_to_fetch,))
    rfp_state = cur.fetchall()
    # let's fetch columns names and create a dataframe
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'rfp_rfp';")
    columns = cur.fetchall()
    # if we are searching by distance we have to add distance column
    if search_by_distance:
        columns.append(('distance',))
    columns = [x[0] for x in columns]
    df = pd.DataFrame(rfp_state, columns=columns)
    # let's add column win_rate
    # win rate is 3 when value if supplier_naics_code_list match with naics and and supplier_state match with state
    # win rate is MEDIUM when value if supplier_naics_code_list match with naics or supplier_state match with state
    # win rate is LOW when value if supplier_naics_code_list does not match with naics and supplier_state does not match with state
    df['win_rate'] = "Low"
    for index, row in df.iterrows():
        if row['naics'] in supplier_naics_code_list and row['state'] == supplier_state:
            df.at[index, 'win_rate'] = "High"
        elif row['naics'] in supplier_naics_code_list or row['state'] == supplier_state:
            df.at[index, 'win_rate'] = "Medium"
    if sort != '':
        df = df.sort_values(by=[sort], ascending=False)
    # let's get first 25 rows
    df = df.head(25)
    # let's convert due_date and end_date to string
    df['due_date'] = df['due_date'].astype(str)
    df['end_date'] = df['end_date'].astype(str)
    # let's latitude and longitude to string and distance
    df['latitude'] = df['latitude'].astype(str)
    df['longitude'] = df['longitude'].astype(str)
    if search_by_distance:
        df['distance'] = df['distance'].astype(str)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "rfp": df.to_dict(orient='records'),
        }),
    }
