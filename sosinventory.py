import sys
import time
import datetime
import requests
import json
import pandas
import pandasql
from math import ceil

BEGIN_TIME = datetime.datetime.now()

#######################################################################################################################
#
# Functions
#
#######################################################################################################################


# Check if Content-Size of request < 500000
def databox(u, h, d, t):
    _s = requests.Session()
    _r = requests.Request('POST', u, data=d, headers=h, auth=(t, ''))
    _p = _s.prepare_request(_r)
    if int(_p.headers['Content-Length']) > 500000:
        return False
    return True


# Yield successive n-sized chunks from lst
def split(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# Pass globals on every SQL query
def sql(query):
    return pandasql.sqldf(query, globals())


#######################################################################################################################
#
# Globals
#
#######################################################################################################################

payload = []

#######################################################################################################################
# Databox
#######################################################################################################################

DATABOX_API = 'https://push.databox.com'
DATABOX_TOKEN = 't6gtkjqyv1eif3xsexlzwc'
DATABOX_HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/vnd.databox.v2+json'
}

#######################################################################################################################
# SOS Inventory
#######################################################################################################################

SOSINVENTORY_API_AUTH = 'https://api.sosinventory.com/oauth2/token'
SOSINVENTORY_API_ACCESS_TOKEN = None
SOSINVENTORY_HEADERS_AUTH = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Host': 'api.sosinventory.com',
    'Accept': 'application/json'
}
SOSINVENTORY_BODY_AUTH = {
    'grant_type': 'refresh_token',
    'refresh_token': None
}
SOSINVENTORY_API_DATA = 'https://api.sosinventory.com/api/v2'
SOSINVENTORY_HEADERS_DATA = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}
SOSINVENTORY_PARAMS_DATA_BUILD = {
    'maxresults': 200,
    'start': 1
}
SOSINVENTORY_PARAMS_DATA_ITEM = {
    'maxresults': 200,
    'start': 1
}
SOSINVENTORY_PARAMS_DATA_SALE = {
    'maxresults': 200,
    'start': 1
}
SOSINVENTORY_PARAMS_DATA_SHIPMENT = {
    'maxresults': 200,
    'start': 1
}
SOSINVENTORY_PARAMS_DATA_WORK_ORDERS = {
    'maxresults': 200,
    'start': 1
}

#######################################################################################################################
#
# Use refresh token to get a new access token
#
#######################################################################################################################

with open('token.txt', 'r') as f:
    refresh_token = f.read().rstrip()
f.close()

if not refresh_token:
    raise Exception('Python > Error: No refresh token found.')

SOSINVENTORY_BODY_AUTH['refresh_token'] = refresh_token

print('Python > Generating access token')

content = requests.post(SOSINVENTORY_API_AUTH,
                        headers=SOSINVENTORY_HEADERS_AUTH,
                        data=SOSINVENTORY_BODY_AUTH).json()

if 'access_token' in content and 'refresh_token' in content:
    SOSINVENTORY_API_ACCESS_TOKEN = content['access_token']
    SOSINVENTORY_HEADERS_DATA['Authorization'] = 'Bearer ' + SOSINVENTORY_API_ACCESS_TOKEN
    with open('token.txt', 'w') as f:
        f.write(content['refresh_token'])
    f.close()
else:
    raise Exception('SOSInventory > API Error: Could not generate access token.')

#######################################################################################################################
#
# Fetch and parse builds
#
#######################################################################################################################

builds = []
for archived in ['no', 'yes']:

    while True:

        time.sleep(0.5)

        SOSINVENTORY_PARAMS_DATA_BUILD['archived'] = archived

        print('SOSInventory > Fetching builds > /build >', archived, '> start >',
              SOSINVENTORY_PARAMS_DATA_BUILD['start'])

        content = requests.get(SOSINVENTORY_API_DATA + '/build',
                               params=SOSINVENTORY_PARAMS_DATA_BUILD,
                               headers=SOSINVENTORY_HEADERS_DATA).json()

        if content['status'] != 'ok':
            raise Exception('SOSInventory > API Error > /build >', archived, '> start >', content['message'])

        if not content['count']:
            print('SOSInventory > Warning > /build >', archived, '> start >',
                  SOSINVENTORY_PARAMS_DATA_BUILD['start'], '> No builds found.')

        for build in content['data']:
            date = build['date'][:10]
            for item in build['outputs']:
                builds.append({
                    'date': date,
                    'id': item['item']['id'],
                    'name': item['item']['name'],
                    'quantity': item['quantity']
                })

        if content['count'] == 200:
            SOSINVENTORY_PARAMS_DATA_BUILD['start'] += 200
        else:
            break

#######################################################################################################################
#
# Fetch and parse items
#
#######################################################################################################################

items = []
for archived in ['no', 'yes']:

    while True:

        time.sleep(0.5)

        SOSINVENTORY_PARAMS_DATA_ITEM['archived'] = archived

        print('SOSInventory > Fetching builds > /item >', archived, '> start >',
              SOSINVENTORY_PARAMS_DATA_ITEM['start'])

        content = requests.get(SOSINVENTORY_API_DATA + '/item',
                               params=SOSINVENTORY_PARAMS_DATA_ITEM,
                               headers=SOSINVENTORY_HEADERS_DATA).json()

        if content['status'] != 'ok':
            raise Exception('SOSInventory > API Error > /item >', archived, '> start >',
                            SOSINVENTORY_PARAMS_DATA_ITEM['start'], '>', content['message'])

        if not content['count']:
            print('SOSInventory > Warning > /item >', archived, '> start >',
                  SOSINVENTORY_PARAMS_DATA_ITEM['start'], '> No items found.')

        for item in content['data']:
            items.append({
                'id': item['id'],
                'name': item['name'],
                'category': item['category']['name'] if item['category'] else 'Not Set'
            })

        if content['count'] == 200:
            SOSINVENTORY_PARAMS_DATA_ITEM['start'] += 200
        else:
            break

#######################################################################################################################
#
# Fetch and parse sales
#
#######################################################################################################################

sales = []
for archived in ['no', 'yes']:

    while True:

        time.sleep(0.5)

        SOSINVENTORY_PARAMS_DATA_SALE['archived'] = archived

        print('SOSInventory > Fetching sales > /salesorder >', archived, '> start >',
              SOSINVENTORY_PARAMS_DATA_SALE['start'])

        content = requests.get(SOSINVENTORY_API_DATA + '/salesorder',
                               params=SOSINVENTORY_PARAMS_DATA_SALE,
                               headers=SOSINVENTORY_HEADERS_DATA).json()

        if content['status'] != 'ok':
            raise Exception('SOSInventory > API Error > /salesorder >', archived, '> start >',
                            SOSINVENTORY_PARAMS_DATA_SALE['start'], '>', content['message'])

        if not content['count']:
            print('SOSInventory > Warning > /salesorder >', archived, '> start >',
                  SOSINVENTORY_PARAMS_DATA_SALE['start'], '> No sales found.')

        for sale in content['data']:

            date = sale['date'][:10]
            if isinstance(sale['department'], dict):
                department = sale['department']['name']
            else:
                department = 'Not set'

            for item in sale['lines']:
                sales.append({
                    'date': date,
                    'id': item['item']['id'],
                    'name': item['item']['name'],
                    'quantity': item['quantity'],
                    'amount': ((item['quantity'] - item['shipped']) * item['unitprice']) if not sale['closed'] else 0,
                    'department': department,
                    'closed': sale['closed']
                })

        if content['count'] == 200:
            SOSINVENTORY_PARAMS_DATA_SALE['start'] += 200
        else:
            break

#######################################################################################################################
#
# Fetch and parse shipments
#
#######################################################################################################################

shipments = []
for archived in ['no', 'yes']:

    while True:

        time.sleep(0.5)

        SOSINVENTORY_PARAMS_DATA_SHIPMENT['archived'] = archived

        print('SOSInventory > Fetching shipments > /shipment >', archived, '> start >',
              SOSINVENTORY_PARAMS_DATA_SHIPMENT['start'])

        content = requests.get(SOSINVENTORY_API_DATA + '/shipment',
                               params=SOSINVENTORY_PARAMS_DATA_SHIPMENT,
                               headers=SOSINVENTORY_HEADERS_DATA).json()

        if content['status'] != 'ok':
            raise Exception('SOSInventory > API Error > /shipment >', archived, '> start >',
                            SOSINVENTORY_PARAMS_DATA_SHIPMENT['start'], '>', content['message'])

        if not content['count']:
            print('SOSInventory > Warning > /shipment >', archived, '> start >',
                  SOSINVENTORY_PARAMS_DATA_SHIPMENT['start'], '> No shipments found.')

        for shipment in content['data']:
            date = shipment['date'][:10]
            for item in shipment['lines']:
                shipments.append({
                    'date': date,
                    'id': item['item']['id'],
                    'name': item['item']['name'],
                    'quantity': item['quantity']
                })

        if content['count'] == 200:
            SOSINVENTORY_PARAMS_DATA_SHIPMENT['start'] += 200
        else:
            break

#######################################################################################################################
#
# Fetch and parse work orders
#
#######################################################################################################################

work_orders = []
for archived in ['no', 'yes']:

    while True:

        time.sleep(0.5)

        SOSINVENTORY_PARAMS_DATA_WORK_ORDERS['archived'] = archived

        print('SOSInventory > Fetching sales > /workorder >', archived, '> start >',
              SOSINVENTORY_PARAMS_DATA_WORK_ORDERS['start'])

        content = requests.get(SOSINVENTORY_API_DATA + '/workorder',
                               params=SOSINVENTORY_PARAMS_DATA_WORK_ORDERS,
                               headers=SOSINVENTORY_HEADERS_DATA).json()

        if content['status'] != 'ok':
            raise Exception('SOSInventory > API Error > /workorder >', archived, '> start >',
                            SOSINVENTORY_PARAMS_DATA_WORK_ORDERS['start'], '>', content['message'])

        if not content['count']:
            print('SOSInventory > Warning > /workorder >', archived, '> start >',
                  SOSINVENTORY_PARAMS_DATA_WORK_ORDERS['start'], '> No work orders found.')

        for work_order in content['data']:

            date = work_order['date'][:10]

            work_orders.append({
                'date': date,
                'id': work_order['id'],
                'closed': 0 if work_order['closed'] else 1
            })

        if content['count'] == 200:
            SOSINVENTORY_PARAMS_DATA_WORK_ORDERS['start'] += 200
        else:
            break

#######################################################################################################################
#
# Prepare the metrics
#
#######################################################################################################################

df_builds = pandas.DataFrame(builds)
df_items = pandas.DataFrame(items)
df_sales = pandas.DataFrame(sales)
df_shipments = pandas.DataFrame(shipments)
df_work_orders = pandas.DataFrame(work_orders)

# Open Sales Orders Amount
q = """SELECT date, SUM(CASE WHEN closed = 'True' THEN 0 ELSE amount END) AS metric
       FROM df_sales
       GROUP BY date"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Open_Sales_Orders_Amount': round(data['metric'], 6),
        'date': data['date']
    }
    payload.append(metric.copy())

# Open Work Orders
q = """SELECT date, SUM(closed) AS metric
       FROM df_work_orders
       GROUP BY date"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Open_Work_Orders': round(data['metric'], 6),
        'date': data['date']
    }
    payload.append(metric.copy())

# Sales
q = """SELECT s.date, SUM(s.amount) AS metric
       FROM df_sales s
       WHERE s.closed = 'True'
       GROUP BY date"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Sales_Orders_Amount': round(data['metric'], 2),
        'date': data['date']
    }
    payload.append(metric.copy())

# Sales by Department
q = """SELECT s.date, s.department AS dimension, SUM(s.amount) AS metric
       FROM df_sales s
       GROUP BY date, dimension"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Sales_Orders_Amount': round(data['metric'], 2),
        'date': data['date'],
        'Department': data['dimension'],
    }
    payload.append(metric.copy())

# Sales by Item
q = """SELECT s.date, s.name AS dimension, SUM(s.amount) AS metric
       FROM df_sales s
       INNER JOIN df_items i ON i.id = s.id 
       WHERE i.category IN('SPACEWORX USA ASSEMBLY', 'SPACEWORX COMPLETE (Metal) PODS')
       GROUP BY date, dimension"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Sales_Orders_Amount': round(data['metric'], 2),
        'date': data['date'],
        'Item': data['dimension'],
    }
    payload.append(metric.copy())

# Units Built
q = """SELECT date, SUM(quantity) AS metric
       FROM df_builds
       GROUP BY date"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Units_Built': round(data['metric'], 2),
        'date': data['date']
    }
    payload.append(metric.copy())

# Units Built by Category
q = """SELECT b.date, i.category AS dimension, SUM(b.quantity) AS metric
       FROM df_builds b
       INNER JOIN df_items i ON i.id = b.id
       GROUP BY date, dimension"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Units_Built': round(data['metric'], 2),
        'date': data['date'],
        'Category': data['dimension'],
    }
    payload.append(metric.copy())

# Units Shipped
q = """SELECT date, SUM(quantity) AS metric
       FROM df_shipments
       GROUP BY date"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Units_Shipped': round(data['metric'], 2),
        'date': data['date']
    }
    payload.append(metric.copy())

# Units Shipped by Category
q = """SELECT b.date, i.category AS dimension, SUM(b.quantity) AS metric
       FROM df_shipments b
       INNER JOIN df_items i ON i.id = b.id
       GROUP BY date, dimension"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Units_Shipped': round(data['metric'], 2),
        'date': data['date'],
        'Category': data['dimension'],
    }
    payload.append(metric.copy())

# Units Sold
q = """SELECT date, SUM(quantity) AS metric
       FROM df_sales
       GROUP BY date"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Units_Sold': round(data['metric'], 2),
        'date': data['date']
    }
    payload.append(metric.copy())

# Units Sold by Category
q = """SELECT b.date, i.category AS dimension, SUM(b.quantity) AS metric
       FROM df_sales b
       INNER JOIN df_items i ON i.id = b.id
       GROUP BY date, dimension"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Units_Sold': round(data['metric'], 2),
        'date': data['date'],
        'Category': data['dimension'],
    }
    payload.append(metric.copy())

# Units Sold by Department
q = """SELECT s.date, s.department AS dimension, SUM(s.quantity) AS metric
       FROM df_sales s
       GROUP BY date, dimension"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Units_Sold': round(data['metric'], 2),
        'date': data['date'],
        'Department': data['dimension'],
    }
    payload.append(metric.copy())

# Units Sold by Item
q = """SELECT s.date, s.name AS dimension, SUM(s.quantity) AS metric
       FROM df_sales s
       INNER JOIN df_items i ON i.id = s.id 
       WHERE i.category IN('SPACEWORX USA ASSEMBLY', 'SPACEWORX COMPLETE (Metal) PODS')
       GROUP BY date, dimension"""

r = sql(q).to_dict(orient='index')

for x in r:
    data = r[x]
    metric = {
        '$Units_Sold': round(data['metric'], 2),
        'date': data['date'],
        'Item': data['dimension'],
    }
    payload.append(metric.copy())

#######################################################################################################################
#
# Push to Databox
#
#######################################################################################################################

if len(payload):

    metrics = payload
    print('Python > Calculating payload size')

    limit = 500000
    size = sys.getsizeof(json.dumps(metrics))
    while True:

        # Split payload into n chunks of size = limit
        slices = ceil(size / limit)
        chunks = ceil(len(metrics) / slices)

        # Check if all chunks are within the limit
        send = True
        for data in split(metrics, chunks):
            if not databox(DATABOX_API, DATABOX_HEADERS, json.dumps({'data': data}), DATABOX_TOKEN):
                send = False

        if not send:
            limit -= 10000
        else:
            break

    # Push chunks to Databox
    print('Python > Pushing data > Size >', size, '> Pushes >', slices)

    for data in split(metrics, chunks):
        response = requests.post(DATABOX_API,
                                 headers=DATABOX_HEADERS,
                                 data=json.dumps({'data': data}),
                                 auth=(DATABOX_TOKEN, ''))

        if response.ok:
            result = response.json()
            print('Databox >', sys.getsizeof(json.dumps({'data': data})), '>', result['status'], result['message'])
            time.sleep(0.5)
        else:
            print('Databox > Error >', response.status_code, response.reason)
            sys.exit("Python > Push aborted.")


print(datetime.datetime.now() - BEGIN_TIME)
