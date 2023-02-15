import os
import ast
import json
from modules.CRUD import read
from google.cloud import bigquery
from datetime import datetime , timedelta

global client
global table_id
credentials_path = os.environ['path_google_credentials']
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()
table_id = "sacred-drive-353312.datalakes.orders"

def storesConfig(type):
    global statusList
    global shipmentStatusList
    global orderStatusIDList
    global failedStores
    failedStores = []
    
    query = read('sacred-drive-353312.config_linx.status' , 'TRUE')
    data = {}
    for row in query:
        data[row.statusID] = row.statusName
        statusList = json.dumps(data)
        statusList = json.loads(statusList)

    query = read('sacred-drive-353312.config_linx.shipmentStatus' , 'TRUE')
    data = {}
    for row in query:
        data[row.shipmentStatusID] = row.shipmentStatusName	
        shipmentStatusList = json.dumps(data)
        shipmentStatusList = json.loads(shipmentStatusList)
  
    query = read('sacred-drive-353312.config_linx.orderStatusID' , 'TRUE')
    data = {}
    for row in query:
        data[row.statusIDNumber] = row.statusIDName
        orderStatusIDList = json.dumps(data, ensure_ascii=False)
        orderStatusIDList = json.loads(orderStatusIDList)
    
    if type == "update": 
        query = read('sacred-drive-353312.config_linx.storesConfig' , 'active is True'.format(str(datetime.today() - timedelta(1))[0:10]))
        return query
    else:
        query = read('sacred-drive-353312.config_linx.storesConfig' , 'active is True AND lastUpdateDatalake <> DATE("{}")'.format(str(datetime.today() - timedelta(1))[0:10]))
        return query

def setGlobalConfig(storesConfig , store):
    global headers
    global url
    global config
    global websiteids
    global timezone_diff
    global storeName
    global repurchaseClientsUpdate

    config = storesConfig
    headers = ast.literal_eval(storesConfig['headers'])
    url = storesConfig['url']
    websiteids = config['website_id']
    timezone_diff = config['timezone_diff']
    storeName = store
    repurchaseClientsUpdate = []

    return

def setWhere(last , first):
    global payload

    lastdate = str('\"' + str(datetime.today() - timedelta(last))[:10] + '\"')
    date = '\"' + str(datetime.today() - timedelta(first))[:10] + '\"'
    completeWhere = "Createddate >= {lastdate} && Createddate < {date}".format(lastdate = lastdate,date = date)
    payload = json.dumps({
    "Page": {
        "PageIndex": 0,
        "PageSize": 0
    },
    "Where": completeWhere,
    })
    return

def addFailedStore(failedStore):
    failedStores.append(failedStore)
    return

def addRepurchaseClients(repurchaseClients):
    repurchaseClientsUpdate.append(repurchaseClients)
    return
