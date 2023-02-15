import requests
import modules.config as config
from datetime import datetime , timedelta
from collections import defaultdict
from modules.CRUD import insert, update, read , readRepurchaseData , lastUpdateDate

def extractOrders():
  response = requests.request("POST", config.url, headers=config.headers, data=config.payload)
  print(response)
  if str(response) != "<Response [200]>":
    return False
  else:
    jsondata = response.json()
    jsondata = jsondata['Result']
    return jsondata

def treatOrders(jsondata , type):
  completeorders = []
  repurchaseUpdateList = []
  for websiteid in config.websiteids:
    for order in jsondata:
        if str(order['WebSiteID']) == str(websiteid):            
            order['CreatedDate'] = str(order['CreatedDate'])[6:-10]
            order['CreatedDate'] = str(datetime.fromtimestamp(int(order['CreatedDate'])))
            order['CreatedDate'] = str(datetime.strptime(order['CreatedDate'], "%Y-%m-%d %H:%M:%S") - timedelta(hours=config.timezone_diff))

            #padroniza para o datalake - alguns números se repetem entre lojas
            order['OrderNumber'] = str(order['OrderNumber']) + "-" + config.storeName
            order['CustomerID'] = str(order['CustomerID']) + " - " + config.storeName

            repurchaseNumber = 'Null'
            daysSinceLastOrder = 0
            repurchaseClient = 'Null'

            #conta a quantidade de dias desde o ultimo pedido
            if type == "load":
                repurchaseData = readRepurchaseData(order['CustomerID'] , config.storeName)
                for repurchase in repurchaseData:
                    try:
                        daysSinceLastOrder = datetime.strptime(order['CreatedDate'], '%Y-%m-%d %H:%M:%S') - repurchase.f0_
                        daysSinceLastOrder = daysSinceLastOrder.days
                    except:
                        daysSinceLastOrder = 0

                    if repurchase.f1_ is None:
                        repurchaseNumber = 0
                    else:
                        repurchaseNumber = int(repurchase.f1_) + 1
                        repurchaseClient = True
                        if repurchase.repurchaseClient is None:
                            repurchaseUpdateList.append(order['CustomerID'])
                            

            order['PaymentStatus'] = config.statusList[str(order['PaymentStatus'])]
            try:
                order['ShipmentStatus'] = config.shipmentStatusList[str(order['ShipmentStatus'])]
            except:
                order['ShipmentStatus'] = str(order['ShipmentStatus'])
            try:
                order['OrderStatusID'] = config.orderStatusIDList[str(order['OrderStatusID'])]
            except:
                order['OrderStatusID'] = str(order['OrderStatusID'])
            try:
                data_set = [{"orderId"    : order['OrderNumber'] , 
                            "creationDate": order['CreatedDate'] , 
                            "status"      : order['PaymentStatus'] ,
                            "paymentNames": order['PaymentMethods'][0]['PaymentInfo']['Alias'],
                            "totalValue"  : order['Total'],
                            "shipmentStatus" : order['ShipmentStatus'],
                            "orderStatusID" : order['OrderStatusID'],
                            "clientDocument" : order['CustomerID'],
                            "daysSinceLastOrder" : daysSinceLastOrder,
                            "repurchaseNumber" : repurchaseNumber,
                            "repurchaseClient" : repurchaseClient
                            }]
            except:
                data_set = [{"orderId"    : order['OrderNumber'] , 
                            "creationDate": order['CreatedDate'] , 
                            "status"      : order['PaymentStatus'] ,
                            "paymentNames": None,
                            "totalValue"  : order['Total'],
                            "shipmentStatus" : order['ShipmentStatus'],
                            "orderStatusID" : order['OrderStatusID'],
                            "clientDocument" : order['CustomerID'],
                            "daysSinceLastOrder" : daysSinceLastOrder,
                            "repurchaseNumber" : repurchaseNumber,
                            "repurchaseClient" : repurchaseClient
                            }]
            completeorders.extend(data_set)
  if type == "load":
    if repurchaseUpdateList:
        update("repurchaseClient = True" , "clientDocument IN ({})".format(str(repurchaseUpdateList)[1:-1]) , "Repurchase status updated.")
    else:
        print("No recurrent clients to uptade.")

  return completeorders

def loadList(orders):
    lenCounter = 0
    try:
        lenOrders = len(orders)
    except:
        lenOrders = 0
    insertString = ""
    print('Loading data into Big Query.')
    for order in orders:
        lenCounter = lenCounter + 1
        insertString = insertString + "( '" + str(order['orderId']) +"' , '"+ str(order['creationDate']) +"' , '"+ str(order['status']) +"' , '"+ str(order['paymentNames']) +"' , "+ str(order['totalValue']) +" , '"+ str(order['shipmentStatus']) +"' , '"+ str(order['orderStatusID']) +"' , '"+ "Linx" + "' , '" + config.storeName +"' , '" + str(order['clientDocument']) +"' , "+ str(order['daysSinceLastOrder']) +" , "+ str(order['repurchaseNumber']) +" , "+ str(order['repurchaseClient'])+" )"
        if lenCounter != len(orders):
            insertString = insertString + " , "
    if insertString == "":
        print("No insertions to execute.")
        lastUpdateDate(str(datetime.today() - timedelta(1))[0:10])
    else:
        insert(insertString)
        print('Data was successfully loaded.')
        lastUpdateDate(str(datetime.today() - timedelta(1))[0:10])
    
    return lenOrders

def newOrders(counter , orders):
    while counter > 0:
        config.setWhere(counter, counter - 1)
        extractedOrders = extractOrders()
        if extractedOrders == False:
            config.addFailedStore(str(config.storeName))
            print("Extraction error")
        else:
            orderCounter = loadList(treatOrders(extractedOrders , "load"))
        counter = counter - 1
        orders = orders + orderCounter
        
    try:
        return orders
    except:
        return 0

def updateOrders():
    config.setWhere(15 , 1)
    extractedOrders = extractOrders()
    if extractedOrders == False:
        print("Extraction error")
    else:
        vtexOrders = treatOrders(extractedOrders , "update")
        orderId = ''
        orderUpadateList = defaultdict(list)
        statusList = []
        #cria a consulta com os parametros dos chamados da vtex
        for vtexOrder in vtexOrders:
            orderId = orderId + str("'{}' , ".format(vtexOrder['orderId']))
        orderId = orderId[:-3]
        readCondition = "orderId IN ({}) AND ecommerceName = '{}'".format(orderId , config.storeName)

        #Consulta os chamados o bigquery de acordo com os pedidos da vtex
        try:
            bqOrders = read(config.table_id , readCondition)
            for bqOrder in bqOrders:
                #essa linha filtra e cria uma lista com apenas 1 item, aquele que corresponde ao orderId do bigquery
                vtexOrder = list(filter(lambda x:x["orderId"]==str(bqOrder.orderId),vtexOrders))
                status = str(vtexOrder[0]['status'])
                if (bqOrder.status != status):
                    test = str(status) in orderUpadateList
                    if (test == False):
                        #cria a chave dentro do json
                        orderUpadateList[status] = [bqOrder.orderId]
                        #cria lista de status
                        statusList.append(status)
                    else:
                        #adiciona orderId na chave criada acima
                        orderUpadateList[status].append(bqOrder.orderId)

            for status in statusList:
                updateCondition = "orderId IN ({})".format(str(orderUpadateList[status])[1:-1])
                update("status = '" + status + "'", updateCondition , str("Orders status uptade to: " + status))
        except:
            print('Update error')
    return