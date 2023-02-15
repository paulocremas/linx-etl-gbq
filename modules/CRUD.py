import modules.config as config

#Insere pedidos no BigQuery -- aka CREATE
# def insertOrders(orderId , creationDate , status , paymentNames , totalValue , shipmentStatus , orderStatusID , clientDocument , daysSinceLastOrder , repurchaseNumber , repurchaseClient):
#     rows_to_insert = [
#             {u'orderId': str(orderId) , u'creationDate': str(creationDate) , u'status': str(status) , u'paymentNames': str(paymentNames) , u'totalValue': str(totalValue) , u'shipmentStatus': str(shipmentStatus) , u'orderStatusID': str(orderStatusID) , u'platformName' : "Linx" , u'ecommerceName' : config.storeName , u'clientDocument' : clientDocument, u'daysSinceLastOrder' : daysSinceLastOrder , u'repurchaseNumber' : repurchaseNumber , u'repurchaseClient' : repurchaseClient}
#             ]
        
#     errors = config.client.insert_rows_json(config.table_id, rows_to_insert)
#     if errors == []:
#         counter = 1
#     else:
#         print(f'Encountered errors while inserting rows: {errors}')
#     return counter

def insert(insertString):
    query_job = config.client.query("""
        INSERT INTO {} (orderId , creationDate , status , paymentNames , totalValue , shipmentStatus , orderStatusID , platformName , ecommerceName , clientDocument , daysSinceLastOrder , repurchaseNumber , repurchaseClient)
        VALUES {}
        """.format(config.table_id , insertString))
    orders = query_job.result()
    return orders

#lê dados dos big query -- aka READ
def read(table_id , condition):
    query_job = config.client.query("""
        SELECT *
        FROM {}
        WHERE {}
        """.format(table_id , condition))  
    orders = query_job.result()
    return orders

def readRepurchaseData(clientDocument , ecommerceName):
    query_job = config.client.query("""
        SELECT MAX(creationDate) , MAX(repurchaseNumber) , repurchaseClient
        FROM `sacred-drive-353312.datalakes.orders`
        WHERE clientDocument = '{}' AND ecommerceName = '{}'
        GROUP BY repurchaseClient;
        """.format(clientDocument , ecommerceName))
    result = query_job.result()
    return result

#deleta os dados das condições configurada -- aka DELETE
def delete():
    query_job = config.client.query("""
        DELETE FROM {}
        WHERE {};
        """.format(config.table_id , config.interval)) 
    query_job.result()

    return

def update(update , condition , message):
    query_job = config.client.query("""
        UPDATE {}
        SET {}
        WHERE {};
        """.format(config.table_id , update , condition)) 
    query_job.result()

    print(message)

    return

def lastUpdateDate(update):
    query_job = config.client.query("""
        UPDATE `sacred-drive-353312.config_linx.storesConfig`
        SET lastUpdateDatalake = "{}"
        WHERE store = "{}";
        """.format(update , config.storeName))
    query_job.result()

    print('Update date updataded.')

    return