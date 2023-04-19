import ast
import modules.config as configuration
from modules.ETL import newOrders, updateOrders
from datetime import datetime as dt
from modules.emailsender import sendEmail

def run():
  print("Updates")
  storesConfigUpdate = configuration.storesConfig("update")
  for config in storesConfigUpdate:
    print('------------------------')
    print(config.store)
    configuration.setGlobalConfig(ast.literal_eval(config.data) , config.store)
    updateOrders()

  orderCounter = 0
  print('------------------------')
  print()
  print("Insertions")
  storesConfigInsert = configuration.storesConfig("insertion")
  for config in storesConfigInsert:
    print('------------------------')
    print(config.store)
    configuration.setGlobalConfig(ast.literal_eval(config.data) , config.store)
    days = dt.today() - config.lastUpdateDatalake
    days = days.days
    days = days - 1
    orderCounter = newOrders(days  , orderCounter) 
  
  if orderCounter != 0:
    sendEmail("""<p>Lojas LINX atualizadas com sucesso</p>
            <p>Lista de erros: {}</p>
            <p>Pedidos inseridos: {}</p>""".format(configuration.failedStores , str(orderCounter)).encode('utf-8'))
    print('Email enviado')
  return orderCounter

run()
print("Complete.")