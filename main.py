import ast
import modules.config as configuration
from modules.ETL import newOrders, updateOrders
from datetime import datetime as dt

def updateDatalake():
  print("Updates")
  storesConfigUpdate = configuration.storesConfig("update")
  for config in storesConfigUpdate:
    print('------------------------')
    print(config.store)
    configuration.setGlobalConfig(ast.literal_eval(config.data) , config.store)
    updateOrders()
  return

def insertDatalake():
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
    orderCounter = newOrders(days - 1 , orderCounter)
  return orderCounter

