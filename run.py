from main import updateDatalake , insertDatalake
from modules.emailsender import sendEmail
import modules.config as configuration

orderCounter = insertDatalake()
updateDatalake()
if orderCounter != 0:
    sendEmail("""<p>Lojas LINX atualizadas com sucesso</p>
            <p>Lista de erros: {}</p>
            <p>Pedidos inseridos: {}</p>""".format(configuration.failedStores , str(orderCounter)).encode('utf-8'))
    print('Email enviado')
