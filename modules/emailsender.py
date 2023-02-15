import smtplib
import email.message

def sendEmail(message):  

    msg = email.message.Message()
    msg['Subject'] = "Automatização LINX - OK"
    msg['From'] = 'fg.bi.notifications@gmail.com'
    msg['To'] = 'bi@agenciafg.com.br'
    password = '' 
    msg.add_header('Content-Type', 'text/html')
    msg.set_payload(message )

    s = smtplib.SMTP('smtp.gmail.com: 587')
    s.starttls()
    # Login Credentials for sending the mail
    s.login(msg['From'], password)
    s.sendmail(msg['From'], [msg['To']], msg.as_string().encode('utf-8'))
