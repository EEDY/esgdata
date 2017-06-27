import smtplib
from email.mime.text import MIMEText

import threading

mailSmtpsvc="smtp.126.com"
mailUser="tpc_test"
mailPass="traf1234"

mailFrom="tpc_test@126.com"
mailTo=["qilin.chen@esgyn.cn"]

mailSub="tpc_test message"
mailContent="test mail!"


class MailHelper(object):
	def __init__(self,user=mailUser,password=mailPass,smtpsvc=mailSmtpsvc):
		self.__lock=threading.Lock()
		self.__user=user
		self.__password=password
		self.__smtpsvc=smtpsvc
		self.__from=mailFrom
		self.__to=mailTo
		msg = MIMEText(mailContent,_subtype='plain',_charset='utf-8')
		msg['Subject'] =mailSub
		msg['From'] = self.__user+'<'+self.__from+'>'
		msg['To'] = ";".join(self.__to)
		self.__msg=msg.as_string()

	def setAddress(self,eFrom=mailFrom,eTo=mailTo):
		self.__lock.acquire()
		self.__from=eFrom
		self.__to=eTo
		self.__lock.release()

	def setContent(self,subject=mailSub,content=mailContent):
		self.__lock.acquire()
		msg = MIMEText(content,_subtype='plain',_charset='utf-8')
		msg['Subject'] = subject
		msg['From'] = self.__user+'<'+self.__from+'>'
		msg['To'] = ";".join(self.__to)
		self.__msg=msg.as_string()
		self.__lock.release()

	def sendMail(self):
		try:
			self.__lock.acquire()
			svc=smtplib.SMTP()
			svc.connect(self.__smtpsvc)
			svc.login(self.__user,self.__password)
			svc.sendmail(self.__from,self.__to,self.__msg)
			svc.close()
			self.__lock.release()
			return True
		except Exception,e:
			print(str(e))
			self.__lock.release()
			return False		
if __name__ == '__main__':
	a=MailHelper()
	a.setContent("Youv","kill -9 ...")	
	if a.sendMail():
		print "ok"
	else:
		print "faild"

