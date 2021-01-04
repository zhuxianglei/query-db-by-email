#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
	Version  Created by     Creation Time  Description
	-------  -------------  -------------  ----------------------------------------------
	1.0      xlzhu@ips.com  2018-11-13     upload mail attachment&send mail
	
	Version  Updated by     Update Time    Description
	-------  -------------  -------------  ----------------------------------------------
	
'''
import os
import smtplib
import public
import time
import shutil
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from common import refresh_ini,setfilepgok,getfilepgok,saveobject,updateflagmsg_byuidflag
from common import logaq,get2betw13,writefile,getidxbyuidflag,deldict_fromlist,breaktime,getpdbyen
#from common import init_dirs,init_log,refresh_paras#!!!!!!!!must delete in prod
#import threading                    #!!!!!!!!!!!!!!!!!!!!!!!must delete in prod

#contants&variables
ms_cmd_merge='rem --This cmd file created by '+public.GSYS_FNAME+','+public.GVERSION+'\r\n'+ \
             'rem --Purpose:Combining multiple files which splited by linux/unix shell into one\r\n' + \
             'copy /b FILENAME.* FILENAME'
MS_MSGH="Dear all,\r\n  "
MS_MSGT="\r\nThanks!"
ms_msg1='Dear all,\r\n  This is a system mail of '+public.GSYS_FNAME+','+public.GVERSION+ \
		';\r\n  MaxTime of each mailquery is limited to MAX_QTIMEMin;\r\n'+ \
		'  System breaktime(HH24):STOPTIME;\r\n'+ \
		'  Any questions,Please contact with DBA@IPS.COM.\r\nThanks!'
MS_MSG2="This query spend a long time(MAX_QTIMEMin),Please check&modify your sql."
MS_MSG3="This mail can't be replied after a long time(MAX_QTIMEMin),Please check&modify&send again."
ms_serv=None
 
def send_mail(pfrom,pto,pcc,psub,pcontt,pdiratta,plstatta):
	#mailheader
	global ms_serv
	message = MIMEMultipart()
	message['From'] = pfrom
	message['To'] = pto
	message['Cc'] = pcc
	message['Subject'] = psub
	#mailbody
	message.attach(MIMEText(pcontt, 'plain','utf-8'))#plain or html 
	#attachment
	if pdiratta and plstatta:
		for filen in plstatta.split(','):
			logaq('uploading mail attachment:'+filen,'i')
			atta = MIMEApplication(open(pdiratta+'\\'+filen,'rb').read())
			atta.add_header('Content-Disposition', 'attachment', filename=filen)
			message.attach(atta)
	try:
		lreceivers=pto+','+pcc
		logaq('receivers:'+lreceivers)
		ms_serv.sendmail(pfrom,lreceivers.split(','),message.as_string())
	except Exception as e:
		logaq('mailss sendmail Error: %s' % e,'e')	
		ms_serv=smtplib.SMTP_SSL(public.gshst, public.gsprt)
		ms_serv.login(public.gmusr, getpdbyen(public.gmenpd))
		logaq('smtp server relogin ok','i')
		ms_serv.sendmail(pfrom,lreceivers.split(','),message.as_string())

def process_exceptmaillst(pmaillst,pmutex):
	#1.reply flag=E mail
	#2.reply flag=B&Spent>30Min mail
	#3.reply flag=G&Stayfor>2H
	idx=0
	while idx<len(pmaillst):
		if pmaillst[idx]['ftime'] and pmaillst[idx]['flag']==public.GSTATUS_SQLDATA_BEFEXPORT and (time.time()-pmaillst[idx]['ftime'])>public.gmmaxqtime+60:
			updateflagmsg_byuidflag(pmaillst,pmutex,pmaillst[idx]['uid'],public.GSTATUS_SQLDATA_BEFEXPORT,public.GSTATUS_ERROR,MS_MSG2.replace('MAX_QTIME',str(public.gmmaxqtime/60)))
		if pmaillst[idx]['ftime'] and pmaillst[idx]['flag']==public.GSTATUS_SQLDATA_DOWNLOADED and (time.time()-pmaillst[idx]['ftime'])>public.gmmaxqtime*4:
			updateflagmsg_byuidflag(pmaillst,pmutex,pmaillst[idx]['uid'],public.GSTATUS_SQLDATA_DOWNLOADED,public.GSTATUS_ERROR,MS_MSG3.replace('MAX_QTIME',str(public.gmmaxqtime/60)))
		if pmaillst[idx]['flag']==public.GSTATUS_ERROR:
			logaq('sendmail uid:'+pmaillst[idx]['uid'])
			send_mail(public.gmusr,pmaillst[idx]['from']+','+pmaillst[idx]['to'],pmaillst[idx]['cc'],'Re:'+pmaillst[idx]['subject'],MS_MSGH+pmaillst[idx]['msg']+MS_MSGT,'','')
			logaq('deleting mailmeta by uid')
			deldict_fromlist(pmaillst,pmutex,'uid',pmaillst[idx]['uid'])
			idx=idx-1
		idx=idx+1
		
def relogin_mail(ppretime):
	global ms_serv
	try:
		if time.time()-ppretime>public.gmcycle:
			try:
				ms_serv.quit()
				logaq('mailss servquit(>cycle)','w')
			except Exception as e:
				logaq('mailss conquit: %s' % e,'e')
			ms_serv=smtplib.SMTP_SSL(public.gshst, public.gsprt)
			ms_serv.login(public.gmusr, getpdbyen(public.gmenpd))
			logaq('smtp server login ok','i')
	except Exception as e:
		logaq('mailss ini cycle: %s' % e,'e')	
	
def mails_main(pmaillst,pmutex,pdirattachment):
	if breaktime():
		exit()
	try:
		global ms_serv
		global ms_msg1
		lpretime=time.time()
		ms_serv=smtplib.SMTP_SSL(public.gshst, public.gsprt)
		ms_serv.login(public.gmusr, getpdbyen(public.gmenpd))
		logaq('smtp server login ok','i')
		while not breaktime():
			while not getfilepgok(public.gmutex_filepg) and not breaktime():
				logaq('waiting for filepg '+str(public.gsleep)+'s...','i')
				time.sleep(public.gsleep)
				logaq('process exception mail')
				process_exceptmaillst(pmaillst,pmutex)
				continue
			pretmpmuid=''
			tmpmuid='';
			existsplitf=False;
			relogin_mail(lpretime)
			lpretime=time.time()
			try:
				lexistnewfile=False			
				fileslst = os.listdir(pdirattachment)
				fileslst.sort()
				for i in range(0,len(fileslst),1):
					tmplstfile=fileslst[i]
					logaq('file:'+tmplstfile,'i')
					lpath = os.path.join(pdirattachment,tmplstfile)
					if time.time()-os.path.getmtime(lpath)<=1:
						logaq('mtime<=1:'+lpath)
						lexistnewfile=True
						continue
					tmpmuid=get2betw13(tmplstfile,public.GFLAG_UID,public.GSUFFIX_TARGZFILEMAIL)
					#--split file process
					if tmpmuid==pretmpmuid:
						existsplitf=True
					if (existsplitf and (i<len(fileslst)-1) and get2betw13(fileslst[i+1],public.GFLAG_UID,'_')!=tmpmuid) \
						or (existsplitf and i==len(fileslst)-1):
						tmpfilelike=get2betw13(tmplstfile,'',public.GSUFFIX_TARGZFILEMAIL)+public.GSUFFIX_TARGZFILEMAIL
						writefile(pdirattachment+'UID'+pretmpmuid,ms_cmd_merge.replace('FILENAME',tmpfilelike),'.cmd')
						tmplstfile=tmplstfile+',UID'+pretmpmuid+'.cmd'
						existsplitf=False
					#--end split file process	
					idxmail=getidxbyuidflag(tmpmuid,public.GSTATUS_SQLDATA_DOWNLOADED,pmaillst)	
					ltmpuid=pmaillst[idxmail]['uid']
					#get mailheader info
					if idxmail>-1:
						logaq('sendmail uid:'+tmpmuid)
						ms_msg1=ms_msg1.replace('STOPTIME',public.gstop_time)
						ms_msg1=ms_msg1.replace('MAX_QTIME',str(public.gmmaxqtime/60))
						send_mail(public.gmusr,pmaillst[idxmail]['from']+','+pmaillst[idxmail]['to'],pmaillst[idxmail]['cc'],'Re:'+pmaillst[idxmail]['subject'],ms_msg1,public.gdir_atachmt_willsend,tmplstfile)
					if pmaillst[idxmail]['uid']!=ltmpuid:
						logaq('!!!!!!multithreads data conflict!!!!!!','e')
					if os.path.exists(public.gdir_history+fileslst[i]):
						os.remove(public.gdir_history+fileslst[i])
					logaq('moving file to his uid:'+tmpmuid)
					shutil.move(lpath,public.gdir_history)
					#--delete pmaillst
					if pretmpmuid and tmpmuid!=pretmpmuid:
						logaq('deleting mailmeta by preuid(cur<>pre)')
						deldict_fromlist(pmaillst,pmutex,'uid',pretmpmuid)
					if i==(len(fileslst)-1):
						logaq('deleting mailmeta by uid(lastone)')
						deldict_fromlist(pmaillst,pmutex,'uid',tmpmuid)
					#--end delete
					pretmpmuid=tmpmuid
				process_exceptmaillst(pmaillst,pmutex)
				if not lexistnewfile:
					setfilepgok(public.gmutex_filepg,False)
				else:
					time.sleep(3)
				logaq(pmaillst)
				logaq('while end','i')
			except Exception as e:
				logaq('mailss Error: %s' % e,'e')
				time.sleep(3)
			finally:
				logaq('while finally','i')
				saveobject(pmaillst,'maills4')
				#refresh_ini()
	finally:
		try:
			ms_serv.quit()
		except Exception as e:
			logaq('smtp exit exception: %s' % e,'i')
#test codes
'''
tmpmalst=[{'uid':'1435935728','from': 'xiangll115@163.com', 'to': '"xlzhu@ips.com" <xlzhu@ips.com>, "henry.zhu@outlook.com" <henry.zhu@outlook.com>', 'cc': 'siqi.zhu@outlook.com, "78108673@qq.com" <78108673@qq.com>', 'subject': 'A中国风-中國風E0', 'flag': 'S','ftime':1, 'user': 'bnk_2b', 'tns': 'bnk7'}]
tmppalst=[]
refresh_ini()
init_dirs()
init_log('-v')
#2.refresh parameters
refresh_paras(tmppalst)
tmpmutex=threading.Lock()
setfilepgok(public.gmutex_filepg,True)
mails_main(tmpmalst,tmpmutex,public.gdir_atachmt_willsend)

#writefile('e:\\test\\'+'UID1234567890',ms_cmd_merge,'.cmd')

ms_serv=smtplib.SMTP_SSL(MS_LOGINHST, public.gsprt)
ms_serv.login(public.gmusr, public.gmpd)
send_mail(ms_serv,'xlzhu@ips.com','"78108673@qq.com" <78108673@qq.com>, "henry.zhu@outlook.com" <henry.zhu@outlook.com>','QueryData',MS_MSG1,'','')
'''
	
