#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
	Version  Created by     Creation Time  Description
	-------  -------------  -------------  ----------------------------------------------
	1.0      xlzhu@ips.com  2018-10-25     ssh exec shell cmd,sftp put&get files to/from server
										   db-dbuser,dbpwdencrypted,osuser,ospwdencrypted,osip,osport	
										   solving BUG about diff between Linux&SunOS,20181126
	Version  Updated by     Update Time    Description
	-------  -------------  -------------  ----------------------------------------------

'''

import os
import time
import shutil
import paramiko
import public
from common import get2betw13
from common import getdbtnsdbusr,logaq,getdbusrbydbtns,saveobject,clearblankchar,replaceline_infile
from common import getoinforbytnsusr,breaktime,setfilepgok,gets2fileok,refresh_ini,sets2fileok,existflag_inmailslst
from common import updateflagmsg_byuidflag,gettnsset_inmailslst,getidxkeywordinlst,getuidstrlst_inmailslst
#import threading #temp test,must delete
#from common import init_dirs,init_log,refresh_paras,refresh_ini #temp test,must delete


#contants
PG_MSG1='Exception:attachment/mail infor match error!,please modify your mail by right format,send again!'
PG_MSG2="Warning:data export error,can't find result file in server(mlst=S),please contact dba@ips.com"
PG_MSG3="Warning:can't get data,attachment encoding format maybe error,Please modify&send again(ANSI<->UTF8)."
#PG_CMD_GZIP1M="find ./ -name 'UID*.csv' -size +1M -exec  gzip {} \\;"
PG_ENV_BASH={'$SHELL':'bash'}
PG_CMD_LPATH=os.path.split(os.path.realpath(__file__))[0]+'\\'
#PG_CMD_SPLIT50M="echo -e 'find ./ -name UID\*.tar.gz -size +50M -exec split -b 50M -d -a 2 {} {}. \\;'>>UIDcsvtargz.sh"#-d {} not support in SunOS
#PG_CMD_RMRF="find ./ -name 'UID*.tar.gz' -exec rm -rf {} \\;"BUG
#PG_CMD_TARGZ01="echo -e '#!/bin/bash'>UIDcsvtargz.sh"
#PG_CMD_TARGZ02="echo -e ulp=\'UIDLST\'>>UIDcsvtargz.sh"
#PG_CMD_TARGZ03="echo -e 'for f1 in `find ./ -name UID\*csv|cut -d_ -f1|cut -d/ -f2|sort -u`\ndo\n fu=`echo $f1|sed s/UID//g`\n cnt=`echo $ulp|grep $fu|wc -l`\n if [ $cnt -ge 1 ] ; then\n  rm -rf $f1.tar*\n  tar -cf $f1.tar $f1*csv\n  gzip $f1.tar\n  rm -rf $f1*csv\n fi\ndone'>>UIDcsvtargz.sh"
#PG_CMD_RMGZBEFSPLIT="echo -e 'for f1 in `find ./ -name UID\*.tar.gz|cut -d. -f2|cut -d/ -f2|sort -u`\ndo\n split -b 50m -a 2 $f1.tar.gz $f1.tar.gz.\n rm -rf $f1.tar.gz\ndone'>>UIDcsvtargz.sh"
PG_CMD_TEMPLATE='TARGZSPLIT_TEMPLATE.sh'
PG_CMD_KEYWORD='UIDLIST_BYCOMMA'
PG_CMD_EXESH='UIDFtargzsplit.sh'
PG_CMD_SH='sh UIDFtargzsplit.sh'



def put_main(pmailslst,pparaslst,pmutex):
	fileslst = os.listdir(public.gdir_atachmt_downloaded)
	fileslst.sort()
	curmuid=-1
	predbdusr='';
	curdbdusr=''
	for i in range(0,len(fileslst),1):
		print(fileslst[i])
		path = os.path.join(public.gdir_atachmt_downloaded,fileslst[i])
		#process file context
		
		#
		curdbdusr=getdbtnsdbusr(get2betw13(fileslst[i],'UID','_'),public.GSTATUS_ATACHMT_DOWNLOADED,pmailslst)
		print(curdbdusr)
		if not curdbdusr:
			os.rename(path,path.replace(public.GSUFFIX_FILEMAIL,'.urr'))
			logaq('moving file to senddir')
			shutil.move(path.replace(public.GSUFFIX_FILEMAIL,'.urr'),public.gdir_atachmt_willsend)
			updateflagmsg_byuidflag(pmailslst,pmutex,get2betw13(fileslst[i],'UID','_'),public.GSTATUS_ATACHMT_DOWNLOADED,public.GSTATUS_ERROR,PG_MSG1)
			if i>0:
				updateflagmsg_byuidflag(pmailslst,pmutex,get2betw13(fileslst[i-1],'UID','_'),public.GSTATUS_ATACHMT_DOWNLOADED,public.GSTATUS_ATACHMT_UPLOADED,'')
		elif predbdusr!=curdbdusr:
			#getoinfo
			if predbdusr:
				transport.close()
			loip,loprt,lousr,lopd=getoinforbytnsusr(curdbdusr,pparaslst)
			print(loip+','+loprt+','+lousr+','+lopd)
			transport = paramiko.Transport((loip, int(loprt)))
			transport.connect(username=lousr, password=lopd)
			sftp = paramiko.SFTPClient.from_transport(transport)	
		if os.path.isfile(path):
			sftp.put(path, '/home/test/'+fileslst[i])
			shutil.move(path,public.gdir_atachmt_uploaded)
			#updatedict_inlist(pmailslst,pmutex,pg_dictname_uid,get2betw13(fileslst[i],'UID','_'),pg_dictname_flag,public.GSTATUS_ATACHMT_DOWNLOADED,public.GSTATUS_ATACHMT_UPLOADED)
		if (predbdusr and predbdusr!=curdbdusr) or (i==len(fileslst)-1):
			idxlast=i-1
			if i==len(fileslst)-1:
				idxlast=len(fileslst)-1
			print(get2betw13(fileslst[idxlast],'UID','_'))
			updateflagmsg_byuidflag(pmailslst,pmutex,get2betw13(fileslst[idxlast],'UID','_'),public.GSTATUS_ATACHMT_DOWNLOADED,public.GSTATUS_ATACHMT_UPLOADED,'')
		if i==len(fileslst)-1:
			transport.close()
		#if fileslst[i].find('DW')>-1:#fileuid not match mailslst uid
		#print('----\n'+public.gdir_atachmt_uploaded+fileslst[i]+'\n'+public.gdir_atachmt_uploaded+'err.err'+'----\n')
		#os.rename(public.gdir_atachmt_uploaded+fileslst[i],public.gdir_atachmt_uploaded+'err.err')
		predbdusr=curdbdusr
		
def targzipsplit_servfiles(ptransport,puidstringlst):
	#ssh = paramiko.SSHClient()
	#ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	#ssh.connect(hostname='ip', port=22, username='test', password='test#1234')
	#tmploginhome=''
	ssh = paramiko.SSHClient()
	ssh._transport = ptransport
	try:
		#stdin, stdout, stderr = ssh.exec_command('pwd')
		#res,err = stdout.read(),stderr.read()
		#result = res if res else err
		#tmploginhome=result.decode()
		#logaq(tmploginhome)
		'''
		stdin, stdout, stderr = ssh.exec_command(command=PG_CMD_TARGZ01)#,environment=PG_ENV_BASH
		res,err = stdout.read(),stderr.read()
		result = res if res else err
		logaq('echo tar&gzip01>*.sh:'+result.decode())
		stdin, stdout, stderr = ssh.exec_command(command=PG_CMD_TARGZ02.replace('UIDLST',puidstringlst))
		res,err = stdout.read(),stderr.read()
		result = res if res else err
		logaq('echo tar&gzip02>>*.sh:'+result.decode())
		stdin, stdout, stderr = ssh.exec_command(command=PG_CMD_TARGZ03)
		res,err = stdout.read(),stderr.read()
		result = res if res else err
		logaq('echo tar&gzip03>>*.sh:'+result.decode())
		#remarked for solaris split -d {} bug
		stdin, stdout, stderr = ssh.exec_command(command=PG_CMD_SPLIT50M)
		res,err = stdout.read(),stderr.read()
		result = res if res else err
		logaq('echo split nM file>>*.sh:'+result.decode())
		stdin, stdout, stderr = ssh.exec_command(command=PG_CMD_RMGZBEFSPLIT)#window copy /b /path/UID* /path/UID.zip;
		res,err = stdout.read(),stderr.read()
		result = res if res else err
		logaq('echo rm -rf *tar.gz(exist *.gz.00)>>*.sh:'+result.decode())
		'''
		stdin, stdout, stderr = ssh.exec_command(PG_CMD_SH)
		res,err = stdout.read(),stderr.read()
		result = res if res else err
		logaq('sh *.sh:'+result.decode())
		#return tmploginhome
	finally:
		#ssh.close() #Error SSH session not active
		pass

def put_shfile(ptransport,plocalshname,premoteshname):
	sftp = paramiko.SFTPClient.from_transport(ptransport)
	try:
		sftp.put(plocalshname,premoteshname)
	finally:
		sftp.close()
		
def getdel_servfiles(ptransport,pserloginhome,plocalpath,pfilelstremove):
	sftp = paramiko.SFTPClient.from_transport(ptransport)
	try:
		tmpfiles = sftp.listdir()
		logaq('sftp filelst:'+str(tmpfiles),'i')
		tmpfiles.sort()
		for tfile in tmpfiles:
			if tfile.find('UID')>-1 and tfile.find('tar.gz')>0:
				logaq('get&del:'+tfile,'i')
				sftp.get(pserloginhome+tfile,plocalpath+tfile)
				pfilelstremove.append(tfile)
				sftp.remove(pserloginhome+tfile)
	finally:
		sftp.close()
		
def process_mailslst(pmailslst,pmutex,pservfileprocessedlst,pdbtns):
	idxtmp=0
	while idxtmp<len(pmailslst):
		luid=pmailslst[idxtmp]['uid']
		if getidxkeywordinlst(pservfileprocessedlst,pmailslst[idxtmp]['uid'])>-1:
			updateflagmsg_byuidflag(pmailslst,pmutex,pmailslst[idxtmp]['uid'],public.GSTATUS_SQLDATA_EXPORTED,public.GSTATUS_SQLDATA_DOWNLOADED,'')
		elif pmailslst[idxtmp]['dbtns']==pdbtns and pmailslst[idxtmp]['flag']==public.GSTATUS_SQLDATA_EXPORTED:
			updateflagmsg_byuidflag(pmailslst,pmutex,pmailslst[idxtmp]['uid'],public.GSTATUS_SQLDATA_EXPORTED,public.GSTATUS_ERROR,PG_MSG3)
		if luid!=pmailslst[idxtmp]['uid']:
			logaq('!!!!!!multithreads data conflict(maillst idx which being used is deleted)','e')
		idxtmp=idxtmp+1

def process_localfiles(pmailslst,pmutex,pservfileprocessedlst,pfrompath,ptopath):
	#1.move file to his dir
	#2.update maillst status by uid&flag
	fileslst = os.listdir(pfrompath)
	fileslst.sort()
	for i in range(0,len(fileslst),1):
		if getidxkeywordinlst(pservfileprocessedlst,get2betw13(fileslst[i],public.GFLAG_UID,public.GSEPTOR_FILE))>-1:
			#move to ptopath
			logaq('moving file to '+ptopath)
			shutil.move(pfrompath+fileslst[i],ptopath+fileslst[i])
			updateflagmsg_byuidflag(pmailslst,pmutex,get2betw13(fileslst[i],public.GFLAG_UID,public.GSEPTOR_FILE),public.GSTATUS_SQLDATA_EXPORTED,public.GSTATUS_SQLDATA_DOWNLOADED,'')
		else:
			#local file exist,serverfile no exist
			tmpidx=getidxbyuidflag(get2betw13(fileslst[i],public.GFLAG_UID,public.GSEPTOR_FILE),public.GSTATUS_SQLDATA_EXPORTED,pmailslst)
			if tmpidx>-1:
				#os.rename(pfrompath+fileslst[i],(pfrompath+fileslst[i]).replace(public.GSUFFIX_FILEMAIL,'.drr'))
				logaq('moving file to send')
				shutil.move(pfrompath+fileslst[i],public.gdir_atachmt_willsend)
				updateflagmsg_byuidflag(pmailslst,pmutex,get2betw13(fileslst[i],public.GFLAG_UID,public.GSEPTOR_FILE),public.GSTATUS_SQLDATA_EXPORTED,public.GSTATUS_ERROR,PG_MSG2)
		
def get_main(pmailslst,pparaslst,pmutex):
	'''
	#1.get all servers(tnslist) where login to download UID*.csv files
	#2.ssh to server for tar&biz2&split all UID*.csv files
	#3.sftp to server for downloading&removing all UID*.gz or UID*.gz.00(00 stand for two bit number)
	#4.move local file TNS_UID0000000000.sql to his dir
	#5.update pmailst status to sqldata_downloaded
	'''
	tmpfilelst=[]
	transport=None
	while not breaktime():
		while not gets2fileok(public.gmutex_s2file) and not breaktime():
			logaq('waiting for s2file '+str(public.gsleep)+'s...','i')
			time.sleep(public.gsleep)
			continue
		strlstuid10=','
		try:
			for serv in gettnsset_inmailslst(pmailslst,public.GSTATUS_SQLDATA_EXPORTED):
				tmpip,tmpprt,tmpusr,tmppd=getoinforbytnsusr(serv+getdbusrbydbtns(serv,pparaslst),pparaslst)
				transport = paramiko.Transport((tmpip, int(tmpprt)))
				transport.connect(username=tmpusr, password=tmppd)
				logaq('server connect ok','i')
				while strlstuid10:
					strlstuid10=getuidstrlst_inmailslst(pmailslst,serv,public.GSTATUS_SQLDATA_EXPORTED,strlstuid10)
					if strlstuid10:
						logaq('process uid:'+strlstuid10,'i')
						replaceline_infile(PG_CMD_LPATH,PG_CMD_TEMPLATE,PG_CMD_EXESH,PG_CMD_KEYWORD,strlstuid10)
						put_shfile(transport,PG_CMD_LPATH+PG_CMD_EXESH,PG_CMD_EXESH)
						targzipsplit_servfiles(transport,strlstuid10)
				del tmpfilelst[:]
				getdel_servfiles(transport,'./',public.gdir_atachmt_willsend,tmpfilelst)
				#process_localfiles(pmailslst,pmutex,tmpfilelst,public.gdir_atachmt_downloaded,public.gdir_history)
				process_mailslst(pmailslst,pmutex,tmpfilelst,serv)
				logaq('sleep 3s for nextloop...','i')
				time.sleep(3)
			logaq('filepg.get_main end','i')
			if not existflag_inmailslst(pmailslst,public.GSTATUS_SQLDATA_EXPORTED):
				sets2fileok(public.gmutex_s2file,False)
			setfilepgok(public.gmutex_filepg,True)
			logaq(pmailslst)
		except Exception as e:
			logaq('filespg Error: %s' % e,'e')
			time.sleep(3)
		finally:
			if transport:
				try:
					transport.close()
				except Exception as e:
					logaq('transport close exception: %s' % e,'i')
			logaq('Dowload datafile of SQLExported over','i')
			saveobject(pmailslst,'maills3')
			#refresh_ini()
		
#test codes
'''
glparaslst=[]
refresh_ini()
init_dirs()
init_log('-v')
refresh_paras(glparaslst)
transport = paramiko.Transport(('ip', 22))
transport.connect(username='test', password='test#1234')

replaceline_infile(PG_CMD_LPATH,PG_CMD_TEMPLATE,PG_CMD_EXESH,PG_CMD_KEYWORD,'1435939713,1234567890')
put_shfile(transport,PG_CMD_LPATH+PG_CMD_EXESH,PG_CMD_EXESH)
targzipsplit_servfiles(transport,'1435939713,1234567890')

tmpmalst=[{'uid': '2345678901', 'from': 'xlzhu@ips.com', 'to': 'xlzhu@ips.com', 'cc': '', 'subject': 'test1', 'flag': 'S', 'ftime': 1542638495.3562727, 'dbusr': '', 'dbtns': 'IBT', 'msg': ''}, \
{'uid': '0123456789', 'from': 'xlzhu@ips.com', 'to': 'xlzhu@ips.com', 'cc': '', 'subject': 'test2', 'flag': 'S', 'ftime': 1542638495.3562727, 'dbusr': '', 'dbtns': 'IBT', 'msg': ''}, \
{'uid': '1234567890', 'from': 'xlzhu@ips.com', 'to': 'xlzhu@ips.com', 'cc': '', 'subject': 'test3', 'flag': 'S', 'ftime': 1542638495.3562727, 'dbusr': '', 'dbtns': 'IBT', 'msg': ''}]
tmppalst=[]
refresh_ini()
init_dirs()
init_log('-v')
#2.refresh parameters
refresh_paras(tmppalst)
tmpmutex=threading.Lock()
sets2fileok(public.gmutex_s2file,True)
get_main(tmpmalst,tmppalst,tmpmutex)

##########
transport = paramiko.Transport(('ip', int('22')))
transport.connect(username='t', password='')
tmpfilelst=[]
getdel_servfiles(transport,'/home/x/','e:\\test\\willsend\\',tmpfilelst)
print(tmpfilelst)

#test maillst update&files process&upload 20181029
lmailslst=[]
#del lmailslst[:]
lmailslst.append({'uid':'1435935729','from': 'xi9@163.com', 'to': '"xlzhu@ips.com"', 'cc': '<73@qq.com>', 'subject': 'A中国风-中國風E0', 'flag': 'D', 'user': 'bnk2b', 'db': 'db67','msg':''})
lmailslst.append({'uid':'1435935683','from': 'xi3@163.com', 'to': '78108673@qq.com', 'cc': '"hu@ok.com"', 'subject': 'A2中国风B1', 'flag': 'D', 'user': 'u67', 'db': 'db67','msg':''})
lmailslst.append({'uid':'1435935694','from': 'xi4@163.com', 'to': '"xlzhu@ips.com"', 'cc': '<he.zhu@ook.com>', 'subject': '民族的国际的', 'flag': 'D', 'user': '', 'db': '','msg':''})
lmailslst.append({'uid':'41','from': '41@163.com', 'to': '"41@ips.com"', 'cc': '"41@ou.com"', 'subject': '41', 'flag': 'D', 'user': 'sys', 'db':'db68','msg':''})
lmailslst.append({'uid':'42','from': '42@163.com', 'to': '"42@ips.com"', 'cc': '"42@o.com"', 'subject': '42', 'flag': 'D', 'user': 'sys', 'db':'db68','msg':''})
lmailslst.append({'uid':'414','from': '414@163.com', 'to': '"42@ips.com"', 'cc': '"42@uk.com"', 'subject': '414', 'flag': 'D', 'user': '', 'db':'','msg':''})

lparalst=[]
#del pparalst[:]
lparalst.append(13245435.99)
lparalst.append({'dbdbusr':'db67bnk2b','dbpd':'xxx1','osusr':'osuser','ospd':'xxxx','osip':'ip','osprt':'22'})
lmutex = threading.Lock()
put_main(lmailslst,lparalst,lmutex)
print(lmailslst)
print('--update others')
updatedict_inlist(lmailslst,lmutex,pg_dictname_flag,public.GSTATUS_ATACHMT_DOWNLOADED,pg_dictname_msg,'',pg_msg1)
updatedict_inlist(lmailslst,lmutex,pg_dictname_flag,public.GSTATUS_ATACHMT_DOWNLOADED,pg_dictname_flag,'',public.GSTATUS_ERROR)
print(lmailslst)
'''
'''	
#test sftp put
transport = paramiko.Transport((serverip, serverprt))
transport.connect(username=loginusr, password=loginpwd)
sftp = paramiko.SFTPClient.from_transport(transport)

sftp.put('e:\\test\\query.sql', '/home/test/query.sql')

transport.close()
'''