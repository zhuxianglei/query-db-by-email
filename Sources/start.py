#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
	Version  Created by     Creation Time  Description
	-------  -------------  -------------  ----------------------------------------------
	1.0      xlzhu@ips.com  2018-10-29     starting multiple threads process self's work.
										   first integration Testing at 2018-11-19 16:40,pass
										   second integration Testing at 2018-11-21 13:13,failed,
										   BUG:related to MultiThread call s2file.execsfile&execprocedure
										   three integration Testing at 2018-11-27 20:00,pass,BUG solved
											prerunning(first) preparation:
											==============================
											1.config a email account&run&dir info in start.ini
											2.Create a os account,own self's homedir,
											  shell privilege(ssh&sftp;find&cut&sort&sed&rm&wc&grep&echo&tar&gzip&split) 
											3.Create a database account,own select any table privilege,
											  own execute privilege on exportcsv_forbigdata&exportcsv_byviews
											  (maybe you need to create db procedure:exportcsv_forbigdata&exportcsv_byviews;grant exec to query account)
											  Create a db directory which path as os account homedir,grant read,write on this directory to db account
											4.config db&os account&tns&connection info in paras.csv which in this script dir
														
											mailformat:
											==============================
											1.mail body must include keyword:database:dbtnsdescription;for example:  database:mydborcl
											2.all query must be send as attachment;attachment body format:sql;sql;sql;
											  forbid appearing risk keywork(truncate,drop,delete...) in sql

	Version  Updated by     Update Time    Description
	-------  -------------  -------------  ----------------------------------------------
	
'''
import sys
import time
import threading
import public
from mailsa import maila_main
from s2file import s2file_main
from filepg import get_main
from mailss import mails_main
from common import helps,logaq,init_log,init_dirs,refresh_ini,refresh_paras,isrunning
from common import breaktime,check_processthread,gettnsset_indir,exist_tnsthread
#!!!!!!!!!!!!!!!!!!!!!!in prod please remark refresh_ini
#parameters
gmailslst=[]
gparaslst=[]
gthreadlst_bn=[]
gprocesslst_bn=[]
gdbconlst_bn=[]
gmutex = threading.Lock()
gmutexprocess = threading.Lock()
lpypara=''

def create_s2fthread(pmaillst,pparalst,pmutex,pthreadlst,pproclst,pmutexp,pdbconlst):
	#this is a bottleneck,so must create 3 thread at least
	#script execute must be stop in GMAXSECS_RUN seconds
	#del pthreadlst[:]#copy:tmplst=lst[:],thread can not use deepcopy
	while not breaktime():
		try:
			check_processthread(pproclst,pthreadlst,pmutexp,pdbconlst)
			if len(pthreadlst)<public.gmaxcnt_bnthread:
				setdirtns=gettnsset_indir(public.gdir_atachmt_downloaded,public.GPREFIX_FILEUID)
				for st in setdirtns:
					if not exist_tnsthread(pthreadlst,st):
						logaq('create s2fthread:'+public.GPREFIX_BNTHREAD+st,'i')
						t_bn= threading.Thread(target=s2file_main,args=(pmaillst,pparalst,pmutex,st,pproclst,pmutexp,pdbconlst))
						t_bn.setName(public.GPREFIX_BNTHREAD+st)
						t_bn.setDaemon(False)
						t_bn.start()
						pthreadlst.append({'thread':t_bn,'ctime':time.time()})
						if len(pthreadlst)>=public.gmaxcnt_bnthread:
							break
			logaq('create_s2fthread sleep '+str(public.gsleep)+'s,refreshini...')
			time.sleep(public.gsleep)			
		except Exception as e:
			time.sleep(3)
			logaq('start while Error: %s' % e,'e')
		finally:
			pass
			#refresh_ini()#call external program,maybe will produce error which casing sys exit
			
	if breaktime():
		check_processthread(pproclst,pthreadlst,pmutexp,pdbconlst,True)
			
  
#**********************************************************************************
#----------------------------------------------------------------------------------
#--  start.py entry(Don't running this scripts,Please run start.cmd,Thanks!)
#----------------------------------------------------------------------------------
#**********************************************************************************
if len(sys.argv)>1:
	lpypara=sys.argv[1]
if helps(lpypara):
	exit()
try:
	#1.init; get ini config parameter,init dirs,log,set loglevel by arg passed
	refresh_ini()
	init_dirs()
	init_log(lpypara)
	#2.refresh parameters of db
	refresh_paras(gparaslst)
	#3.prevent script runing again
	if isrunning():
		exit()
		
	#4.create thread:mails analyzing&downloading
	logaq('--starting--','i')
	logaq('create thread Thread_mailsa','i')
	t_mailsa= threading.Thread(target=maila_main,args=(gmailslst,gparaslst,gmutex))
	t_mailsa.setName('Thread_mailsa1')
	t_mailsa.setDaemon(False)
	t_mailsa.start()
	
	#5.create thread:exportfiles tar&zip&put/get	
	logaq('create thread Thread_filepg')
	t_filepg= threading.Thread(target=get_main,args=(gmailslst,gparaslst,gmutex))
	t_filepg.setName('Thread_filepg1')
	t_filepg.setDaemon(False)
	t_filepg.start()
	
	#6.create thread:mails sending
	logaq('create thread Thread_mailss')
	t_mailss= threading.Thread(target=mails_main,args=(gmailslst,gmutex,public.gdir_atachmt_willsend))
	t_mailss.setName('Thread_mailss1')
	t_mailss.setDaemon(False)
	t_mailss.start()

	#7.create N thread of s2file which export data from database will spend a long time(bottleneck) 
	create_s2fthread(gmailslst,gparaslst,gmutex,gthreadlst_bn,gprocesslst_bn,gmutexprocess,gdbconlst_bn)

except Exception as e:
	logaq('start Error: %s' % e,'e')
finally:
	logaq('maillist infor:')
	logaq(gmailslst)
	logaq('paralist infor:')
	logaq(gparaslst)
	logaq('s2fprocesslist infor:')
	logaq(gprocesslst_bn)
	logaq('s2fdbconlist infor:')
	logaq(gdbconlst_bn)
	logaq('s2fhreadlist infor:')
	logaq(gthreadlst_bn)
	logaq('--startend--','i')
	if breaktime():
		logaq('this script exit by setting of stoptime parameter','i')	


    
