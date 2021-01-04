#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
	Version  Created by     Creation Time  Description
	-------  -------------  -------------  ----------------------------------------------
	1.0      xlzhu@ips.com  2018-10-25     common base tool
	
	Version  Updated by     Update Time    Description
	-------  -------------  -------------  ----------------------------------------------

'''
import os
import sys
import time
import csv
import pickle
import base64
import threading
import logging
import public
import chardet
import copy
import socket
import inspect
import ctypes
import configparser
from logging.handlers import TimedRotatingFileHandler
#attention!!!test ospassword no encode,please unremark

#contants
CN_PARAPATH=os.path.split(os.path.realpath(__file__))[0]+'\\'
CN_PARAFILE='paras'
CN_INI_FILE='start'
CN_INI_SRUN='run'
CN_INI_SMAIL='mail'
CN_INI_SIMAP='imap'
CN_INI_SSMTP='smtp'
CN_INI_OSTOPTIME='stoptime'
CN_INI_OMAXTHREAD='maxthread'
CN_INI_OSLEEP='sleep'
CN_INI_OUSR='user'
CN_INI_OPD='pwd'
CN_INI_OENPD='enpwd'
CN_INI_OCYCLE='cycle'
CN_INI_OFROMALLOW='fromallow'
CN_INI_OMAXTIME='maxqtime'
CN_INI_OWKDIR='workdir'
CN_INI_OPRT='port'
CN_INI_OHST='host'
CN_INI_SDIR='dir'
CN_INI_ODOWNLOADED='downloaded'
CN_INI_OWILLSEND='willsend'
CN_INI_OUPLOADED='uploaded'
CN_INI_OHIS='history'
CN_INI_OLOG='log'

chset='utf-8'

#get file encode type,accuracy rate 90%,you may be use chardet 99%
def getfileencode(file_path: str):
	with open(file_path, 'rb') as f:
		return getstringencode(f.read())
 
#get char encode type
def getstringencode(data: bytes):
	for code in public.GCODES:
		try:
			data.decode(encoding=code)
			if 'UTF-8' == code and data.startswith(public.GUTF8_BOM):
				return 'UTF-8-SIG'
			return code
		except UnicodeDecodeError:
			continue
	return 'unknown'
	
def clearblankchar(str):
	tmpstr=clearctrlchar(str)
	return tmpstr.replace(' ','')
	
#clear control char:tab,return,newline	
def clearctrlchar(str):
	tempstr=str.replace('\r','')
	tempstr=tempstr.replace('\n','')
	return tempstr.replace('\t','')
	
#serializing object	
def saveobject(obj,objname):
	if len(obj)>0:
		fd = open(CN_PARAPATH+objname+'.bin','wb')
		try:
			fd.write(pickle.dumps(obj))
		finally:
			fd.close() 
			
#unserializing object 			
def init_object(objname):
	try:
		fd = open(CN_PARAPATH+objname+'.bin','rb')
		try:
			obj = pickle.loads(fd.read())
		finally:
			fd.close()
		return obj
	except Exception as e:
		logaq('openfile or pickle.loads error: %s' % e,'e')			
		return []

#init dir
def init_dirs():
	if not os.path.exists(public.gdir_atachmt_downloaded):
		os.makedirs(public.gdir_atachmt_downloaded,755)
	if not os.path.exists(public.gdir_atachmt_uploaded):
		os.makedirs(public.gdir_atachmt_uploaded,755)
	if not os.path.exists(public.gdir_atachmt_willsend):
		os.makedirs(public.gdir_atachmt_willsend,755)
	if not os.path.exists(public.gdir_history):
		os.makedirs(public.gdir_history,755)
	if not os.path.exists(public.gdir_log):
		os.makedirs(public.gdir_log,755)
		
#get key word value string,for example: KWORD:KWORDVALUE	
def getkeyword(str,kword):
	idx=str.lower().find(kword.lower())
	if idx<0:
		return ''
	idxe=str.lower().find('\n',idx)
	if idxe<0:
		idxe=len(str)
	idxm=str.lower().find(';',idx,idxe)
	if idxm<0:
		idxm=str.lower().find('；',idx,idxe)
	if idxm<0:
		idxm=str.lower().find(',',idx,idxe)
	if idxm<0:
		idxm=str.lower().find('，',idx,idxe)
	if idxm<0:
		idxm=str.lower().find('.',idx,idxe)	
	if idxm<0:
		idxm=str.lower().find('。',idx,idxe)	
	if idxm>-1:
		tempkw=str[idx+len(kword):idxm]
	else:
		tempkw=str[idx+len(kword):idxe]
	tempkw=tempkw.replace(':','')
	tempkw=tempkw.replace('：','')
	return clearctrlchar(tempkw.strip())
	
#create parameter key item dict
#dbtnsdesc:used for user's mail database keywork
def newparameta():
	parameta = {'dbtns':'','dbusr':'','dbpd':'','osusr':'','ospd':'','osip':'','osprt':'','dbprt':'','dbservnam':'','dbtnsdesc':''}
	return parameta

def getpdbyen(penpd):
	return base64.decodestring(penpd.encode(chset)).decode(chset)

def formatpydir(pdir):
	rst=pdir
	if rst[len(rst)-1:len(rst)]!='\\':
		rst=rst+'\\'
	if rst.find('\\\\')<0:
		rst=rst.replace('\\','\\\\')
	return rst

		
def refresh_ini():	
	try:
		lpath=CN_PARAPATH+CN_INI_FILE+'.ini'
		config=configparser.ConfigParser()
		config.read(lpath)
		public.gstop_time = config.get(CN_INI_SRUN, CN_INI_OSTOPTIME) 
		try:
			public.gsingleinstance_prt=int(config.get(CN_INI_SRUN, CN_INI_OPRT))
		except Exception as e:
			public.gsingleinstance_prt=64444
		try:
			public.gmaxcnt_bnthread=int(config.get(CN_INI_SRUN, CN_INI_OMAXTHREAD))
		except Exception as e:
			public.gmaxcnt_bnthread=3
		try:
			public.gsleep=int(config.get(CN_INI_SRUN, CN_INI_OSLEEP))
		except Exception as e:
			public.gsleep=10
		public.gmusr = config.get(CN_INI_SMAIL, CN_INI_OUSR) 
		mpd = config.get(CN_INI_SMAIL, CN_INI_OPD) 
		public.gmenpd = config.get(CN_INI_SMAIL, CN_INI_OENPD)
		try:
			public.gmcycle = int(config.get(CN_INI_SMAIL, CN_INI_OCYCLE))
		except Exception as e:
			public.gmcycle =600
		public.gmfromallow= config.get(CN_INI_SMAIL, CN_INI_OFROMALLOW)
		try:
			public.gmmaxqtime= int(config.get(CN_INI_SMAIL, CN_INI_OMAXTIME))
		except Exception as e:
			public.gmmaxqtime=public.GMAXSECS_RUN
		public.giwkdir = config.get(CN_INI_SIMAP, CN_INI_OWKDIR) 
		public.giprt = config.get(CN_INI_SIMAP, CN_INI_OPRT) 
		public.gihst = config.get(CN_INI_SIMAP, CN_INI_OHST) 
		public.gsprt = config.get(CN_INI_SSMTP, CN_INI_OPRT) 
		public.gshst = config.get(CN_INI_SSMTP, CN_INI_OHST)
		public.gdir_atachmt_downloaded = formatpydir(config.get(CN_INI_SDIR, CN_INI_ODOWNLOADED)) 
		public.gdir_atachmt_uploaded = formatpydir(config.get(CN_INI_SDIR, CN_INI_OUPLOADED))
		public.gdir_atachmt_willsend = formatpydir(config.get(CN_INI_SDIR, CN_INI_OWILLSEND)) 
		public.gdir_history = formatpydir(config.get(CN_INI_SDIR, CN_INI_OHIS)) 
		public.gdir_log = formatpydir(config.get(CN_INI_SDIR, CN_INI_OLOG))
		if mpd.strip():
			config.set(CN_INI_SMAIL, CN_INI_OPD, "")
			public.gmenpd=(base64.encodestring(mpd.strip().encode(chset))).decode(chset)
			config.set(CN_INI_SMAIL, CN_INI_OENPD, public.gmenpd)
			config.write(open(lpath, "w")) 
	except Exception as e:
		print('refresh ini exception:%s' % e,'e')
		
def initparacsv_serializing(pparalst,pmtime):
	#please attention!!only run one time after pd change!
	logaq('begin read parafile:'+CN_PARAPATH+CN_PARAFILE+'.csv','i')
	try:
		fd=open(CN_PARAPATH+CN_PARAFILE+'.csv')
		csv_reader = csv.reader(fd)
		pparalst.append(pmtime)
		for row in csv_reader:
			pparalst.append(newparameta())
			pparalst[len(pparalst)-1]['dbtns']=row[0]
			pparalst[len(pparalst)-1]['dbusr']=row[1]
			pparalst[len(pparalst)-1]['dbpd']=base64.encodestring(row[2].encode(chset))
			pparalst[len(pparalst)-1]['osusr']=row[3]
			pparalst[len(pparalst)-1]['ospd']=base64.encodestring(row[4].encode(chset))
			pparalst[len(pparalst)-1]['osip']=row[5]
			pparalst[len(pparalst)-1]['osprt']=row[6]
			pparalst[len(pparalst)-1]['dbprt']=row[7]
			pparalst[len(pparalst)-1]['dbservnam']=row[8]
			pparalst[len(pparalst)-1]['dbtnsdesc']=row[9]
		saveobject(pparalst,CN_PARAFILE)
		logaq('serializing data:')
		logaq(pparalst)
		logaq('serializing parameters file ok!')
	finally:
		fd.close()
	os.remove(CN_PARAPATH+CN_PARAFILE+'.csv')
	logaq('delete parafile:'+CN_PARAPATH+CN_PARAFILE+'.csv ok!','i')

def refresh_paras(pparalst):
	try:
		del pparalst[:]
		tmplst=init_object(CN_PARAFILE)
		for lst in tmplst:
			pparalst.append(lst)
		mtime=os.path.getmtime(CN_PARAPATH+CN_PARAFILE+'.csv')
		if len(pparalst)>0:
			if mtime!=pparalst[0]:
				logaq('parafile mtime changed,begin refreshing:'+CN_PARAPATH+CN_PARAFILE+'.csv')
				#pparalst=[]
				del pparalst[:]
				initparacsv_serializing(pparalst,mtime)
			else:
				logaq('mtime is same,donnot refresh:'+CN_PARAPATH+CN_PARAFILE+'.csv')
				os.remove(CN_PARAPATH+CN_PARAFILE+'.csv')
				logaq('delete parafile:'+CN_PARAPATH+CN_PARAFILE+'.csv ok!')
		else:
			initparacsv_serializing(pparalst,mtime)
	except Exception as e:
		logaq('refresh exception:parafile donot exist:'+CN_PARAPATH+CN_PARAFILE+'.csv','e')

def getserialnumber(pstr):
	idxfirst=-1
	idxend=-1
	for i in range(0,len(pstr),1):
		if pstr[i].isdecimal():
			if idxfirst<0:
				idxfirst=i
		else:
			if idxfirst>-1 and idxend<0:
				idxend=i
	if idxend<0:
		idxend=len(pstr)
	return pstr[idxfirst:idxend]

def get2betw13(pstr,pfirst,pthird):
	if not pfirst:
		idxfirst=0
	else:
		idxfirst=pstr.lower().find(pfirst.lower())+len(pfirst)	
	if not pthird:
		idxthird=len(pstr)
	else:
		idxthird=pstr.lower().find(pthird.lower(),idxfirst+len(pfirst))
	if idxfirst<len(pfirst) or idxthird<0:
		return ''
	else:
		return pstr[idxfirst:idxthird]
		
def getoinfor(pdbtns,pdbusr,pparalst):
	for idx in range(1,len(pparalst),1):
		if pparalst[idx]['dbtns']==pdbtns and pparalst[idx]['dbusr']==pdbusr:
			return pparalst[idx]['osip'],pparalst[idx]['osprt'],pparalst[idx]['osusr'],base64.decodestring(pparalst[idx]['ospd']).decode(chset)#base64.decodestring(pparalst[idx]['ospd']).decode(chset)

def getoinforbytnsusr(pdbtnsusr,pparalst):
	for idx in range(1,len(pparalst),1):
		if pparalst[idx]['dbtns']+pparalst[idx]['dbusr']==pdbtnsusr:
			return pparalst[idx]['osip'],pparalst[idx]['osprt'],pparalst[idx]['osusr'],base64.decodestring(pparalst[idx]['ospd']).decode(chset)#base64.decodestring(pparalst[idx]['ospd']).decode(chset)

def gettnsset_inparalst(pparalst):
	tnsset=set()
	for idx in range(1,len(pparalst),1):
		#if pparalst[idx]['pri']=='0':
		tnsset.add(pparalst[idx]['dbtns'])
	return tnsset
			
def getsqlpluscon(pdbtns,pdbusr,pparalst):
	#if pdusr is null,only check dbtns,then using default user
	rst=''
	for idx in range(1,len(pparalst),1):
		if pparalst[idx]['dbtns']==pdbtns:
			if not pdbusr or (pdbusr and pparalst[idx]['dbusr']==pdbusr):
				rst=pparalst[idx]['dbusr']+'/'+base64.decodestring(pparalst[idx]['dbpd']).decode(chset)+'@'+pparalst[idx]['dbtns']#base64.decodestring(pparalst[idx]['ospd']).decode(chset)
				break
	return rst

def getcxoracon(pdbtns,pdbusr,pparalst):
	#pusr+'/'+ppd+'@'+'//'+pdbip+':'+pdbprt+'/'+psid
	rst=''
	for idx in range(1,len(pparalst),1):
		if pparalst[idx]['dbtns']==pdbtns:
			if not pdbusr or (pdbusr and pparalst[idx]['dbusr']==pdbusr):
				rst=pparalst[idx]['dbusr']+'/'+base64.decodestring(pparalst[idx]['dbpd']).decode(chset)+'@//'+pparalst[idx]['osip']+ \
					':'+pparalst[idx]['dbprt']+'/'+pparalst[idx]['dbservnam']#base64.decodestring(pparalst[idx]['ospd']).decode(chset)
				break
	return rst
	
def getdpd(pdbtns,pdbusr,pparalst):
	for idx in range(1,len(pparalst),1):
		if pparalst[idx]['dbtns']==pdbtns and pparalst[idx]['dbusr']==pdbusr:
			return base64.decodestring(pparalst[idx]['dbpd']).decode(chset)

def getdbtnsdbusr(pmuid,pflag,pmaillst):
	rst=''
	for idx in range(0,len(pmaillst),1):
		if pmaillst[idx]['uid']==pmuid and pmaillst[idx]['flag']==pflag:
			rst=pmaillst[idx]['dbtns']+pmaillst[idx]['dbusr']
			break
	return rst	
	
def getdbtnsbymaildb(pmaildb,pparalst,pidx):
	#return dbtnsname by databasename in mail
	#attention!!!idx 0 of paralst is mtime,so begin from 1
	#add pidx parameter for loadbalance
	rst=''
	ldbcnt=0
	for idx in range(1,len(pparalst),1):
		if pparalst[idx]['dbtnsdesc'].lower().find(pmaildb.lower())>-1:
			ldbcnt=ldbcnt+1
	if ldbcnt==0:
		return rst
	else:
		mval=(pidx%ldbcnt)+1
	ldbcnt=0
	for idx in range(1,len(pparalst),1):
		if pparalst[idx]['dbtnsdesc'].lower().find(pmaildb.lower())>-1:
			ldbcnt=ldbcnt+1
			if mval==ldbcnt:
				rst=pparalst[idx]['dbtns']
				break
	return rst
	
def getdbusrbydbtns(pdbtns,pparalst):
	#return dbuser by dbtns
	#
	rst=''
	for idx in range(1,len(pparalst),1):
		if pparalst[idx]['dbtns']==pdbtns:
			rst=pparalst[idx]['dbusr']
			break
	return rst
	
def getidxbyuidflag(pmuid,pflag,pmaillst):
	#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	#Attention:this function not safe for multithread
	#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	lidx=-1
	for idx in range(0,len(pmaillst),1):
		if pmaillst[idx]['uid']==pmuid and pmaillst[idx]['flag']==pflag:
			lidx=idx
			break
	return lidx
	
def getflagbyuid(pmuid,pmaillst):
	rst=''
	for idx in range(0,len(pmaillst),1):
		if pmaillst[idx]['uid']==pmuid:
			rst=pmaillst[idx]['flag']
			break
	return rst

def getdbtnsbyuid(pmuid,pmaillst):
	rst=''
	for idx in range(0,len(pmaillst),1):
		if pmaillst[idx]['uid']==pmuid:
			rst=pmaillst[idx]['dbtns']
			break
	return rst

def getdbusrbyuid(pmuid,pmaillst):
	rst=''
	for idx in range(0,len(pmaillst),1):
		if pmaillst[idx]['uid']==pmuid:
			rst=pmaillst[idx]['dbusr']
			break
	return rst
	
def check_fileformat(pstr):
	rst=False
	tmpstr=pstr
	idx1=pstr.lower().find('select')
	idx2=pstr.lower().find('from',idx1)
	if idx1>-1 and idx2>idx1+len('select')+2:
		rst=True
	return rst
	
def replace_oldkeyword(ptext,pold,pnew,psegseptor,pnewautoadd=0,pnewvar=''):
	#replace old keyword by new keyword once at begin of text segment
	#pnewautoadd>0,pnew=pnew.replace(pnew,pnewvar+pnewautoadd++)
	returnnl='\r\n'
	idxend_presegment=0
	idxend_cursegment=len(ptext)
	seqnew=pnewautoadd
	otherstext=ptext
	temptext=returnnl
	if otherstext.lower().find(pold.lower())>-1 and otherstext.find(psegseptor)<0:
		otherstext=otherstext+psegseptor
	while idxend_cursegment>0:
		idxend_cursegment=otherstext.find(psegseptor)
		#print(idxend_cursegment)
		cursegment=otherstext[idxend_presegment:idxend_cursegment+1].strip()
		#print(cursegment)
		otherstext=otherstext[idxend_cursegment+1:len(otherstext)+1].strip()
		#print(otherstext)
		#process,for example:
		#                                   select a.c,a.b from a where a.d in(select b.a from b where b.b>sysdate-1);  
		#create or replace view UID123456_file1_1V as select a.c,a.b from a where a.d in(select b.a from b where b.b>sysdate-1);
		idx_oldkword=cursegment.lower().find(pold.lower())
		#print(idx_oldkword)
		if idx_oldkword<0:
			idxbegincurseg=0
		else:
			idxbegincurseg=idx_oldkword+len(pold)
		if seqnew>0:
			temptext=temptext+pnew.replace(pnewvar,pnewvar+str(seqnew)+'V')+cursegment[idxbegincurseg:len(cursegment)+1]+returnnl
			seqnew=seqnew+1
		else:
			temptext=temptext+pnew+cursegment[idxbegincurseg:len(cursegment)+1]+returnnl
			
		#print(temptext)
		idxend_cursegment=otherstext.find(psegseptor)
		if idxend_cursegment<0 and otherstext.lower().find(pold.lower())>-1:
			otherstext=otherstext+psegseptor
			idxend_cursegment=otherstext.find(psegseptor)
		#print(idxend_cursegment)
	return temptext

#add dictionary into maillist			
def addmaillist(plst,pmutex,puid,pfrom,pto,pcc,psub,pflag,pftime,pdbusr,pdbtns,pmsg):
	if puid and pmutex.acquire(1):
		plst.append({'uid':puid,'from':pfrom, 'to':pto,'cc':pcc,'subject':psub,'flag':pflag,'ftime':pftime,'dbusr':pdbusr,'dbtns':pdbtns,'msg':pmsg})
		pmutex.release()
		
#delete all match dictionary from list			
def deldict_fromlist(plst,pmutex,pdictname,pdictval):
	if pmutex.acquire(1):
		idxd=0
		while idxd<len(plst):
			if plst[idxd][pdictname]==pdictval:
				plst.remove(plst[idxd])
				idxd=idxd-1
			idxd=idxd+1
		pmutex.release()
			
#update all match dictionary in list
def updatedict_inlist(plst,pmutex,pdict1n,pdict1val,pdict2n,pdict2oldval,pdict2newval):
	if pmutex.acquire(1):
		for idxd in range(0,len(plst),1):
			if plst[idxd][pdict1n]==pdict1val:
				if not pdict2oldval or(pdict2oldval and plst[idxd][pdict2n]==pdict2oldval):
					plst[idxd][pdict2n]=pdict2newval		
		pmutex.release()

def updateflagmsg_byuidflag(plst,pmutex,puid,pflagold,pflagnew,pmsg,ptime=''):
	if pmutex.acquire(1):
		for idxd in range(0,len(plst),1):
			if plst[idxd]['uid']==puid and plst[idxd]['flag']==pflagold:
				plst[idxd]['flag']=pflagnew	
				if pmsg:
					plst[idxd]['msg']=pmsg
				if ptime:
					plst[idxd]['ftime']=ptime
		pmutex.release()

def helps(parg):
	if not parg:
		print('**********************************************************')
		print('this script run method:python path/scriptname.py parameter')
		print('parameter:')
		print('-h/-help/help:help')
		print('-v:set log level to debug;default:info')
		print('**********************************************************')
		return ''
	if parg.lower()=='-h' or parg.lower()=='-help' or parg.lower()=='help':
		print('**************************************')
		print('-h/-help/help:help')
		print('-v:set log level to debug;default:info')
		print('**************************************')
		return 'Y'
	if parg.lower()=='-v':
		print('**************************************')
		print('-v:set log level to debug;default:info')
		print('**************************************')
		return ''

def init_log(parg):
	public.glogger = logging.getLogger('AQBOEM@IPS')
	formatter = logging.Formatter('%(asctime)s %(threadName)s %(levelname)s  %(message)s')
	if not public.glogger.handlers:
		#file_handler = logging.FileHandler(public.gdir_log+"run.log")
		#logrotatingset
		file_handler = TimedRotatingFileHandler(public.gdir_log+"run.log", 'midnight', 1, 3)
		file_handler.suffix='%Y-%m-%d'
		file_handler.encoding='utf-8'
		#logrotatingsetend
		file_handler.setFormatter(formatter)
		console_handler = logging.StreamHandler(sys.stdout)
		console_handler.formatter = formatter
		console_handler.encoding='utf-8'
		public.glogger.addHandler(file_handler)
		public.glogger.addHandler(console_handler)
	if parg.lower()=='-v':
		public.glogger.setLevel(logging.DEBUG)
	else:
		public.glogger.setLevel(logging.INFO)
	return public.glogger,file_handler
		
def logaq(pmsg,plevel='d'):	
	if plevel=='d':
		public.glogger.debug(pmsg)
	if plevel=='i':
		public.glogger.info(pmsg)
	if plevel=='w':
		public.glogger.warning(pmsg)
	if plevel=='e':
		public.glogger.error(pmsg)
	if plevel=='c':
		public.glogger.critical(pmsg)
		
def remove_riskfactors(pstr):
	tempstr=pstr
	temperr=''
	for idxfactor in range(0,len(public.GRISK_FACTORS1),1):
		if tempstr.lower().find(public.GRISK_FACTORS1[idxfactor])>-1:
			tempstrafter=tempstr[tempstr.lower().find(public.GRISK_FACTORS1[idxfactor])+len(public.GRISK_FACTORS1[idxfactor]):len(tempstr)]
			if tempstrafter.lower().find(public.GRISK_FACTORS2[idxfactor])>0:
				tempstrafter=clearctrlchar(tempstrafter.strip())
				if tempstrafter.lower().find(public.GRISK_FACTORS2[idxfactor])==0:
					temperr=temperr+' '+public.GRISK_FACTORS1[idxfactor]+' '+public.GRISK_FACTORS2[idxfactor]
					tempstr=tempstr[0:tempstr.lower().find(public.GRISK_FACTORS1[idxfactor])]+tempstr[tempstr.lower().find(public.GRISK_FACTORS2[idxfactor],tempstr.lower().find(public.GRISK_FACTORS1[idxfactor]))+len(public.GRISK_FACTORS2[idxfactor]):len(tempstr)]
	return temperr,tempstr

def writefile(ppathfile,pmsg,psuffix):
	#for example:file.sql/file.sq/file.s to file.err;file.sqls to file.sqls.err
	lpathfile='';
	if ppathfile.find('.',len(ppathfile)-4)>0:
		lpathfile=ppathfile[0:ppathfile.find('.',len(ppathfile)-4)]+psuffix
	else:
		lpathfile=ppathfile+psuffix
	fd = open(lpathfile, 'wb')
	try:
		fd.write(pmsg.encode(chset))#save attachment
	finally:
		fd.close()

def replaceline_infile(ppath,pftemplate,pshfile,poldstr,pnewstr):
	tmpfd=open(ppath+pftemplate, 'rb')
	try:
		tmpdata = tmpfd.read()
	finally:
		tmpfd.close()
	tmpdata=tmpdata.decode().replace(poldstr,pnewstr)
	tmpdata=tmpdata.encode()
	tmpfd = open(ppath+pshfile, 'wb')
	try:
		tmpfd.write(tmpdata)
	finally:
		tmpfd.close()
		
def gettnsset_indir(ppath,ptnsseptor):
	tnsset=set()
	fileslst = os.listdir(ppath)
	fileslst.sort()
	curmuid=-1
	ltnsname='';
	for i in range(0,len(fileslst),1):
		ltnsname=get2betw13(fileslst[i],'',ptnsseptor).upper()
		if ltnsname:
			tnsset.add(ltnsname)
	return tnsset
	
def gettnsset_inmailslst(pmaillst,pflag):
	tnsset=set()
	for idx in range(0,len(pmaillst),1):
		if pmaillst[idx]['flag']==pflag:
			tnsset.add(pmaillst[idx]['dbtns'])
	return tnsset

def existflag_inmailslst(pmaillst,pflag):
	rst=False
	for idx in range(0,len(pmaillst),1):
		if pmaillst[idx]['flag']==pflag:
			rst=True
			break
	return rst
	
def getlastone_byseptor(pstr,pseptor):
	laststr=''
	for mt in pstr.split(pseptor):
		laststr=mt
	return laststr
	
def getuidstrlst_inmailslst(pmaillst,pdbtns,pflag,plastuidlst):
	uidstr=''
	cnt=0
	lastuid=getlastone_byseptor(plastuidlst,',')
	lastidx=-1
	for idx in range(0,len(pmaillst),1):
		if pmaillst[idx]['dbtns']==pdbtns and pmaillst[idx]['flag']==pflag:
			if not lastuid and not uidstr:
				uidstr=pmaillst[idx]['uid']
				lastidx=idx
			elif lastuid==pmaillst[idx]['uid']:
				lastidx=idx
				
			if idx>lastidx and lastidx>=0:
				if not uidstr:
					uidstr=pmaillst[idx]['uid']
				else:
					uidstr=uidstr+','+pmaillst[idx]['uid']
				cnt=cnt+1
			if cnt>9:
				break			
	return uidstr	
	
def getidxkeywordinlst(plst,pkeyword):
	rst=-1;
	for i in range(0,len(plst),1):
		if plst[i].find(pkeyword)>-1:
			rst=i
			break
	return rst
	
def breaktime():
	curh=time.strftime("%H",time.localtime(time.time()))
	rst=False
	try:
		for hh in public.gstop_time.split(','):
			if int(curh)==int(hh):			
				rst=True
				break
	except Exception as e:
		rst=True
		logaq('breaktime Error: %s' % e,'e')
	return rst

		
def setmailsaok(pmutex,pok):
	if pmutex.acquire(1):
		public.gmailsaok=pok
		pmutex.release()
		
def getmailsaok(pmutex):
	if pmutex.acquire(1):
		pmutex.release()
		return public.gmailsaok
	else:
		return False
		
def sets2fileok(pmutex,pok):
	if pmutex.acquire(1):
		public.gs2fileok=pok
		pmutex.release()
		
def gets2fileok(pmutex):
	if pmutex.acquire(1):
		pmutex.release()
		return public.gs2fileok
	else:
		return False

def setfilepgok(pmutex,pok):
	if pmutex.acquire(1):
		public.gfilepgok=pok
		pmutex.release()
		
def getfilepgok(pmutex):
	if pmutex.acquire(1):
		pmutex.release()
		return public.gfilepgok
	else:
		return False
		
def isrunning():
	try: 
		public.gsklock = socket.socket()
		addr = ('',public.gsingleinstance_prt)
		public.gsklock.bind(addr);
		rst=False	
	except Exception as e:
		#print('socket error: %s' % e)
		logaq('Warning:This pythonscript is running!Dont start again!','w')
		rst=True
	return rst
	
def _async_raise(tid, exctype):
   """raises the exception, performs cleanup if needed"""
   tid = ctypes.c_long(tid)
   if not inspect.isclass(exctype):
      exctype = type(exctype)
   res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
   if res == 0:
      raise ValueError("invalid thread id")
   elif res != 1:
      # """if it returns a number greater than one, you're in trouble,  
      # and you should call it again with exc=NULL to revert the effect"""  
      ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
      raise SystemError("PyThreadState_SetAsyncExc failed")

def stop_thread(thread):
   _async_raise(thread.ident, SystemExit)

def add_process(pproclst,pproc,pthreadname,pmutex):
	if pmutex.acquire(1):	
		pproclst.append({'process':pproc,'tname':pthreadname})
		pmutex.release()

def getidx_process(pproclst,pthreadname):
	rst=-1
	for idxp in rang(0,len(pproclst),1):
		if pproclst[idxp]['tname']==pthreadname:
			rst=idxp
			break
	return rst
		
def remove_process(pproclst,pthreadname,pmutex):
	idx=0
	lremoved=False
	if pmutex.acquire(1):	
		while idx<len(pproclst):
			if pproclst[idx]['tname']==pthreadname:
				pproclst.remove(pproclst[idx])
				lremoved=True
				pmutex.release()
				break
			idx=idx+1
		if not lremoved:
			pmutex.release()
			
def add_dbcon(pdbconlst,pcon,pcur,pthreadname,pmutex):
	if pmutex.acquire(1):	
		pdbconlst.append({'dbcon':pcon,'dbcur':pcur,'tname':pthreadname})
		pmutex.release()

def getidx_dbcon(pdbconlst,pthreadname):
	rst=-1
	for idxp in rang(0,len(pdbconlst),1):
		if pdbconlst[idxp]['tname']==pthreadname:
			rst=idxp
			break
	return rst
		
def remove_dbcon(pdbconlst,pthreadname,pmutex):
	idx=0
	lremoved=False
	if pmutex.acquire(1):	
		while idx<len(pdbconlst):
			if pdbconlst[idx]['tname']==pthreadname:
				pdbconlst.remove(pdbconlst[idx])
				lremoved=True
				pmutex.release()
				break
			idx=idx+1
		if not lremoved:
			pmutex.release()		
			
def close_dbcon(pdbconlst,pidxdbcon):
	tname=''
	if pidxdbcon<len(pdbconlst):
		tname=pdbconlst[pidxdbcon]['tname']
		try:
			logaq('dbcon cancel long-running transaction...','i')
			#pdbconlst[pidxdbcon]['dbcur'].close()
			pdbconlst[pidxdbcon]['dbcon'].cancel()#Cancel a long-running transaction
		except Exception as e:
			logaq('dbcon cancel Error: %s' % e,'e')
		'''
		try:
			logaq('close dbcon...','i')
			pdbconlst[pidxdbcon]['dbcon'].close()
			logaq('close successfully','i')
		except Exception as e:
			logaq('db close Error: %s' % e,'e')
		'''
	return tname
			
def kill_process(pproclst,pidxproc):
	tname=''
	if pidxproc<len(pproclst):
		tname=pproclst[pidxproc]['tname']
		pproclst[pidxproc]['process'].terminate()
	return tname
			
def exist_tnsthread(pthreadlst,ptns):
	tdname=''
	for td in pthreadlst:
		tdname=td['thread'].getName()
		if tdname[len(public.GPREFIX_BNTHREAD):len(tdname)].upper()==ptns.upper():
			return True

def check_processthread(pproclst,pthreadlst,pmutex,pdbconlst,pkillall=False):
	idx=0
	logaq('check_processthread(ProCNT,ThrCNT):'+str(len(pproclst))+','+str(len(pthreadlst)),'i')
	while idx<len(pthreadlst) and not pkillall:
		if not pthreadlst[idx]['thread'].isAlive():
			logaq(pthreadlst[idx]['thread'].getName()+' have already over,removed from list','i')
			#remove_process(pproclst,pthreadlst[idx]['thread'].getName(),pmutex)
			pthreadlst.remove(pthreadlst[idx])
			continue
			
		if time.time()-pthreadlst[idx]['ctime']>public.gmmaxqtime+60:#spend ~20s before executing db procedure
			#close db
			tmpidx=getidx_dbcon(pdbconlst,pthreadlst[idx]['thread'].getName())
			if tmpidx>-1:
				tmptname=close_dbcon(pdbconlst,tmpidx)
				#remove_dbcon(pdbconlst,tmptname,pmutex)
				time.sleep(3)
			#kill processes
			tmpidx=getidx_process(pproclst,pthreadlst[idx]['thread'].getName())
			if tmpidx>-1:
				logaq('stop process(>maxqtime):'+str(pproclst[tmpidx]),'i')
				tmptname=kill_process(pproclst,tmpidx)
				if tmptname:
					logaq('stop successfully','i')
				else:
					logaq('maybe process have already over','w')
				time.sleep(3)
			#kill threads
			if pthreadlst[idx]['thread'].isAlive():
				logaq('stop thread(>maxqtime):'+pthreadlst[idx]['thread'].getName(),'i')
				stop_thread(pthreadlst[idx]['thread'])
				logaq('stop successfully','i')	
				continue
			else:
				logaq(pthreadlst[idx]['thread'].getName()+' have already over','i')
				#remove_process(pproclst,pthreadlst[idx]['thread'].getName(),pmutex)
				pthreadlst.remove(pthreadlst[idx])
				continue
		idx=idx+1
		
	if pkillall:
		#close db
		while len(pdbconlst)>0:
			tmptname=close_dbcon(pdbconlst,0)
			#remove_dbcon(pdbconlst,tmptname,pmutex)
			time.sleep(3)
				
		#kill processes
		idxp=0
		while len(pproclst)>0:
			if idxp==len(pproclst):
				idxp=0
			logaq('stop process(breaktime):'+str(pproclst[idxp]),'i')
			logaq('process cnt:'+str(len(pproclst)),'i')
			try:
				tmptname=kill_process(pproclst,idxp)
				if tmptname:
					logaq('stop successfully','i')
				else:
					logaq('maybe process have already over','w')
				time.sleep(3)
				idxp=idxp+1
				#remove_process(pproclst,tmptname,pmutex)#BUG DEADLOCK
			except Exception as e:
				logaq('process terminate Error: %s' % e,'e')
				time.sleep(10)
			
		
		#kill threads
		while len(pthreadlst)>0:
			time.sleep(3)
			if pthreadlst[0]['thread'].isAlive():
				logaq('stop thread(breaktime):'+pthreadlst[0]['thread'].getName(),'i')
				stop_thread(pthreadlst[0]['thread'])
				logaq('stop successfully','i')	
			else:
				logaq(pthreadlst[0]['thread'].getName()+' have already over','i')
				pthreadlst.remove(pthreadlst[0]) 
				

#test codes
'''
plst=[{'uid':'111','flag':'D','dbtns':'TNS'},{'uid':'112','flag':'D','dbtns':'TNSs'},{'uid':'113','flag':'D','dbtns':'TNS'},{'uid':'114','flag':'D','dbtns':'TNS'},{'uid':'115','flag':'D','dbtns':'TNS'},{'uid':'116','flag':'D','dbtns':'X'}]
SS=getuidstrlst_inmailslst(plst,'TNS','D','111')
print(SS)

public.gstop_time='12,1,2,3,23'
if breaktime():
	print('break')
else:
	print('running')

refresh_ini(CN_PARAPATH+CN_INI_FILE+'.ini')
print(public.gstop_time)
print(public.gmpd)
print(public.gmenpd)
print()
#print(get2betw13TEST('PAY77_UID1234567_file1.sql','_UID','.sql'))
#print(chardet.detect(open('d:\\query2\\100.txt','rb').read())['encoding'])
#plst=[{'uid':'111','flag':'D','dbtns':'TNS'},{'uid':'112','flag':'D','dbtns':'TNS'},{'uid':'113','flag':'D','dbtns':'TNS'},{'uid':'114','flag':'D','dbtns':'TNS'},{'uid':'115','flag':'D','dbtns':'TNS'},{'uid':'116','flag':'D','dbtns':'X'}]
#print(getuidstrlst_inmailslst(plst,'TNS','x',200))

init_log('-v')
glst=[]
print(id(glst))
refresh_paras(glst)
print('---end')
print(glst)
print(id(glst))

tmpfileslst = os.listdir('d:\\query2')

tmpfileslst.sort()
for i in range(0,len(tmpfileslst),1):
	tmppath = os.path.join('d:\\query2',tmpfileslst[i])
	#os.rename(tmppath,'d:\\query2\\'+str(i)+'.sql')
	print(chardet.detect(open(tmppath, 'rb').read())['encoding'])
	#print(getfileencode(tmppath)+' --'+tmpfileslst[i])

#test riskfactors
ptest='select * from dual;drop table a;truncate table b'
print(remove_riskfactors(ptest))

#test multithread dict data process
logaq('hello','d')
logaq('hello','i')
logaq('hello','w')
logaq('hello','e')
logaq('default info')

def newdict():
	dict = {'uid':'','flag':'N'}
	return dict
dictlst=[]
#del dictlst[:]	
dictlst.append(newdict())
dictlst[len(dictlst)-1]['uid']='1001'
dictlst[len(dictlst)-1]['flag']='M'
dictlst.append(newdict())
dictlst[len(dictlst)-1]['uid']='1002'
dictlst[len(dictlst)-1]['flag']='M'
dictlst.append(newdict())
dictlst[len(dictlst)-1]['uid']='1003'
dictlst[len(dictlst)-1]['flag']='M'
print(dictlst)
mutex = threading.Lock()
deldict_fromlist(dictlst,mutex,'uid','1001')
print(dictlst)
'''
'''
#1.test init&encrypt&serializing
#initparacsv_serializing()

#2.test unserializing&decrypt
paralstnew=getobject(CN_PARAFILE)
print('------\n')
print(paralstnew)
for i in range(0,len(paralstnew),1):
	paralstnew[i]['dbpd']=base64.decodestring(paralstnew[i]['dbpd']).decode(chset)
	paralstnew[i]['ospd']=base64.decodestring(paralstnew[i]['ospd']).decode(chset)
print('------decode\n')
print(paralstnew)

#3.test replace_oldkeyword
stext='  --remark\r\n   select a.c,a.b from a where a.d in(select b.a from b where b.b>sysdate-1);  \r\n--remark\r\n   select * from a where a.a >1 union select * from b where b.b>4 \r\n;   \r\n drop table c ; '
#stext=' select * from dual '
sold='select'
snew='create or replace view UID12435_file1_ as select '
ssep=';'
print('----\n'+stext)
#replace_oldkeyword(ptext,pold,pnew,psegseptor,pnewautoadd=0,pnewvar='')
stemp=replace_oldkeyword(stext,sold,snew,ssep,1,'UID12435_file1_')
print('----\n'+stemp)
'''



	
	

