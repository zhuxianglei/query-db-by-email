#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
	Version  Created by     Creation Time  Description
	-------  -------------  -------------  ----------------------------------------------
	1.0      xlzhu@ips.com  2018-10-31     exec sql&producing datafile;test passed,20181106
										   solving BUG of mutithread call subprocess.Popen(sqlplus) 20181127
	Version  Updated by     Update Time    Description
	-------  -------------  -------------  ----------------------------------------------

'''

import os
import sys
import time
import shutil
import chardet
import public
import threading
from subprocess import Popen, PIPE
from common import get2betw13,getidxbyuidflag,saveobject,logaq,getdbusrbydbtns,breaktime,getmailsaok,setmailsaok,sets2fileok,add_process,remove_process
from common import replace_oldkeyword,remove_riskfactors,getsqlpluscon,updateflagmsg_byuidflag,getflagbyuid,clearblankchar,check_fileformat
from common import getdbusrbyuid,getdbtnsbyuid,getcxoracon,add_dbcon,remove_dbcon
import cx_Oracle

#contants&variables
S2_SQLAPP='sqlplus'
S2_ERROR=['Error','ORA-']
s2_charset='utf-8'
S2_SELECT='select'
S2_CREATEORREP='create or replace view '
S2_SUFFIX='_ as select '
S2_SEGSEPTOR=';'
S2_PROC_NAME='SYS.EXPORTCSV_BYVIEWS'
S2_PROC_PRODUCTDATA="EXEC "+S2_PROC_NAME+"('POWNER','PVIEWLIKE');"
S2_RFACTORHINT="Warning:mail file exist risk keyword:"
S2_MSG1="UID of attachment Can't find in maillist"
S2_MSG2="Exist UID file maybe execute too long time,Please check explain plan!"
S2_MSG3="Can't find db connection info in paramslist"
S2_MSG4="Attachment content format Error!Please check&modify&send again."
S2_MSG5="Error:Query_be_terminated(maybe_timeout/exception,NOERRCODE&MSG_return)."
S2_TAIL=',Please modify&send again'

def execsfile(pconstr,pfile):
	global s2_charset
	logaq('NLS_LANG='+os.environ["NLS_LANG"])
	logaq(S2_SQLAPP+' constr:'+get2betw13(pconstr,'','/')+'/******@'+get2betw13(pconstr,'@','')+' file:'+pfile)#/xlzhu@
	procf = Popen([S2_SQLAPP, '-S', pconstr], stdout=PIPE, stdin=PIPE, stderr=PIPE)
	logaq('opensqlplus id:'+str(id(procf)))
	logaq('begin create views...')
	#procf.stdin.write(('@'+pfile).encode())#exist bug
	try:
		(outf, errf) = procf.communicate(input=('@'+pfile).encode(s2_charset),timeout=60)
		logaq('create views end')
	except TimeoutExpired:
		logaq('sqlplus timeout','w')
		procf.terminate()
		(outf,errf) = procf.communicate()
	except Exception as e:
		logaq('sqlplus Exception: %s' % e,'w')
		(outf,errf) = procf.communicate()
		
	if procf.returncode!=0:
		returnmsgf=errf.decode()
		logaq('returncode:'+str(procf.returncode)+'err:'+returnmsgf+'out:'+outf.decode(),'e')
		if len(returnmsgf)==0:
			returnmsgf=outf.decode()
		if len(returnmsgf)==0:
			returnmsgf=S2_MSG5
		#sys.exit(procf.returncode)
	else:
		returnmsgf=outf.decode()
	return returnmsgf

def exeprocedure_bycxora(pconstr,pproname,pproparaowner,pproparaviewlike,pdbconlst,pmutex):
	#instead of execprocedure; solving BUG:mutithread call sqlplus hang
	rst='begin exeprocedure_bycxora'
	logaq('ConToDB constr:'+get2betw13(pconstr,'','/')+'/******@'+get2betw13(pconstr,'@',''))
	logaq('callproc:'+pproname+'('+pproparaowner+','+pproparaviewlike+')')
	try:
		odb=cx_Oracle.Connection(pconstr)
		dbcur = odb.cursor()
	except Exception as e:
		logaq('DB connect Error: %s' % e,'e')
		rst="Error:DB connect Exception"
		return rst
		
	try:
		add_dbcon(pdbconlst,odb,dbcur,threading.currentThread().getName(),pmutex)
		logaq('dblst size(AaddedBcall):'+str(len(pdbconlst)))
		dbcur.callproc(pproname, [pproparaowner,pproparaviewlike])
		logaq('remove dbfromlst')
		remove_dbcon(pdbconlst,threading.currentThread().getName(),pmutex)
		logaq('dblst size(After call&removed):'+str(len(pdbconlst)))
	except Exception as e:
		logaq('DB Error: %s' % e,'e')
		rst=S2_MSG5
		logaq('remove dbfromlst')
		remove_dbcon(pdbconlst,threading.currentThread().getName(),pmutex)
		logaq('dblst size(After call&removed):'+str(len(pdbconlst)))
	finally:
		try:
			dbcur.close()
		except Exception as e:
			logaq('dbcur close Error: %s' % e,'e')
		try:
			odb.close()
		except Exception as e:
			logaq('dbcon close Error: %s' % e,'e')
	return rst	
	
def execprocedure(pconstr,pprocname,pproclst,pmutexp):
	'''
	if procp.stdin:
		procp.stdin.close()
	if procp.stdout:
		procp.stdout.close()
	if procp.stderr:
		procp.stderr.close()
	'''
	logaq(S2_SQLAPP+' constr:'+pconstr+' procedure:'+pprocname)
	procp = Popen([S2_SQLAPP, '-S', pconstr],stdout=PIPE, stdin=PIPE, stderr=PIPE)
	logaq('opensqlplus id:'+str(id(procp)))
	add_process(pproclst,procp,threading.currentThread().getName(),pmutexp)
	logaq('sqlpluslst size(AaddedBexec):'+str(len(pproclst)))
	#procp.stdin.write(pprocname.encode())#exist bug
	try:
		(outp,errp) = procp.communicate(input=pprocname.encode(),timeout=public.gmmaxqtime)
	except TimeoutExpired:
		logaq('sqlplus timeout','w')
		procp.terminate()
		(outp,errp) = procp.communicate()
	except Exception as e:
		logaq('sqlplus Exception: %s' % e,'w')
		(outp,errp) = procp.communicate()
		
	if procp.returncode!=0:
		returnmsgp=errp.decode()
		logaq('returncode:'+str(procp.returncode)+'err:'+returnmsgp+'out:'+outp.decode(),'e')
		if len(returnmsgp)==0:
			returnmsgp=outp.decode()
		if len(returnmsgp)==0:
			returnmsgp=S2_MSG5
	else:
		returnmsgp=outp.decode()
	logaq('remove sqlplusfromlst')
	remove_process(pproclst,threading.currentThread().getName(),pmutexp)
	logaq('sqlpluslst size(After exec&removed):'+str(len(pproclst)))
	return returnmsgp
	
def checkfilestatus(pmailslst,ppath,pfilename):
	rst=False
	tmpidxuid=getidxbyuidflag(get2betw13(pfilename,public.GPREFIX_FILEUID,public.GSEPTOR_FILE),public.GSTATUS_SQLDATA_BEFEXPORT,pmailslst)
	if tmpidxuid>-1:
		logaq(pfilename+':'+S2_MSG2,'w')
		#shutil.move(ppath,public.gdir_history)
	else:
		tmpidxuid=getidxbyuidflag(get2betw13(pfilename,public.GPREFIX_FILEUID,public.GSEPTOR_FILE),public.GSTATUS_ATACHMT_DOWNLOADED,pmailslst)
		if tmpidxuid<0:
			#write error file to his,move this file to his
			logaq(pfilename+':'+S2_MSG1,'e')
			#writefile(os.path.join(public.gdir_history,pfilename),S2_MSG1,'.err')
			if os.path.exists(public.gdir_history+pfilename):
				os.remove(public.gdir_history+pfilename)
			logaq('moving file to his')
			shutil.move(ppath,public.gdir_history)
		else:
			rst=True
	return rst

def processfile(pmailslst,pmutex,ppath,pfilename):
	#process file content for creating view 
	#Format:
	#old file:select ...... from ......where ......;select ...... from ...... where ......;
	#new file:create view filename_seq1V as select ...... from ......where ......;create view filename_seq2V as select ...... from ...... where ......; 
	global s2_charset
	ltmpuid=get2betw13(pfilename,public.GPREFIX_FILEUID,public.GSEPTOR_FILE)
	tmpfd=open(ppath, 'rb')
	try:
		tmpdata=tmpfd.read()
	finally:
		tmpfd.close()
	s2_charset=chardet.detect(tmpdata)['encoding']
	if s2_charset[0:3]=='UTF':#AMERICAN_AMERICA.ZHS16GBK   AL32UTF8  ZHS16CGB231280
		os.environ["NLS_LANG"] = public.GNLS_LANG_UTF8
	else:
		os.environ["NLS_LANG"] = public.GNLS_LANG_GBK
	
	if check_fileformat(tmpdata.decode(s2_charset)):
		lviewname=public.GFLAG_UID+get2betw13(pfilename,public.GPREFIX_FILEUID,public.GSUFFIX_FILEMAIL)
		tmpfiletext=replace_oldkeyword(tmpdata.decode(s2_charset),S2_SELECT,S2_CREATEORREP+lviewname+S2_SUFFIX,S2_SEGSEPTOR,1,lviewname+'_')
		tmperr,tmpfiletext=remove_riskfactors(tmpfiletext)
		if tmperr:
			updateflagmsg_byuidflag(pmailslst,pmutex,ltmpuid,public.GSTATUS_ATACHMT_DOWNLOADED,public.GSTATUS_ERROR,S2_RFACTORHINT+tmperr+S2_TAIL)
			logaq(pfilename+':'+S2_RFACTORHINT+tmperr,'e')
			if os.path.exists(public.gdir_history+pfilename):
				os.remove(public.gdir_history+pfilename)
			logaq('moving file to his')
			shutil.move(ppath,public.gdir_history)
			return False
							
		tmpdata=tmpfiletext.encode(s2_charset)
		tmpfd = open(ppath, 'wb')
		try:
			tmpfd.write(tmpdata)
		finally:
			tmpfd.close()
		return True
	else:
		updateflagmsg_byuidflag(pmailslst,pmutex,ltmpuid,public.GSTATUS_ATACHMT_DOWNLOADED,public.GSTATUS_ERROR,S2_MSG4)
		logaq(pfilename+':'+S2_MSG4,'e')
		if os.path.exists(public.gdir_history+pfilename):
			os.remove(public.gdir_history+pfilename)
		logaq('moving file to his')
		shutil.move(ppath,public.gdir_history)
		return False
		
def callprocedure(pmailslst,pparaslst,pmutex,ppath,pfilename,pproclst,pmutexp,pdbconlst):#add pdbconlst
	#!!!!!!!!!!this procedure maybe spend a long time!!!!!!!!!!!
	#BUG FINDED 20181123
	ltmpuid=get2betw13(pfilename,public.GPREFIX_FILEUID,public.GSEPTOR_FILE)
	updateflagmsg_byuidflag(pmailslst,pmutex,ltmpuid,public.GSTATUS_ATACHMT_DOWNLOADED,public.GSTATUS_SQLDATA_BEFEXPORT,'',time.time())
	ltmpviewlike=public.GFLAG_UID+get2betw13(pfilename,public.GPREFIX_FILEUID,public.GSUFFIX_FILEMAIL)+'%'
	ltmpowner=getdbusrbyuid(ltmpuid,pmailslst).strip()
	ltmpdbtns=getdbtnsbyuid(ltmpuid,pmailslst)
	if len(ltmpowner)==0:
		ltmpowner=getdbusrbydbtns(ltmpdbtns,pparaslst)
	logaq('dbtns:'+ltmpdbtns)
	logaq('dbusr:'+ltmpowner)
	lrst=exeprocedure_bycxora(getcxoracon(ltmpdbtns,ltmpowner,pparaslst),S2_PROC_NAME,ltmpowner.upper(),ltmpviewlike.upper(),pdbconlst,pmutexp)
	#!!!!!!!remarked for bug of mutithread call Popen sqlplus(End of exec procedure,but sqlplus can't exit)
	#ltmpprocname=S2_PROC_PRODUCTDATA.replace('POWNER',ltmpowner.upper())
	#ltmpprocname=ltmpprocname.replace('PVIEWLIKE',ltmpviewlike.upper())
	#lrst=execprocedure(getsqlpluscon(ltmpdbtns,ltmpowner,pparaslst),ltmpprocname,pproclst,pmutexp)
	lsqlerr=''
	
	for lerr in S2_ERROR:
		if lrst.lower().find(lerr.lower())>-1:
			lsqlerr=lrst
	if lsqlerr:#procedure exec error
		lsqlerr=clearblankchar(lsqlerr)
		updateflagmsg_byuidflag(pmailslst,pmutex,ltmpuid,public.GSTATUS_SQLDATA_BEFEXPORT,public.GSTATUS_ERROR,lsqlerr)
		logaq(pfilename+':'+lsqlerr,'e')
		if os.path.exists(public.gdir_history+pfilename):
			os.remove(public.gdir_history+pfilename)
		logaq('moving file to his')
		shutil.move(ppath,public.gdir_history)
	else:#cannot updateflag to exported until all UID attachments process over.
	#	updateflagmsg_byuidflag(pmailslst,pmutex,pmailslst[pidxuid]['uid'],public.GSTATUS_SQLDATA_BEFEXPORT,public.GSTATUS_SQLDATA_EXPORTED,'')
		if os.path.exists(public.gdir_history+pfilename):
			os.remove(public.gdir_history+pfilename)
		logaq('moving file to his')
		shutil.move(ppath,public.gdir_history)

def createviews(pmailslst,pparaslst,pmutex,ppath,pfilename):
	#login to sqlplus exec sqlfile
	rst='OK'
	lsqlerr=''
	ltmpuid=get2betw13(pfilename,public.GPREFIX_FILEUID,public.GSEPTOR_FILE)
	lcon=getsqlpluscon(getdbtnsbyuid(ltmpuid,pmailslst),getdbusrbyuid(ltmpuid,pmailslst),pparaslst)
	if lcon:
		lrst=execsfile(lcon,ppath)
		try:
			logaq('execsfile return')
		except Exception as e:
			logaq('log Error: %s' % e,'e')
		
	else:
		logaq(pfilename+':'+S2_MSG3,'e')
		if os.path.exists(public.gdir_history+pfilename):
			os.remove(public.gdir_history+pfilename)
		logaq('moving file to his')
		shutil.move(ppath,public.gdir_history)
		rst=''
		return rst
	#logaq('judging returnstr error')					
	for lerr in S2_ERROR:
		if lrst.lower().find(lerr.lower())>-1:
			lsqlerr=lrst
	#logaq('judging end 20s')
	time.sleep(20)
	if lsqlerr:#sqlfile exec error
		lsqlerr=clearblankchar(lsqlerr)
		updateflagmsg_byuidflag(pmailslst,pmutex,ltmpuid,public.GSTATUS_ATACHMT_DOWNLOADED,public.GSTATUS_ERROR,lsqlerr)
		logaq(pfilename+':'+lsqlerr,'e')
		if os.path.exists(public.gdir_history+pfilename):
			os.remove(public.gdir_history+pfilename)
		logaq('moving file to his')
		shutil.move(ppath,public.gdir_history)
		rst=''	
	return rst
		
def s2file_main(pmailslst,pparaslst,pmutex,pdb,pproclst,pmutexp,pdbconlst):#add pdbconlst
	while not getmailsaok(public.gmutex_mailsa) and not breaktime():
		logaq('waiting for mailsa 10s...','i')
		time.sleep(10)
		continue
	preuid=''
	curuid=''
	try:
		fileslst = os.listdir(public.gdir_atachmt_downloaded)
		fileslst.sort()
		logaq('FilesCNT:'+str(len(fileslst)))
		for i in range(0,len(fileslst),1):
			try:
				lexistnewfile=False
				#only process file start with pdb
				logaq('Process idx:'+str(i))
				if get2betw13(fileslst[i],'',public.GPREFIX_FILEUID)==pdb:
					logaq('Curfile:'+fileslst[i])
					lpath = os.path.join(public.gdir_atachmt_downloaded,fileslst[i])
					curuid=get2betw13(fileslst[i],public.GPREFIX_FILEUID,public.GSEPTOR_FILE)
					#if exist one attachment error in same mail,will not process others
					if getflagbyuid(curuid,pmailslst)==public.GSTATUS_ERROR:
						if os.path.exists(public.gdir_history+fileslst[i]):
							os.remove(public.gdir_history+fileslst[i])
						logaq('moving file to his')
						shutil.move(lpath,public.gdir_history)
						continue
					#if all attachment have been processed in same mail,update flag to GSTATUS_SQLDATA_EXPORTED
					if preuid and preuid!=curuid:
						updateflagmsg_byuidflag(pmailslst,pmutex,preuid,public.GSTATUS_SQLDATA_BEFEXPORT,public.GSTATUS_SQLDATA_EXPORTED,'')
					if time.time()-os.path.getmtime(lpath)<=1:
						logaq('MTime<=1:'+lpath)
						lexistnewfile=True
						continue
						
					if not checkfilestatus(pmailslst,lpath,fileslst[i]):
						continue
					else:
						#process file context
						#1.judging file charset
						#2.changing select segment to create or replace view ..... as select segment
						#3.judging whether exist risk factors
						if not processfile(pmailslst,pmutex,lpath,fileslst[i]):
							continue
						#end process file context
						#login to sqlplus exec sqlfile
						if not createviews(pmailslst,pparaslst,pmutex,lpath,fileslst[i]):
							continue
						else:
							callprocedure(pmailslst,pparaslst,pmutex,lpath,fileslst[i],pproclst,pmutexp,pdbconlst)
					preuid=curuid
					logaq('Curfile end')
			except Exception as e:
				logaq('s2file Forloop Error: %s' % e,'e')
				time.sleep(3)
				updateflagmsg_byuidflag(pmailslst,pmutex,curuid,public.GSTATUS_ATACHMT_DOWNLOADED,public.GSTATUS_ERROR,str(e))
				updateflagmsg_byuidflag(pmailslst,pmutex,curuid,public.GSTATUS_SQLDATA_BEFEXPORT,public.GSTATUS_ERROR,str(e))
		updateflagmsg_byuidflag(pmailslst,pmutex,curuid,public.GSTATUS_SQLDATA_BEFEXPORT,public.GSTATUS_SQLDATA_EXPORTED,'')
		if not lexistnewfile:
			setmailsaok(public.gmutex_mailsa,False)
		else:
			time.sleep(3)
		sets2fileok(public.gmutex_s2file,True)
		logaq(pmailslst)
	except Exception as e:
		logaq('s2file Error: %s' % e,'e')
	finally:
		#saveobject(pmailslst,'maills2')
		logaq('s2file finally')
		#refresh_ini()

'''
consss='query_ro/pwd//ip:1521/service_name'
uu='QUERY_RO'
vv='UID1435941225_FILE1%'
glmutex = threading.Lock()	
clst=[]
init_log('-v')
exeprocedure_bycxora(consss,S2_PROC_NAME,uu,vv,clst,glmutex)

conn=cx_Oracle.connect(consss)
cur=conn.cursor()
cur.callproc(S2_PROC_NAME,[uu,vv])
#conn.commit();
cur.close()
conn.close()
'''