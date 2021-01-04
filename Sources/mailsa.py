#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
	Version  Created by     Creation Time  Description
	-------  -------------  -------------  ----------------------------------------------
	1.0      xlzhu@ips.com  2018-10-22     Receiving&analyzing mail,get key item list&attachments;test passed,20181106
	
	Version  Updated by     Update Time    Description
	-------  -------------  -------------  ----------------------------------------------
	
'''
import imaplib
import email 
import pickle
import public
import time
import threading
import chardet
from common import logaq,clearctrlchar,saveobject,addmaillist,get2betw13
from common import getkeyword,getdbtnsbymaildb,breaktime,setmailsaok,getpdbyen,refresh_ini,deldict_fromlist
#Attention!!!!!!!!!!!!!!!!!prod enviroment please unremark delete mail&remark break!!!!!!!!!!!!!!!!!!!!!!

#contants&variables
MS_MSG1="Error:Can't find right database keyword in your mail,Please modify&send again"
MS_MSG2="Error:Can't find database info in mail/parameter,break this,to next mail"
MS_MSG3="Error:Can't find attachment info in mail,Please modify&send again"
ma_chset='utf-8'
ma_existatta=False
#add global var,cancel value to maillst[len(maillst)-1](exist bug in mutithread env)
ma_uid=''
ma_from=''
ma_to=''
ma_cc=''
ma_sub=''
ma_flag='I'
ma_ftime=''
ma_dbusr=''
ma_dbtns=''
ma_msg=''

#decoding by charset
def decode_str(s):
    value, charset = email.header.decode_header(s)[0]
    if charset:
        value = value.decode(charset)
    return value	

#create mail key item dict
def newmailmeta():
#I:init;D:downloaded;U:uploaded;S:server process;E:ERROR
	mailmeta = {'uid':'','from':'', 'to':'','cc':'','subject':'','flag':'I','ftime':'','dbusr':'','dbtns':'','msg':''}
	return mailmeta

def clearmailmeta():
	global ma_uid
	global ma_from
	global ma_to
	global ma_cc
	global ma_sub
	global ma_flag
	global ma_ftime
	global ma_dbusr
	global ma_dbtns
	global ma_msg
	ma_uid=''
	ma_from=''
	ma_to=''
	ma_cc=''
	ma_sub=''
	ma_flag='I'
	ma_ftime=''
	ma_dbusr=''
	ma_dbtns=''
	ma_msg=''	
	
#get mail uid
def getmailuid(str):
	tempstr=getkeyword(str,'UID')
	return tempstr.replace(')','')
	
def allow_from(pfrom,pallowlst):
	rst=False
	for af in pallowlst.split(','):
		if pfrom.lower().find(af.lower())>-1:
			rst=True
			break
	return rst
	
def format_tocc(pstr):
	rst=''
	for ml in pstr.split(','):
		tmpem=get2betw13(ml,'<','>')
		if not tmpem:
			tmpem=ml
		if not rst:
			rst=tmpem.strip()
		else:
			rst=rst+','+tmpem.strip()
	return rst	
	
def process_mailheader(pmsg):
	global ma_from
	global ma_to
	global ma_cc
	global ma_sub
	logaq('--from||to||cc||subject--','i')
	ma_sub=clearctrlchar(decode_str(pmsg.get('subject')))
	ma_from=clearctrlchar(email.utils.parseaddr(pmsg.get('from'))[1])	
	ma_to=clearctrlchar(pmsg.get('to'))	
	ma_to=format_tocc(ma_to)
	cc=pmsg.get('cc')
	if cc:
		ma_cc=clearctrlchar(cc)
		ma_cc=format_tocc(ma_cc)
	logaq(ma_from+'||'+ma_to+'||'+ma_cc+'||'+ma_sub,'i')
	
def process_mailbody(pparaslst,pmsg,pmidx):
	isfirstpart=1
	idxattach=0
	global ma_existatta
	global ma_chset
	global ma_flag
	global ma_ftime
	global ma_dbusr
	global ma_dbtns
	global ma_msg
	ma_existatta=False
	for part in pmsg.walk():
		if not part.is_multipart():
			try:
				ma_chset=chardet.detect(part.get_payload(decode=True))['encoding']
				logaq('bodyencode:'+str(ma_chset))
				#ma_chset='utf-8'
				part.get_payload(decode=True).decode(ma_chset)#mail content
			except Exception as e:
				logaq(str(ma_chset)+' decode error(will default utf-8): %s' % e,'e')
				ma_chset='utf-8'
				part.get_payload(decode=True).decode(ma_chset)
			if not ma_dbusr and isfirstpart:
				tmpstr=getkeyword(part.get_payload(decode=True).decode(ma_chset),'username')
				if tmpstr:
					ma_dbusr=tmpstr
			if not ma_dbtns and isfirstpart:
				tmpstr=getkeyword(part.get_payload(decode=True).decode(ma_chset),'database')
				logaq('database:'+tmpstr,'i')
				if tmpstr:
					ma_dbtns=getdbtnsbymaildb(tmpstr,pparaslst,pmidx)
				logaq('tns:'+ma_dbtns,'i')
			if not ma_dbtns:
				ma_msg=MS_MSG1
				ma_flag=public.GSTATUS_ERROR
				logaq(MS_MSG2,'e')
				break
			isfirstpart=0
			#################-----------downloading attachments
			file_name = part.get_filename()
			contType = part.get_content_type()
			logaq('attachment:'+str(file_name))
			if file_name: 
				ma_existatta=True
				idxattach=idxattach+1
				'''remark for unifying attachment name format:TNS+'_UID'+UID+'_file'+sequence.sql
				h = email.header.Header(file_name)
				dh = email.header.decode_header(h)#decode attachment name
				filename = dh[0][0]
				logaq(filename)
				logaq(dh[0][1])
				if dh[0][1]:
					filename = decode_str(str(filename,dh[0][1]))
					logaq(filename)
				'''
				data = part.get_payload(decode=True)#begin download
				lfilename=ma_dbtns + '_UID' + ma_uid + '_file' + str(idxattach)
				logaq('download data to file:'+public.gdir_atachmt_downloaded + lfilename + public.GSUFFIX_FILEMAIL)
				att_file = open(public.gdir_atachmt_downloaded + lfilename + public.GSUFFIX_FILEMAIL, 'wb')
				try:
					att_file.write(data)#save attachment
				finally:
					att_file.close()
			#################----------end download
			
def relogin_mail(ppretime,pcon):
	try:
		if time.time()-ppretime>public.gmcycle:
			try:
				pcon.close()
				pcon.logout()
				logaq('mailsa conclosed(>cycle)','w')
			except Exception as e:
				logaq('mailsa conclose: %s' % e,'e')
			tmpcon = imaplib.IMAP4_SSL(port = public.giprt,host = public.gihst)
			logaq('connect ok','i')
			tmpcon.login(public.gmusr,getpdbyen(public.gmenpd))
			logaq('login ok','i')
			return tmpcon
		else:
			return pcon
	except Exception as e:
		logaq('mailsa ini cycle: %s' % e,'e')	
		
def maila_main(pmailslst,pparaslst,pmutex):
	global ma_uid
	global ma_from
	global ma_to
	global ma_cc
	global ma_sub
	global ma_flag
	global ma_ftime
	global ma_dbusr
	global ma_dbtns
	global ma_msg
	if breaktime():
		exit()
	try:
		lpretime=time.time()
		ma_uid=''
		conn = imaplib.IMAP4_SSL(port = public.giprt,host = public.gihst)
		logaq('connect ok','i')
		conn.login(public.gmusr,getpdbyen(public.gmenpd))
		logaq('login ok','i')
		while not breaktime():
			try:
				global ma_chset
				global ma_existatta
				#del pmailslst[:]
				conn=relogin_mail(lpretime,conn)
				conn.select(public.giwkdir)
				logaq('working in folder:%s' % public.giwkdir,'i')
				#type, data = conn.search(None, 'ALL', 'SUBJECT "*review1810221613"')#can not search mail like 'keyword'
				type, data = conn.search(None, 'ALL')
				mailbidx=data[0].split()#[b'1',b'2',b'3']
				mailcnt=len(mailbidx)
				logaq('mail cnt:'+str(mailcnt),'i')
				for num in range(mailcnt-1,-1,-1):#process from new mail(if delete mail,Don't begin from old,else Error:Nontype for decode)
					try:
						#pmailslst.append(newmailmeta())
						#get mailuid
						clearmailmeta()
						type, data = conn.fetch(mailbidx[num], 'UID')
						ma_chset=chardet.detect(data[0])['encoding']
						logaq('uidencode:'+ma_chset)
						ma_uid=getmailuid(str(email.message_from_string(data[0].decode(ma_chset))))
						logaq('current mail uid:'+ma_uid,'i')
						#get maildetailinfo
						type, data = conn.fetch(mailbidx[num], 'RFC822')
						logaq('--MailBinaryInfo:')
						logaq(data)
						logaq('--EndBinaryInfo')
						try:
							ma_chset=chardet.detect(data[0][1])['encoding']
							logaq('mailencode:'+ma_chset)
						except Exception as e:
							logaq(str(ma_chset)+' decode error(will default utf-8): %s' % e,'e')
							ma_chset='utf-8'
						msg = email.message_from_string(data[0][1].decode(ma_chset))#.decode('gbk') 
						process_mailheader(msg)
						if allow_from(ma_from,public.gmfromallow):
							process_mailbody(pparaslst,msg,num)
							if ma_msg:
								ma_flag=public.GSTATUS_ERROR
							else:
								if not ma_existatta:
									ma_msg=MS_MSG3
									ma_flag=public.GSTATUS_ERROR
								else:
									ma_flag=public.GSTATUS_ATACHMT_DOWNLOADED
						else:
							logaq('mail will be deleted directly(sender not in allow list),uid:'+ma_uid,'w')
							ma_uid=''
					except Exception as e:
						logaq('mailsa Forloop Error: %s' % e,'e')
						ma_flag=public.GSTATUS_ERROR
						ma_msg=str(e)
						time.sleep(3)
					
					addmaillist(pmailslst,pmutex,ma_uid,ma_from,ma_to,ma_cc,ma_sub,ma_flag,ma_ftime,ma_dbusr,ma_dbtns,ma_msg)
					#--testing please remark this codes
					logaq('Deleting mail uid:'+ma_uid,'i')
					conn.store(mailbidx[num], '+FLAGS', '\\Deleted') #delete mail
					#--testing remark
						
				logaq(pmailslst)
				setmailsaok(public.gmutex_mailsa,True)
				logaq('maila_main sleep '+str(public.gsleep)+'s,refresh_ini...')
				time.sleep(public.gsleep)
				refresh_ini()
				#break;#if delete mail,please mark break
			except Exception as e:
				logaq('mailsa while Error: %s' % e,'e')
				time.sleep(3)
	finally:
		try:
			conn.close()
		except Exception as e:
			logaq('connect close exception: %s' % e,'i')
		conn.logout()
		logaq('logout mailserver','i')
		saveobject(pmailslst,'maillst')
		logaq('serializing maillist','i')

