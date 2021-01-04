#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
	Version  Created by     Creation Time  Description
	-------  -------------  -------------  ----------------------------------------------
	1.0      xlzhu@ips.com  2018-10-30     public global contants&variables
	
	Version  Updated by     Update Time    Description
	-------  -------------  -------------  ----------------------------------------------
	
'''
import threading

glogger=None
gsklock=None
gmailsaok=False
gs2fileok=False
gfilepgok=False
gmutex_mailsa=threading.Lock()
gmutex_s2file=threading.Lock()
gmutex_filepg=threading.Lock()
GVERSION='BetaVer1.0'
GSYS_FNAME='AutomaticQuerySystemBaseOnEMail'
GSYS_SNAME='AQBM'
GCODES=['UTF-8','GBK']
GUTF8_BOM = b'\xef\xbb\xbf'
GRISK_FACTORS1=['drop','truncate','alter','delete','execute']
GRISK_FACTORS2=['table','table','table','from','immediate']
GOS_LINUX='Linux'
GOS_SUN='SunOS'
GNLS_LANG_UTF8='AMERICAN_AMERICA.UTF8'
GNLS_LANG_GBK='AMERICAN_AMERICA.ZHS16GBK'
GMAXSECS_RUN=1800
GSTATUS_INIT='I'
GSTATUS_ATACHMT_DOWNLOADED='D' 
GSTATUS_ATACHMT_UPLOADED='U'#UNUSED
GSTATUS_SQLDATA_BEFEXPORT='B'#TEMP STATUS,if this status exist for long time,sql explain plan too bad!
GSTATUS_SQLDATA_EXPORTED='S'
GSTATUS_SQLDATA_REEXPORT='R'
GSTATUS_SQLDATA_DOWNLOADED='G'
GSTATUS_SQLDATA_SENDED='O'
GSTATUS_ERROR='E'
GPREFIX_FILEUID='_UID'
GSUFFIX_FILEMAIL='.sql'
GSUFFIX_TARGZFILEMAIL='.tar.gz'
GFLAG_UID='UID'
GSEPTOR_FILE='_'
GPREFIX_BNTHREAD='Thread_s2f'
GTARGET_BNTHREAD='s2file_main'
gdir_atachmt_downloaded=''
gdir_atachmt_uploaded=''#UNUSED
gdir_atachmt_willsend=''
gdir_history=''
gdir_log=''
gstop_time=''
gsingleinstance_prt=64444
gmaxcnt_bnthread=3
gsleep=10
gmusr = ''
gmpd = ''
gmenpd = ''
gmcycle=600
gmfromallow=''
gmmaxqtime=1800
giwkdir = '' 
giprt = '' 
gihst = '' 
gsprt = ''
gshst = ''