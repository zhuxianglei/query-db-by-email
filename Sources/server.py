#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    Version  Created by     Creation Time  Description
    -------  -------------  -------------  ----------------------------------------------
    1.0      xlzhu@ips.com  2018-11-26     executed by server,tar&zip&split files,instead of shell script,
                                           solving shell cmd bug between linux&sunos
										   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
										   Be discarded because of executing too slowly than shell
										   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    Version  Updated by     Update Time    Description
    -------  -------------  -------------  ----------------------------------------------

'''
import os
import time
import glob
import tarfile

ls_splitfilemaxMB=50
ls_splitfilemaxB=ls_splitfilemaxMB*1024*1024

def get2betw13(pstr,pfirst,pthird):
    if not pfirst:
        idxfirst=0
    else:
        idxfirst=pstr.lower().find(pfirst.lower())+len(pfirst)
    if not pthird:
        idxthird=len(pstr)
    else:
        idxthird=pstr.lower().find(pthird.lower(),idxfirst+len(pfirst))
    return pstr[idxfirst:idxthird]

def targz_files(pobjfilelike):
    #pobjfilelike:'./UID*.csv'
    uidcsvlst = []
    pre_tarfile=''
    cur_tarfile='?'
    btime_tarfile=0
    uidcsvlst = glob.glob(pobjfilelike)
    uidcsvlst.sort()
    for uidcsv in uidcsvlst:
        if time.time()-os.path.getmtime(uidcsv)<1:
            print('file:'+uidcsv+' is being modified,goto next')
            break
        print('process file:'+uidcsv)
        cur_tarfile=get2betw13(uidcsv,'','_')
        if cur_tarfile!=pre_tarfile:
            print('tar&gz:'+cur_tarfile+'*')
            if pre_tarfile:
                print('close pretar:'+pre_tarfile+'*')
                tar.close
                print('tar&gz spend(s):'+str(time.time()-btime_tarfile))
            btime_tarfile=time.time()
            tar = tarfile.open(cur_tarfile+'.tar.gz','w:gz')
        print('add:'+get2betw13(uidcsv,'/',''))
        tar.add(get2betw13(uidcsv,'/',''))
        pre_tarfile=cur_tarfile
    print('close last tar&gz')
    tar.close
    print('tar&gz spend(s):'+str(time.time()-btime_tarfile))

def splitbigfile(pbigfile,chunksize=ls_splitfilemaxB):
    #pbigfile format:./UID0000000000.tar.gz
    partnum = 0
    if os.path.getsize(pbigfile)>chunksize:
        targzflst = glob.glob('./UID*.tar.gz.*')
        targzflst.sort()
        for targzf in targzflst:
            print('removing existfile:'+targzf)
            os.remove(targzf)
        inputfile = open(pbigfile,'rb')  #open the pbigfile
        while True:
            chunk = inputfile.read(chunksize)
            if not chunk: 
                break
            partnum += 1
            filename = os.path.join(todir,(get2betw13(pbigfile,'/','.tar.gz')+'.%02d'%partnum))
            fileobj = open(filename,'wb')
            fileobj.write(chunk)
            fileobj.close()
		print('removing file:'+pbigfile)
        os.remove(pbigfile)
    else:
        print('file:'+pbigfile+' not be splited')
    return partnum
#targz_files('./UID*.csv')