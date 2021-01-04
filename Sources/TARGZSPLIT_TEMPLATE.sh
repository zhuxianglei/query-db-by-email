#!/bin/bash
#!!!!!!Forbidding modifying this script in windows!!!!!!
MAXB_SPLITFILE=52428800
ulp='UIDLIST_BYCOMMA'

for f1 in `find ./ -name UID\*csv|cut -d_ -f1|cut -d/ -f2|sort -u`
do
  fu=`echo $f1|sed s/UID//g`
  cnt=`echo $ulp|grep $fu|wc -l`
  if [ $cnt -ge 1 ] ; then
    rm -rf $f1.tar*
    tar -cf $f1.tar $f1*csv
    gzip $f1.tar
    rm -rf $f1*csv
  fi
done

for f1 in `find ./ -name UID\*.tar.gz|cut -d/ -f2`
do
  sz=`ls -l $f1|awk '{print$5}'`
  if [ $sz -ge $MAXB_SPLITFILE ] ; then
    split -b $MAXB_SPLITFILE -a 2 $f1 $f1.
    rm -rf $f1
  fi
done
