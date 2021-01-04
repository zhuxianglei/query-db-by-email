CREATE OR REPLACE PROCEDURE exportcsv_forbigdata(p_viewname varchar2,p_filename varchar2,p_maxlineperfile number default 0,p_dir varchar2 default 'DMPDIR',p_wnullfile boolean default false)
/*
   Description: export .csv file for bigdata query
                1.create view as select .... 
                  Attention!!!! please replace '',chr(10) chr(13) which exist in column!
                  for example:
                  select replace(replace(replace(replace(column,'"',"¡°'),',','£¬'),chr(10),''),chr(13),'') from table
                2.exec exportcsv_forbigdata(...);
                3.dowload file from server
    Created by: xlzhu
Creattion time: 201709
    Updated by: xlzhu add default parameter p_dir
  Updated time: 20181116
	Updated by: xlzhu add three space in V_COLNAMELST when charset=UTF8 and default parameter p_wnullfile(only write columnname title if without data)
  Updated time: 20181207
*/
 IS
  --p_maxlineperfile<=0 ,put all content to one file;
  V_FILE        UTL_FILE.FILE_TYPE;
  V_FILENO      NUMBER:=1;
  V_FILENAME    VARCHAR2(30);
  v_p_filename  VARCHAR2(30);
  V_FILECURLINE NUMBER:=0;
  V_CUR         INTEGER DEFAULT DBMS_SQL.OPEN_CURSOR;
  V_DESCT       DBMS_SQL.DESC_TAB;
  V_COLCNT      NUMBER;
  V_COLNAMELST  VARCHAR2(4000);
  V_COLVALUE    VARCHAR2(4000);
  V_STATUS      INTEGER;
  V_ROWS        NUMBER;
  V_UTF8        NUMBER(2);
BEGIN
  SELECT COUNT(*) INTO V_UTF8 FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER LIKE 'NLS_CHARACTERSET' AND VALUE LIKE '%UTF%';
  --1.producting filename
  select substr(p_filename,1,decode(instr(p_filename,'.'),0,length(p_filename),instr(p_filename,'.')-1)) into v_p_filename from dual;
  IF p_maxlineperfile>0 THEN
    V_FILENAME:=v_p_filename||'_'||TO_CHAR(V_FILENO)||'.csv';
  ELSE
    V_FILENAME:=v_p_filename||'.csv';
  END IF;
  dbms_output.put_line(V_FILENAME);
  --2.parse sql
  DBMS_SQL.PARSE(V_CUR, 'select * from '||p_viewname, DBMS_SQL.NATIVE);
  DBMS_SQL.DESCRIBE_COLUMNS(V_CUR, V_COLCNT, V_DESCT);

  --3.get column name list
  FOR I IN 1 .. V_COLCNT LOOP
    IF V_COLNAMELST IS NULL THEN
      IF V_UTF8>0 THEN
        V_COLNAMELST:='   "'||V_DESCT(I).COL_NAME || '"';--add three space which will be replaced by windows file BOM 20181207
      ELSE
        V_COLNAMELST:='"'||V_DESCT(I).COL_NAME || '"';--add three space which will be replaced by windows file BOM 20181207
      END IF;
    ELSE
      V_COLNAMELST:=V_COLNAMELST||',"'||V_DESCT(I).COL_NAME || '"';
    END IF;
    DBMS_SQL.DEFINE_COLUMN(V_CUR, I, V_COLVALUE,4000);
  END LOOP;
  dbms_output.put_line(V_COLNAMELST);
  --4.execute sql
  V_STATUS := DBMS_SQL.EXECUTE(V_CUR);
  dbms_output.put_line('ExecuteSQLStatus:'||to_char(V_STATUS));

  LOOP
    --5.first line
    V_ROWS:=DBMS_SQL.FETCH_ROWS(V_CUR);
    
	--6.if nodata whether write columnname only to file
    IF V_ROWS<=0 AND V_FILECURLINE = 0 AND p_wnullfile THEN
      V_FILE := UTL_FILE.FOPEN(p_dir, V_FILENAME, 'W',32767);
      UTL_FILE.PUT_LINE(V_FILE,V_COLNAMELST);
      UTL_FILE.FFLUSH(V_FILE);
      UTL_FILE.FCLOSE(V_FILE);
    END IF;
    --7.last row
    IF V_ROWS<=0 AND V_FILECURLINE>0 THEN
      dbms_output.put_line('FileLines:'||to_char(V_FILECURLINE));
      --8.close file;
      UTL_FILE.FFLUSH(V_FILE);
      UTL_FILE.FCLOSE(V_FILE);
    END IF;

    EXIT WHEN V_ROWS <= 0;

    IF V_FILECURLINE = 0 THEN
        V_FILE := UTL_FILE.FOPEN(p_dir, V_FILENAME, 'W',32767);
        UTL_FILE.PUT_LINE(V_FILE,V_COLNAMELST);
    ELSE
      UTL_FILE.NEW_LINE(V_FILE);
    END IF;
    --9.put line data to file;
    FOR I IN 1 .. V_COLCNT LOOP
      DBMS_SQL.COLUMN_VALUE(V_CUR, I, V_COLVALUE);
      --dbms_output.put_line(V_COLVALUE);
      UTL_FILE.PUT(V_FILE,'"'||V_COLVALUE||'"');
      IF I<V_COLCNT THEN
        UTL_FILE.PUT(V_FILE,',');
      END IF;
    END LOOP;
    V_FILECURLINE:=V_FILECURLINE+1;
    --10.close file,begin new file
    IF p_maxlineperfile>0 AND V_FILECURLINE >=p_maxlineperfile THEN
      dbms_output.put_line('FileLines:'||to_char(V_FILECURLINE));
      UTL_FILE.FFLUSH(V_FILE);
      UTL_FILE.FCLOSE(V_FILE);
      V_FILECURLINE:=0;
      V_FILENO:=V_FILENO+1;
      V_FILENAME:=v_p_filename||'_'||TO_CHAR(V_FILENO)||'.csv';
      dbms_output.put_line(V_FILENAME);
    END IF;
  END LOOP;
  --11.CLOSE CURSOR
  DBMS_SQL.CLOSE_CURSOR(V_CUR);
  dbms_output.put_line('Success!');
EXCEPTION
  WHEN OTHERS THEN
    UTL_FILE.fclose_all;
    RAISE;
END;
/
