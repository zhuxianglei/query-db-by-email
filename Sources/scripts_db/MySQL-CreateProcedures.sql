DELIMITER //
CREATE PROCEDURE exportcsv_forbigdata(powner VARCHAR(30),pviewnamelike VARCHAR(30))
/*
   Description: using for AutomaticQuerySystemBaseOnEMail
    Created by: xlzhu@ips.com
Creattion time: 20190304
    Updated by: 
  Updated time: 
*/
BEGIN
  DECLARE DONEO INT DEFAULT FALSE;
  DECLARE DONEI INT DEFAULT FALSE;
  DECLARE V_NAME1 VARCHAR(30);
  DECLARE V_NAME2 VARCHAR(30);
  DECLARE V_NAME3 VARCHAR(30);
  DECLARE V_TEMP  VARCHAR(254);
  DECLARE V_TMPPR  VARCHAR(254);
  DECLARE V_SQL1  VARCHAR(4000);
  DECLARE V_SQL2  VARCHAR(4000);
  DECLARE V_I     INT;
  DECLARE CUR_VIEW CURSOR FOR SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_NAME LIKE 'UID%V' AND TABLE_SCHEMA=powner AND TABLE_NAME LIKE pviewnamelike;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET DONEO=TRUE;

  #1.product view.csv file by schema&viewnamelike
  OPEN CUR_VIEW;
  WHILE NOT DONEO DO
    SET V_NAME1='';
    SET V_NAME2='';
    FETCH CUR_VIEW INTO V_NAME1,V_NAME2;
    IF NOT DONEO THEN
      BEGIN
        #1.1.create format data select sql
        DECLARE CUR_COLS CURSOR FOR SELECT column_name  FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=V_NAME1 AND TABLE_NAME = V_NAME2 ORDER BY ORDINAL_POSITION;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET DONEI=TRUE;
        SET V_SQL1='SELECT CONCAT_WS(''","'',CONCAT(''"''';
        SET V_I=1;
        SET DONEI=FALSE;
        SET V_TMPPR='';
        SET V_TEMP='';
        OPEN CUR_COLS;
        WHILE NOT DONEI DO
          FETCH CUR_COLS INTO V_NAME3;
          IF NOT DONEI THEN 
            SET V_TEMP= CONCAT(',IFNULL(',V_NAME3,','''')');
            IF V_I=1 THEN #the first column
              SET V_TEMP= CONCAT(V_TEMP,')');
            END IF;
          ELSE            #the last column
            SET V_TMPPR=CONCAT(',CONCAT(',REPLACE(V_TEMP,',IFNULL','IFNULL'),',''"''))');
          END IF;
          IF V_TMPPR IS NOT NULL OR V_TMPPR<>'' THEN
            SET V_SQL1=CONCAT(V_SQL1,V_TMPPR);
          END IF;
          SET V_I=V_I+1;
          SET V_TMPPR=V_TEMP;
        END WHILE;
        CLOSE CUR_COLS;
        SET V_SQL1=CONCAT(V_SQL1,' FROM ',V_NAME1,'.',V_NAME2);
        #1.1.end
      END;
      #1.2.create column name list select sql
	  IF @@character_set_database='utf8' THEN#for windows BOM
        SET V_SQL2= CONCAT('SELECT * FROM (SELECT CONCAT(''   "'',REPLACE(GROUP_CONCAT(column_name),'','',''","''),''"'')  FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=''',V_NAME1,'''',' AND table_name = ''',V_NAME2,'''  ORDER BY ORDINAL_POSITION) AS VC UNION ');
      ELSE
	    SET V_SQL2= CONCAT('SELECT * FROM (SELECT CONCAT(''"'',REPLACE(GROUP_CONCAT(column_name),'','',''","''),''"'')  FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=''',V_NAME1,'''',' AND table_name = ''',V_NAME2,'''  ORDER BY ORDINAL_POSITION) AS VC UNION ');
	  END IF;
	  #1.2end
      
      #1.3.put out data to csv
      SET @V_SQL=CONCAT(V_SQL2,V_SQL1,' INTO OUTFILE ''',@@secure_file_priv,REPLACE(V_NAME2,'V','.csv'),''' CHARACTER SET ',@@character_set_database,' LINES TERMINATED BY ''\\n''');     
      #SELECT @V_SQL;     
      PREPARE STMS FROM @V_SQL;
      EXECUTE STMS;
      #1.3end
    END IF;
  END WHILE;
  CLOSE CUR_VIEW;
  #1.end
  #2.drop views
  SET DONEO=FALSE;
  OPEN CUR_VIEW;
  WHILE NOT DONEO DO
    FETCH CUR_VIEW INTO V_NAME1,V_NAME2;
      IF NOT DONEO THEN
        SET @V_SQL= CONCAT('DROP VIEW ',V_NAME1,'.',V_NAME2);
        #select @V_SQL;
        PREPARE STMS FROM @V_SQL;
        EXECUTE STMS;
      END IF;
  END WHILE;
  CLOSE CUR_VIEW;
  #2.end
END//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE exportcsv_byviews(powner VARCHAR(30),pviewnamelike VARCHAR(30),pcostlimited INT,OUT prst VARCHAR(254))
/*
   Description: using for AutomaticQuerySystemBaseOnEMail
    Created by: xlzhu@ips.com
Creattion time: 20190305
    Updated by:
  Updated time:
*/
BEGIN
  DECLARE V_RST VARCHAR(254);
  #DECLARE CONTINUE HANDLER FOR SQLWARNING,SQLEXCEPTION SET V_RST=concat('MySQL DB Fun Exception:',mysql_error());
  CALL exportcsv_forbigdata(powner,pviewnamelike);
  SET prst='OK';
END//
DELIMITER ;



/*
#test codes
CALL exportcsv_byviews('audit','UID1234567891%',5000000,@msg);
SELECT @msg;

SELECT * FROM (SELECT CONCAT('"',REPLACE(GROUP_CONCAT(column_name),',','","'),'"')  FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='audit' AND table_name = 'UID1234567890_FILE1_1V'  ORDER BY ORDINAL_POSITION) AS VC UNION SELECT CONCAT_WS('","',CONCAT('"',IFNULL(id,'')),IFNULL(sn,''),IFNULL(sn2,''),CONCAT(IFNULL(descr,''),'"')) FROM audit.UID1234567890_FILE1_1V 
INTO OUTFILE '/u03/autoquery/UID1234567890_FILE1_1.csv' CHARACTER SET utf8 LINES TERMINATED BY '\n'
*/