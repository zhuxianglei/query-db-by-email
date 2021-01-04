--1.drop procedure byview
drop procedure exportcsv_byviews;
--2.create exportcsv** two
--3.grant execute byview to query_ro
grant execute on exportcsv_byviews to query_ro;
--3.check&drop directory   select * from dba_directories
drop directory DMP_AUTOQURY
--4.create direcotry
create directory DMP_AUTOQUERY AS '/?/autoquery'
