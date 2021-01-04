--Oracle db========================================
select * from dba_directories;
select * from dba_users
create user query_ro identified by "********"

grant connect to query_ro;
grant select any table to query_ro;
grant create view to query_ro
--SELECT * FROM DBA_DIRECTORIES
create directory DMP_AUTOQUERY AS '/export_dir/autoquery'
grant execute on exportcsv_byviews to query_ro

--execute privilleges
grant execute on Your ENCODE procedure/function/package to query_ro;
grant execute on Your DECODE procedure/function/package to query_ro;
--==============================================

-- mysql==========================================
/*
MySQL - 5.7.24-log : Database - autoquery
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`autoquery` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `autoquery`;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

secure_file_priv=/export_dir/autoquery
log_bin_trust_function_creators=1

CREATE USER query_ro@ip IDENTIFIED BY '*********'
GRANT SELECT ON `Your db`.* TO 'query_ro'@'ip'
GRANT SELECT, CREATE VIEW, SHOW VIEW ON `autoquery`.* TO 'query_ro'@'ip'
GRANT FILE ON *.* TO 'query_ro'@'ip'
GRANT EXECUTE ON PROCEDURE `autoquery`.`exportcsv_byviews` TO 'query_ro'@'ip'
grant drop on autoquery.* to query_ro@ip #only used for replace view
-- =================================================

# os================================================
mkdir autoquery
useradd  -d /export_dir/autoquery autoquery
passwd autoquery  *********
chown autoqury:oinstall -R autoquery
# =================================================