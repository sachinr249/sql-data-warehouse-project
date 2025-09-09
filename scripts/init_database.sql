/* 
==========================================
create database and schemas
==========================================
script purpose:
      This scrit create a new database "DataWarehouse" with schemas 'bronze','silver' and 'gold'
*/

use master;
go

--create the 'DataWarehouse' database
create database DataWarehouse ;
go

use DataWarehouse ;
go

-- create schemas
create schema bronze ;
go
create schema silver ;
go
create schema gold ;
go
