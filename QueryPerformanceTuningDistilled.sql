/* SQL Server 2012 Query Performance Tuning Distilled 
Code Listing */


/* Each chapter starts with a chapter head. Then specified 
code within the chapter will refer to a comment associated 
with the code from that chapter. Most of the specifications 
are unique, but some might not be. Be sure you're looking 
for the correct chapter first.*/


--Chapter 2

SELECT  dopc.cntr_value,
        dopc.cntr_type
FROM    sys.dm_os_performance_counters AS dopc
WHERE   dopc.object_name = 'MSSQL$RANDORI:General Statistics'
        AND dopc.counter_name = 'Logins/sec' ;


SELECT  *
FROM    sys.dm_os_performance_counters




SELECT TOP (10)
        dows.*
FROM    sys.dm_os_wait_stats AS dows
ORDER BY dows.wait_time_ms DESC ;


EXEC sp_configure 
    'show advanced options',
    1 ;
GO
RECONFIGURE ;
GO
EXEC sp_configure 
    'min server memory' ; 
EXEC sp_configure 
    'max server memory' ;




USE master ;
EXEC sp_configure 
    'show advanced option',
    1 ;
RECONFIGURE ;
EXEC sp_configure 
    'min server memory (MB)',
    128 ;
EXEC sp_configure 
    'max server memory (MB)',
    200 ;
RECONFIGURE WITH OVERRIDE ;



--File Group
ALTER DATABASE AdventureWorks2008R2 ADD FILEGROUP INDEXES ;
ALTER DATABASE AdventureWorks2008R2 ADD FILE (NAME = AdventureWorks2008_Data2j,
FILENAME =  'C:\DATA\AdventureWorks2008_2.ndf',
SIZE = 1mb,
FILEGROWTH = 10%) TO FILEGROUP Indexes ;




--Example
SELECT  jc.JobCandidateID,
        e.ModifiedDate
FROM    HumanResources.JobCandidate AS jc
INNER JOIN HumanResources.Employee AS e
        ON jc.BusinessEntityID = e.BusinessEntityID ;


--FileMove
USE master ;
GO
sp_detach_db 
    'AdventureWorks2008' ;
GO


USE master ;
GO
sp_attach_db 
    'AdventureWorks2008R2',
    'C:\DATA\AdventureWorks2008.mdf',
    'F:\DATA\AdventureWorks2008_2.ndf ',
    'C:\DATA\AdventureWorks2008.IDf ' ;
GO

USE Adventureworks2008R2
GO
SELECT  *
FROM    sys.database_files ;
GO


--example index
CREATE INDEX IndexBirthDate 
ON HumanResources.Employee (BirthDate) 
ON Indexes ;





--Chapter 3
SELECT  dxs.name,
        dxs.create_time
FROM    sys.dm_xe_sessions AS dxs ;






SELECT  *
FROM    sys.fn_xe_file_target_read_file('C:\Program Files\Microsoft SQL Server\MSSQL11.RANDORI\MSSQL\Log\Query Performance Tuning*.xel',
                                        NULL, NULL, NULL) ;




WITH    xEvents
          AS (SELECT    object_name AS xEventName,
                        CAST (event_data AS XML) AS xEventData
              FROM      sys.fn_xe_file_target_read_file('C:\Program Files\Microsoft SQL Server\MSSQL11.RANDORI\MSSQL\Log\Query Performance Tuning*.xel',
                                                        NULL, NULL, NULL)
             )
    SELECT  xEventName,
            xEventData.value('(/event/data[@name=''duration'']/value)[1]',
                             'bigint') Duration,
            xEventData.value('(/event/data[@name=''physical_reads'']/value)[1]',
                             'bigint') PhysicalReads,
            xEventData.value('(/event/data[@name=''logical_reads'']/value)[1]',
                             'bigint') LogicalReads,
            xEventData.value('(/event/data[@name=''cpu_time'']/value)[1]',
                             'bigint') CpuTime,
            xEventData.value('(/event/data[@name=''batch_text'']/value)[1]',
                             'varchar(max)') BatchText,
            xEventData.value('(/event/data[@name=''statement'']/value)[1]',
                             'varchar(max)') StatementText,
            xEventData.value('(/event/data[@name=''query_plan_hash'']/value)[1]',
                             'binary(8)') QueryPlanHash
    FROM    xEvents
    ORDER BY LogicalReads DESC ;




USE TestDB ;
GO
WITH    xEvents
          AS (SELECT    object_name AS xEventName,
                        CAST (event_data AS XML) AS xEventData
              FROM      sys.fn_xe_file_target_read_file('C:\Program Files\Microsoft SQL Server\MSSQL11.RANDORI\MSSQL\Log\Query Performance Tuning*.xel',
                                                        NULL, NULL, NULL)
             )
    SELECT  xEventName,
            xEventData.value('(/event/data[@name=''duration'']/value)[1]',
                             'bigint') Duration,
            xEventData.value('(/event/data[@name=''physical_reads'']/value)[1]',
                             'bigint') PhysicalReads,
            xEventData.value('(/event/data[@name=''logical_reads'']/value)[1]',
                             'bigint') LogicalReads,
            xEventData.value('(/event/data[@name=''cpu_time'']/value)[1]',
                             'bigint') CpuTime,
            CASE xEventName
              WHEN 'sql_batch_completed'
              THEN xEventData.value('(/event/data[@name=''batch_text'']/value)[1]',
                                    'varchar(max)')
              WHEN 'rpc_completed'
              THEN xEventData.value('(/event/data[@name=''statement'']/value)[1]',
                                    'varchar(max)')
            END AS SQLText,
            xEventData.value('(/event/data[@name=''query_plan_hash'']/value)[1]',
                             'binary(8)') QueryPlanHash
--INTO Session_Table
    FROM    xEvents ;




USE TestDB ;
GO
SELECT  COUNT(*) AS TotalExecutions,
        st.xEventName,
        st.BatchText,
        SUM(st.Duration) AS DurationTotal,
        SUM(st.CpuTime) AS CpuTotal,
        SUM(st.LogicalReads) AS LogicalReadTotal,
        SUM(st.PhysicalReads) AS PhysicalReadTotal
FROM    Session_Table AS st
GROUP BY st.xEventName,
        st.BatchText
ORDER BY LogicalReadTotal DESC ;





SELECT  ss.sum_execution_count,
        t.TEXT,
        ss.sum_total_elapsed_time,
        ss.sum_total_worker_time,
        ss.sum_total_logical_reads,
        ss.sum_total_logical_writes
FROM    (SELECT s.plan_handle,
                SUM(s.execution_count) sum_execution_count,
                SUM(s.total_elapsed_time) sum_total_elapsed_time,
                SUM(s.total_worker_time) sum_total_worker_time,
                SUM(s.total_logical_reads) sum_total_logical_reads,
                SUM(s.total_logical_writes) sum_total_logical_writes
         FROM   sys.dm_exec_query_stats s
         GROUP BY s.plan_handle
        ) AS ss
CROSS APPLY sys.dm_exec_sql_text(ss.plan_handle) t
ORDER BY sum_total_logical_reads DESC



WITH    xEvents
          AS (SELECT    object_name AS xEventName,
                        CAST (event_data AS XML) AS xEventData
              FROM      sys.fn_xe_file_target_read_file('C:\Program Files\Microsoft SQL Server\MSSQL11.RANDORI\MSSQL\Log\Query Performance Tuning*.xel',
                                                        NULL, NULL, NULL)
             )
    SELECT  xEventName,
            xEventData.value('(/event/data[@name=''duration'']/value)[1]',
                             'bigint') Duration,
            xEventData.value('(/event/data[@name=''physical_reads'']/value)[1]',
                             'bigint') PhysicalReads,
            xEventData.value('(/event/data[@name=''logical_reads'']/value)[1]',
                             'bigint') LogicalReads,
            xEventData.value('(/event/data[@name=''cpu_time'']/value)[1]',
                             'bigint') CpuTime,
            xEventData.value('(/event/data[@name=''batch_text'']/value)[1]',
                             'varchar(max)') BatchText,
            xEventData.value('(/event/data[@name=''query_plan_hash'']/value)[1]',
                             'binary(8)') QueryPlanHash
    FROM    xEvents
    ORDER BY Duration DESC ;


USE AdventureWorks2008R2 ;
GO


--set showplan_xml
SET SHOWPLAN_XML ON
GO
SELECT  soh.AccountNumber,
        sod.LineTotal,
        sod.OrderQty,
        sod.UnitPrice,
        p.Name
FROM    Sales.SalesOrderHeader soh
JOIN    Sales.SalesOrderDetail sod
        ON soh.SalesOrderID = sod.SalesOrderID
JOIN    Production.Product p
        ON sod.ProductID = p.ProductID
WHERE   sod.LineTotal > 1000 ;

GO
SET SHOWPLAN_XML OFF
GO







SELECT  soh.AccountNumber,
        sod.LineTotal,
        sod.OrderQty,
        sod.UnitPrice,
        p.Name
FROM    Sales.SalesOrderHeader soh
JOIN    Sales.SalesOrderDetail sod
        ON soh.SalesOrderID = sod.SalesOrderID
JOIN    Production.Product p
        ON sod.ProductID = p.ProductID
WHERE   sod.LineTotal > 20000 ;







--hash
SELECT  p.*
FROM    Production.Product p
JOIN    Production.ProductCategory pc
        ON p.ProductSubcategoryID = pc.ProductCategoryID ;
	





--merge

SELECT  pm.*
FROM    Production.ProductModel pm
JOIN    Production.ProductModelProductDescriptionCulture pmpd
        ON pm.ProductModelID = pmpd.ProductModelID ;
        
  
  
  

--loop
SELECT  soh.*
FROM    Sales.SalesOrderHeader soh
JOIN    Sales.SalesOrderDetail sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.SalesOrderID = 71832 ;




--example proc
IF (SELECT  OBJECT_ID('p1')
   ) IS NOT NULL 
    DROP PROC p1 
GO
CREATE PROC p1
AS 
CREATE TABLE dbo.Test1 (C1 INT) ;
INSERT  INTO dbo.Test1
        SELECT  ProductID
        FROM    Production.Product ;
SELECT  *
FROM    dbo.Test1 ;
DROP TABLE dbo.Test1 ; 
GO


SET SHOWPLAN_XML ON
GO
EXEC p1 ;
GO
SET SHOWPLAN_XML OFF
GO



SET STATISTICS XML ON
GO
EXEC p1 ;
GO
SET STATISTICS XML OFF
GO



--plan cache
SELECT  p.query_plan,
        t.text
FROM    sys.dm_exec_cached_plans r
CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) p
CROSS APPLY sys.dm_exec_sql_text(r.plan_handle) t ;



              



SELECT TOP 100
        p.*
FROM    Production.Product p ;






SET STATISTICS TIME ON
GO
SELECT  soh.AccountNumber,
        sod.LineTotal,
        sod.OrderQty,
        sod.UnitPrice,
        p.Name
FROM    Sales.SalesOrderHeader soh
JOIN    Sales.SalesOrderDetail sod
        ON soh.SalesOrderID = sod.SalesOrderID
JOIN    Production.Product p
        ON sod.ProductID = p.ProductID
WHERE   sod.LineTotal > 1000 ; 
GO
SET STATISTICS TIME OFF 
GO


DBCC freeproccache()




SET STATISTICS IO ON
GO
SELECT  soh.AccountNumber,
        sod.LineTotal,
        sod.OrderQty,
        sod.UnitPrice,
        p.Name
FROM    Sales.SalesOrderHeader soh
JOIN    Sales.SalesOrderDetail sod
        ON soh.SalesOrderID = sod.SalesOrderID
JOIN    Production.Product p
        ON sod.ProductID = p.ProductID
WHERE   sod.SalesOrderID = 71856 ; 
GO
SET STATISTICS IO OFF 
GO






--Chapter 4

SELECT TOP 10
        p.ProductID,
        p.[Name],
        p.StandardCost,
        p.[Weight],
        ROW_NUMBER() OVER (ORDER BY p.Name DESC) AS RowNumber
FROM    Production.Product p ;



SELECT TOP 10
        p.ProductID,
        p.[Name],
        p.StandardCost,
        p.[Weight],
        ROW_NUMBER() OVER (ORDER BY p.Name DESC) AS RowNumber
FROM    Production.Product p
WHERE   p.StandardCost > 150
ORDER BY p.StandardCost ;





--index test
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO
CREATE TABLE dbo.Test1
    (C1 INT,
     C2 INT,
     C3 VARCHAR(50)
    ) ; 

WITH    Nums
          AS (SELECT TOP (10000)
                        ROW_NUMBER() OVER (ORDER BY (SELECT 1
                                                    )) AS n
              FROM      Master.sys.All_Columns aC1
              CROSS JOIN Master.sys.ALL_Columns aC2
             )
    INSERT  INTO dbo.Test1
            (C1, C2, C3)
            SELECT  n,
                    n,
                    'C3'
            FROM    Nums ;



UPDATE  dbo.Test1
SET     C1 = 1,
        C2 = 1
WHERE   C2 = 1 ;


CREATE CLUSTERED INDEX iTest 
ON dbo.Test1(C1) ;



CREATE INDEX iTest2 
ON dbo.Test1(C2) ;




SELECT  p.ProductID,
        p.Name,
        p.StandardCost,
        p.Weight
FROM    Production.Product p ;





SELECT  p.ProductID,
        p.Name,
        p.StandardCost,
        p.Weight
FROM    Production.Product AS p
WHERE   p.ProductID = 738 ;



--Page Size

IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO
CREATE TABLE dbo.Test1 (C1 INT, C2 INT) ; 

WITH    Nums
          AS (SELECT    1 AS n
              UNION ALL
              SELECT    n + 1
              FROM      Nums
              WHERE     n < 20
             )
    INSERT  INTO dbo.Test1
            (C1, C2)
            SELECT  n,
                    2
            FROM    Nums ;

CREATE INDEX iTest ON dbo.Test1(C1) ;



--check the page size

SELECT  i.Name,
        i.type_desc,
        ddips.page_count,
        ddips.record_count,
        ddips.index_level
FROM    sys.indexes i
JOIN    sys.dm_db_index_physical_stats(DB_ID(N'AdventureWorks2008R2'),
                                       OBJECT_ID(N'dbo.Test1'), NULL, NULL,
                                       'DETAILED') AS ddips
        ON i.index_id = ddips.index_id
WHERE   i.object_id = OBJECT_ID(N'dbo.Test1') ;



-- modify the  structure
DROP INDEX dbo.Test1.iTest ;
ALTER TABLE dbo.Test1 ALTER COLUMN C1 CHAR(500) ;
CREATE INDEX iTest ON dbo.Test1(C1) ;





DROP TABLE dbo.t1Test1 ;




--Determining Selectivity

SELECT  COUNT(DISTINCT e.Gender) AS DistinctColValues,
        COUNT(e.Gender) AS NumberOfRows,
        (CAST(COUNT(DISTINCT e.Gender) AS DECIMAL)
         / CAST(COUNT(e.Gender) AS DECIMAL)) AS Selectivity
FROM    HumanResources.Employee AS e ;



--Selectivity example

SELECT  e.*
FROM    HumanResources.Employee AS e
WHERE   e.Gender = 'F'
        AND e.SickLeaveHours = 59
        AND e.MaritalStatus = 'M' ;



--Selectivity Index

CREATE INDEX IX_Employee_Test ON HumanResources.Employee (Gender) ;



-- Selectivity Index 2

CREATE INDEX IX_Employee_Test ON
HumanResources.Employee (SickLeaveHours,  Gender,  MaritalStatus)
WITH  (DROP_EXISTING = ON) ;


-- Original index
CREATE INDEX IX_Employee_Test ON HumanResources.Employee (Gender)
WITH (DROP_EXISTING = ON) ;



--Selectivity query with hint

SELECT  e.*
FROM    HumanResources.Employee AS e WITH (INDEX (IX_Employee_Test))
WHERE   e.SickLeaveHours = 59
        AND e.Gender = 'F'
        AND e.MaritalStatus = 'M' ;






SELECT  e.*
FROM    HumanResources.Employee AS e WITH (FORCESEEK)
WHERE   e.SickLeaveHours = 59
        AND e.Gender = 'F'
        AND e.MaritalStatus = 'M' ;


--cleanup
DROP INDEX HumanResources.Employee.IX_Employee_Test ;
     
  
  
  
  
  --Column Order
  
  --Sample index

CREATE INDEX IX_Test ON Person.Address (City,  PostalCode) ;
              

--query using the leading edge
SELECT  a.*
FROM    Person.Address AS a
WHERE   a.City = 'Warrington' ;
          	
   
--query using a different column   
SELECT  a.*
FROM    Person.Address AS a
WHERE   a.PostalCode = 'WA3 7BH' ;


--Query using only index columns
SELECT  a.AddressID,
        a.City,
        a.PostalCode
FROM    Person.Address AS a
WHERE   a.City = 'Warrington'
        AND a.PostalCode = 'WA3 7BH' ;
  
  
  
 
 --clustered and non-clustered indexes
SELECT  dl.DatabaseLogID,
        dl.PostTime
FROM    dbo.DatabaseLog AS dl
WHERE   dl.DatabaseLogID = 115 ;





SELECT  d.DepartmentID,
        d.ModifiedDate
FROM    HumanResources.Department AS d
WHERE   d.DepartmentID = 10 ;



--clustered index width

IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO
CREATE TABLE dbo.Test1 (C1 INT, C2 INT) ; 

WITH    Nums
          AS (SELECT TOP (20)
                        ROW_NUMBER() OVER (ORDER BY (SELECT 1
                                                    )) AS n
              FROM      Master.sys.All_Columns aC1
              CROSS JOIN Master.sys.ALL_Columns aC2
             )
    INSERT  INTO dbo.Test1
            (C1, C2)
            SELECT  n,
                    n + 1
            FROM    Nums ;
            
CREATE CLUSTERED INDEX iClustered 
ON dbo.Test1  (C2) ; 

CREATE NONCLUSTERED INDEX iNonClustered 
ON dbo.Test1  (C1) ;



SELECT  i.name,
        i.type_desc,
        s.page_count,
        s.record_count,
        s.index_level
FROM    sys.indexes i
JOIN    sys.dm_db_index_physical_stats(DB_ID(N'AdventureWorks2008R2'),
                                       OBJECT_ID(N'dbo.Test1'), NULL, NULL,
                                       'DETAILED') AS s
        ON i.index_id = s.index_id
WHERE   i.object_id = OBJECT_ID(N'dbo.Test1') ;



DROP INDEX dbo.Test1.iClustered ;
ALTER TABLE dbo.Test1 ALTER COLUMN C2 CHAR(500) ;
CREATE CLUSTERED INDEX iClustered ON dbo.Test1(C2) ;



--Create Sort

IF (SELECT  OBJECT_ID('od')
   ) IS NOT NULL 
    DROP TABLE dbo.od ;
 GO
SELECT  pod.*
INTO    dbo.od
FROM    Purchasing.PurchaseOrderDetail AS pod ;



EXEC sp_helpindex 
    'dbo.od' ;



SELECT  od.*
FROM    dbo.od
WHERE   od.ProductID BETWEEN 500 AND 510
ORDER BY od.ProductID ;




CREATE CLUSTERED INDEX i1 ON od(ProductID) ;


DROP INDEX od.i1 ;
CREATE NONCLUSTERED INDEX i1 ON dbo.od(ProductID) ;



DROP TABLE dbo.od ;



--updates on clustered index
BEGIN TRAN
SET STATISTICS IO ON ;
UPDATE  Sales.SpecialOfferProduct
SET     ProductID = 345
WHERE   SpecialOfferID = 1
        AND ProductID = 720 ;
SET STATISTICS IO OFF ;
ROLLBACK TRAN



CREATE NONCLUSTERED INDEX ixTest 
ON Sales.SpecialOfferProduct (ModifiedDate) ;




DROP INDEX Sales.SpecialOfferProduct.ixTest ;


--Clustered index benefits
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ;
GO
CREATE TABLE dbo.Test1 (C1 INT, C2 INT) ;

WITH    Nums
          AS (SELECT TOP (10000)
                        ROW_NUMBER() OVER (ORDER BY (SELECT 1
                                                    )) AS n
              FROM      Master.sys.all_columns AS aC1
              CROSS JOIN Master.sys.all_columns AS aC2
             )
    INSERT  INTO dbo.Test1
            (C1, C2)
            SELECT  n,
                    2
            FROM    Nums ;
      


SELECT  C1,
        C2
FROM    dbo.Test1 AS t
WHERE   C1 = 1000 ;



CREATE NONCLUSTERED INDEX incl ON dbo.Test1(C1) ;

     

CREATE CLUSTERED INDEX icl ON dbo.Test1(C1) ;






--nonclustered index benefits
SELECT  cc.CreditCardID,
        cc.CardNumber,
        cc.ExpMonth,
        cc.ExpYear
FROM    Sales.CreditCard cc
WHERE   cc.ExpMonth BETWEEN 6 AND 9
        AND cc.ExpYear = 2008
ORDER BY cc.ExpMonth ;
   	  

CREATE NONCLUSTERED INDEX ixTest 
ON Sales.CreditCard (ExpMonth, ExpYear)
INCLUDE (CardNumber) ;


DROP INDEX Sales.CreditCard.ixTest ;



--covering indexes
SELECT  a.PostalCode
FROM    Person.Address AS a
WHERE   a.StateProvinceID = 42 ;




CREATE NONCLUSTERED INDEX [IX_Address_StateProvinceID] 
ON [Person].[Address]  ([StateProvinceID] ASC)
INCLUDE  (PostalCode)
WITH (
DROP_EXISTING = ON) ;



--Index intersectoin:

SELECT  soh.*
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.SalesPersonID = 276
        AND soh.OrderDate BETWEEN '4/1/2005' AND '7/1/2005' ;


CREATE NONCLUSTERED INDEX IX_Test 
ON Sales.SalesOrderHeader (OrderDate) ;



DROP INDEX Sales.SalesOrderHeader.IX_Test ;



--Index Join
SELECT  soh.SalesPersonID,
        soh.OrderDate
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.SalesPersonID = 276
        AND soh.OrderDate BETWEEN '4/1/2005' AND '7/1/2005' ;
  



 
CREATE NONCLUSTERED INDEX IX_Test 
ON Sales.SalesOrderHeader ([OrderDate] ASC) ;          
        




DROP INDEX Sales.SalesOrderHeader.IX_Test




--Filtered index

SELECT  soh.PurchaseOrderNumber,
        soh.OrderDate,
        soh.ShipDate,
        soh.SalesPersonID
FROM    Sales.SalesOrderHeader AS soh
WHERE   PurchaseOrderNumber LIKE 'PO5%'
        AND soh.SalesPersonID IS NOT NULL ;


CREATE NONCLUSTERED INDEX IX_Test 
ON Sales.SalesOrderHeader(PurchaseOrderNumber ,SalesPersonID)
INCLUDE  (OrderDate,ShipDate) ;



CREATE NONCLUSTERED INDEX IX_Test 
ON Sales.SalesOrderHeader(PurchaseOrderNumber,SalesPersonID) 
INCLUDE  (OrderDate,ShipDate) 
WHERE PurchaseOrderNumber IS NOT NULL AND SalesPersonID IS NOT NULL 
WITH  (DROP_EXISTING = ON) ;



DROP INDEX Sales.SalesOrderHeader.IX_Test ;




--Indexed views
--Query 1
SELECT  p.[Name] AS ProductName,
        SUM(pod.OrderQty) AS OrderOty,
        SUM(pod.ReceivedQty) AS ReceivedOty,
        SUM(pod.RejectedQty) AS RejectedOty
FROM    Purchasing.PurchaseOrderDetail AS pod
JOIN    Production.Product AS p
        ON p.ProductID = pod.ProductID
GROUP BY p.[Name] ;


--Query 2
SELECT  p.[Name] AS ProductName,
        SUM(pod.OrderQty) AS OrderOty,
        SUM(pod.ReceivedQty) AS ReceivedOty,
        SUM(pod.RejectedQty) AS RejectedOty
FROM    Purchasing.PurchaseOrderDetail AS pod
JOIN    Production.Product AS p
        ON p.ProductID = pod.ProductID
GROUP BY p.[Name]
HAVING  (SUM(pod.RejectedQty) / SUM(pod.ReceivedQty)) > .08 ;


--Query 3
SELECT  p.[Name] AS ProductName,
        SUM(pod.OrderQty) AS OrderQty,
        SUM(pod.ReceivedQty) AS ReceivedQty,
        SUM(pod.RejectedQty) AS RejectedQty
FROM    Purchasing.PurchaseOrderDetail AS pod
JOIN    Production.Product AS p
        ON p.ProductID = pod.ProductID
WHERE   p.[Name] LIKE 'Chain%'
GROUP BY p.[Name] ;



--view
IF EXISTS ( SELECT  *
            FROM    sys.views
            WHERE   object_id = OBJECT_ID(N'[Purchasing].[IndexedView]') ) 
    DROP VIEW [Purchasing].[IndexedView] ;
GO
CREATE VIEW Purchasing.IndexedView
WITH SCHEMABINDING
AS
SELECT  pod.ProductID,
        SUM(pod.OrderQty) AS OrderQty,
        SUM(pod.ReceivedQty) AS ReceivedQty,
        SUM(pod.RejectedQty) AS RejectedQty,
        COUNT_BIG(*) AS [Count]
FROM    Purchasing.PurchaseOrderDetail AS pod
GROUP BY pod.ProductID ;
GO
CREATE UNIQUE CLUSTERED INDEX iv 
ON Purchasing.IndexedView (ProductID) ; 
GO


SELECT  iv.ProductID,
        iv.ReceivedQty,
        iv.RejectedQty
FROM    Purchasing.IndexedView AS iv ;




--Index Compression

CREATE NONCLUSTERED INDEX IX_Test
ON Person.Address(City ASC, PostalCode ASC) ;



CREATE NONCLUSTERED INDEX IX_Comp_Test 
ON Person.Address (City,PostalCode) 
WITH (DATA_COMPRESSION = ROW ) ;




CREATE NONCLUSTERED INDEX IX_Comp_Page_Test 
ON Person.Address  (City,PostalCode) 
WITH (DATA_COMPRESSION = PAGE) ;





SELECT  i.Name,
        i.type_desc,
        s.page_count,
        s.record_count,
        s.index_level,
        compressed_page_count
FROM    sys.indexes i
JOIN    sys.dm_db_index_physical_stats(DB_ID(N'AdventureWorks2008R2'),
                                       OBJECT_ID(N'Person.Address'), NULL,
                                       NULL, 'DETAILED') AS s
        ON i.index_id = s.index_id
WHERE   i.OBJECT_ID = OBJECT_ID(N'Person.Address') ;




SELECT  a.City,
        a.PostalCode
FROM    Person.Address AS a
WHERE   a.City = 'Newton'
        AND a.PostalCode = 'V2M1N7' ;




SELECT  a.City,
        a.PostalCode
FROM    Person.Address AS a WITH (INDEX = IX_Test)
WHERE   a.City = 'Newton'
        AND a.PostalCode = 'V2M1N7' ;




DROP INDEX Person.Address.IX_Test ; 
DROP INDEX Person.Address.IX_Comp_Test ; 
DROP INDEX Person.Address.IX_Comp_Page_Test ;


-- Columnstore Index

SELECT  tha.ProductID,
        COUNT(tha.ProductID) AS CountProductID,
        SUM(tha.Quantity) AS SumQuantity,
        AVG(tha.ActualCost) AS AvgActualCost
FROM    Production.TransactionHistoryArchive AS tha
GROUP BY tha.ProductID ;



CREATE NONCLUSTERED COLUMNSTORE INDEX ix_csTest
ON Production.TransactionHistoryArchive
(ProductID,
Quantity,
ActualCost) ;


--Chapter 5

SELECT  soh.DueDate,
        soh.CustomerID,
        soh.Status
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.DueDate BETWEEN '1/1/2008' AND '2/1/2008' ;




CREATE PROCEDURE dbo.uspProductSize
AS
SELECT  p.ProductID, 
		p.Size
FROM	Production.Product AS p
WHERE	p.Size = '62';






--Chapter 6

SELECT  p.[Name],
        AVG(sod.LineTotal)
FROM    Sales.SalesOrderDetail AS sod
JOIN    Production.Product p
        ON sod.ProductID = p.ProductID
WHERE   sod.ProductID = 776
GROUP BY sod.CarrierTrackingNumber,
        p.[Name]
HAVING  MAX(sod.OrderQty) > 1
ORDER BY MIN(sod.LineTotal) ;





SELECT  *
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.ProductID = 776 ;



SELECT  *
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.ProductID = 793 ;




SELECT  *
FROM    Sales.SalesOrderDetail AS sod WITH (INDEX (IX_SalesOrderDetail_ProductID))
WHERE   sod.ProductID = 793 ;





SELECT  NationalIDNumber,
        JobTit1e,
        HireDate
FROM    HumanResources.Employee AS e
WHERE   e.NationalIDNumber = '693168613' ;





CREATE UNIQUE NONCLUSTERED INDEX [AK_Employee_NationalIDNumber] ON
[HumanResources].[Employee]
(NationalIDNumber ASC,  
JobTitle ASC,  
HireDate ASC )
WITH DROP_EXISTING ;




CREATE UNIQUE NONCLUSTERED INDEX [AK_Employee_NationalIDNumber]
ON [HumanResources].[Employee]
(NationalIDNumber ASC )  
INCLUDE  (JobTit1e,HireDate) 
WITH DROP_EXISTING ;




SELECT  NationalIDNumber,
        BusinessEntityID
FROM    HumanResources.Employee AS e
WHERE   e.NationalIDNumber BETWEEN '693168613'
                           AND     '7000000000' ;


CREATE UNIQUE NONCLUSTERED INDEX [AK_Employee_NationalIDNumber]
ON [HumanResources].[Employee]
(
[NationalIDNumber] ASC 
)WITH DROP_EXISTING ;



DBCC SHOW_STATISTICS('HumanResources.Employee', 
AK_Employee_NationalIDNumber) ;





SELECT  poh.PurchaseOrderID,
        poh.VendorID,
        poh.OrderDate
FROM    Purchasing.PurchaseOrderHeader AS poh
WHERE   VendorID = 1636
        AND poh.OrderDate = '12/5/2007' ;
  
 
 
 
CREATE NONCLUSTERED INDEX Ix_TEST 
ON Purchasing.PurchaseOrderHeader(OrderDate) ;
  
          


--Chapter 7

--statstest
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO
CREATE TABLE dbo.Test1 (C1 INT, C2 INT IDENTITY) ;

SELECT TOP 1500
        IDENTITY( INT,1,1 ) AS n
INTO    #Nums
FROM    Master.dbo.SysColumns sC1,
        Master.dbo.SysColumns sC2 ;
        
INSERT  INTO dbo.Test1
        (C1)
        SELECT  n
        FROM    #Nums
        
DROP TABLE #Nums

CREATE NONCLUSTERED INDEX i1 ON dbo.Test1 (C1) ;






SELECT  *
FROM    dbo.Test1
WHERE   C1 = 2 ;
 --Retrieve 1 row



INSERT  INTO dbo.Test1
        (C1)
VALUES  (2) ;


--addrows
SELECT TOP 1500
        IDENTITY( INT,1,1 ) AS n
INTO    #Nums
FROM    Master.dbo.SysColumns scl,
        Master.dbo.SysColumns sC2 ;
INSERT  INTO dbo.Test1
        (C1)
        SELECT  2
        FROM    #Nums ;
DROP TABLE #Nums ;




SELECT  *
FROM    dbo.Test1
WHERE   C1 = 2 ;




ALTER DATABASE AdventureWorks2008R2 SET AUTO_UPDATE_STATISTICS OFF ;


ALTER DATABASE AdventureWorks2008R2 SET AUTO_UPDATE_STATISTICS ON ;





--Create first table with 10001 rows IF(SELECT 0BJECT_ID('dbo.Test1'))  IS NOT NULL
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO

CREATE TABLE dbo.Test1
    (Test1_C1 INT IDENTITY,
     Test1_C2 INT
    ) ;
    
INSERT  INTO dbo.Test1
        (Test1_C2)
VALUES  (1) ;

SELECT TOP 10000
        IDENTITY( INT,1,1 ) AS n
INTO    #Nums
FROM    Master.dbo.SysColumns scl,
        Master.dbo.SysColumns sC2 ;
        
INSERT  INTO dbo.Test1
        (Test1_C2)
        SELECT  2
        FROM    #Nums 
GO 

CREATE CLUSTERED INDEX i1 ON dbo.Test1(Test1_C1)

--Create second table with 10001 rows, -- but opposite data distribution IF(SELECT 0BJECT_ID('dbo.Test2')) IS NOT NULL
IF (SELECT  OBJECT_ID('dbo.Test2')
   ) IS NOT NULL 
    DROP TABLE dbo.Test2 ; 
GO

CREATE TABLE dbo.Test2
    (Test2_C1 INT IDENTITY,
     Test2_C2 INT
    ) ;
    
INSERT  INTO dbo.Test2
        (Test2_C2)
VALUES  (2) ;

INSERT  INTO dbo.Test2
        (Test2_C2)
        SELECT  1
        FROM    #Nums ;
DROP TABLE #Nums ; 
GO 

CREATE CLUSTERED INDEX il ON dbo.Test2(Test2_C1) ;





SELECT  DATABASEPROPERTYEX('AdventureWorks2008R2', 'IsAutoCreateStatistics') ;




--nonindexedselect
SELECT  Test1.Test1_C2,
        Test2.Test2_C2
FROM    dbo.Test1
JOIN    dbo.Test2
        ON Test1.Test1_C2 = Test2.Test2_C2
WHERE   Test1.Test1_C2 = 2 ;






SELECT  *
FROM    sys.stats
WHERE   object_id = OBJECT_ID('Test2') ;




SELECT  dbo.Test1.Test1_C2,
        Test2.Test2_C2
FROM    dbo.Test1
JOIN    dbo.Test2
        ON dbo.Test1.Test1_C2 = Test2.Test2_C2
WHERE   dbo.Test1.Test1_C2 = 1 ;





SELECT  *
FROM    sys.stats_columns
WHERE   object_id = OBJECT_ID('dbo.Test1') ;



DROP STATISTICS [dbo.Test1]._WA_Sys_00000002_269AB60B ;


SELECT  *
FROM    sys.stats
WHERE   object_id = OBJECT_ID('dbo.Test1') ;


ALTER DATABASE AdventureWorks2008R2 SET AUTO_CREATE_STATISTICS OFF;



SELECT  Test1.Test1_C2,
        Test2.Test2_C2
FROM    dbo.Test1
JOIN    dbo.Test2
        ON Test1.Test1_C2 = Test2.Test2_C2
WHERE   Test1.Test1_C2 = 2 ;



SELECT  Test1.Test1_C2,
        Test2.Test2_C2
FROM    dbo.Test2
JOIN    dbo.Test1
        ON Test1.Test1_C2 = Test2.Test2_C2
WHERE   Test1.Test1_C2 = 2 ;





IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO

CREATE TABLE dbo.Test1 (C1 INT, C2 INT IDENTITY) ;

INSERT  INTO dbo.Test1
        (C1)
VALUES  (1) ;

SELECT TOP 10000
        IDENTITY( INT,1,1 ) AS n
INTO    #Nums 
FROM    Master.dbo.SysColumns sc1,
        Master.dbo.SysColumns sc2 ;
        
INSERT  INTO dbo.Test1
        (C1)
        SELECT  2
        FROM    #Nums ;
        
DROP TABLE #Nums;
      
CREATE NONCLUSTERED INDEX FirstIndex ON dbo.Test1 (C1) ;





DBCC SHOW_STATISTICS(Test1,   FirstIndex) ;



--Retrieve 1 row; 
SELECT  *
FROM    dbo.Test1
WHERE   C1 = 1 ;

--Retrieve 10000 rows; 
SELECT  *
FROM    dbo.Test1
WHERE   C1 = 2 ;






SELECT  1.0 / COUNT(DISTINCT C1)
FROM    dbo.Test1 ;



CREATE NONCLUSTERED INDEX FirstIndex 
ON dbo.Test1(C1,C2) WITH DROP_EXISTING ;



DBCC SHOW_STATISTICS(dbo.Test1,   FirstIndex) ;




SELECT  1.0 / COUNT(*)
FROM    (SELECT DISTINCT
                C1,
                C2
         FROM   dbo.Test1
        ) DistinctRows ;





CREATE INDEX IX_Test 
ON Sales.SalesOrderHeader (PurchaseOrderNumber) ;



DBCC SHOW_STATISTICS('Sales.SalesOrderHeader',IX_Test) ;



CREATE INDEX IX_Test 
ON Sales.SalesOrderHeader (PurchaseOrderNumber) 
WHERE PurchaseOrderNumber IS NOT NULL 
WITH DROP_EXISTING ;




DROP INDEX Sales.SalesOrderHeader.IX_Test;



SELECT  *
FROM    Sales.CreditCard AS cc
WHERE   cc.CardType = 'Vista' ;







--autoupdates
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 

CREATE TABLE dbo.Test1 (C1 INT) ; 

CREATE INDEX ixl ON dbo.Test1(C1) ; 

INSERT  INTO dbo.Test1
        (C1)
VALUES  (0) ;




SELECT  *
FROM    dbo.Test1
WHERE   C1 = 0 ;



ALTER DATABASE AdventureWorks2008R2 SET AUTO_CREATE_STATISTICS OFF ;





USE AdventureWorks2008R2 ;
EXEC sp_autostats 
    'HumanResources.Department',
    'OFF' ;



EXEC sp_autostats 
    'HumanResources.Department',
    'OFF',
    AK_Department_Name ;





EXEC sp_autostats 
    'HumanResources.Department',
    'ON' ;
EXEC sp_autostats 
    'HumanResources.Department',
    'ON',
    AK_Department_Name ;
  
  
  
SELECT  is_auto_create_stats_on
FROM    sys.databases
WHERE   [name] = 'AdventureWorks2008' ;




USE AdventureWorks2008R2 ;
EXEC sys.sp_autostats 
    'HumanResources.Department' ;



SELECT  DATABASEPROPERTYEX('AdventureWorks2008R2', 'IsAutoUpdateStatistics') ;


EXEC sp_autostats 
    'Sales.SalesOrderDetail' ;



ALTER DATABASE AdventureWorks2008R2 SET AUTO_CREATE_STATISTICS OFF ;
ALTER DATABASE AdventureWorks2008R2 SET AUTO_UPDATE_STATISTICS OFF ;   





IF EXISTS ( SELECT  *
            FROM    sys.objects
            WHERE   object_id = OBJECT_ID(N'dbo.Test1') ) 
    DROP TABLE  [dbo].[dbo.Test1] ;
		GO
	
CREATE TABLE dbo.Test1 (C1 INT, C2 INT, C3 CHAR(50)) ;
INSERT  INTO dbo.Test1
        (C1, C2, C3)
VALUES  (51, 1, 'C3') ,
        (52, 1, 'C3') ;
        
CREATE NONCLUSTERED INDEX iFirstIndex ON dbo.Test1 (C1, C2) ;

SELECT TOP 10000
        IDENTITY( INT,1,1 ) AS n
INTO    #Nums
FROM    Master.dbo.SysColumns scl,
        Master.dbo.SysColumns sC2 ;
        
INSERT  INTO dbo.Test1
        (C1, C2, C3)
        SELECT  n % 50,
                n,
                'C3'
        FROM    #Nums ;
DROP TABLE #Nums ;








SELECT  *
FROM    dbo.Test1
WHERE   C2 = 1 ;

DBCC FREEPROCCACHE();

CREATE STATISTICS Stats1 ON dbo.Test1(C2) ;





DBCC SHOW_STATISTICS(dbo.Test1, iFirstIndex) ;




SELECT  *
FROM    dbo.Test1
WHERE   C1 = 51 ;


UPDATE STATISTICS dbo.Test1;





ALTER DATABASE AdventureWorks2008R2 SET AUTO_CREATE_STATISTICS ON; 
ALTER DATABASE AdventureWorks2008R2 SET AUTO_UPDATE_STATISTICS ON;


EXEC sp_msforeachtable  'UPDATE STATISTICS ? ALL'

EXEC sys.sp_MSforeachtable 
    'UPDATE STATISTICS ? ALL' ;
    @command1 = N'', -- nvarchar(2000)
    @replacechar = N'', -- nchar(1)
    @command2 = N'', -- nvarchar(2000)
    @command3 = N'', -- nvarchar(2000)
    @whereand = N'', -- nvarchar(2000)
    @precommand = N'', -- nvarchar(2000)
    @postcommand = N'' -- nvarchar(2000)




--Chapter 8

--fragment

IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ;
GO
CREATE TABLE dbo.Test1
    (C1 INT,
     C2 CHAR(999),
     C3 VARCHAR(10)
    )
INSERT  INTO dbo.Test1
VALUES  (100, 'C2', ''),
        (200, 'C2', ''),
        (300, 'C2', ''),
        (400, 'C2', ''),
        (500, 'C2', ''),
        (600, 'C2', ''),
        (700, 'C2', ''),
        (800, 'C2', '') ;

CREATE CLUSTERED INDEX iClust 
ON dbo.Test1(C1) ;




SELECT  ddips.avg_fragmentation_in_percent,
        ddips.fragment_count,
        ddips.page_count,
        ddips.avg_page_space_used_in_percent,
        ddips.record_count,
        ddips.avg_record_size_in_bytes
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2008R2'),
                                       OBJECT_ID(N'dbo.Test1'), NULL, NULL,
                                       'Sampled') AS ddips ;
  
  
  



UPDATE  dbo.Test1
SET     C3 = 'Add data'
WHERE   C1 = 200 ;


DBCC IND(AdventureWorks2008R2,'dbo.Test1',-1)



INSERT  INTO dbo.Test1
VALUES  (410, 'C4', ''),
        (900, 'C4', '')


DBCC TRACEON(3604);
DBCC PAGE('AdventureWorks2008R2',1,23124,3);



                                             



--reset Test1 above

INSERT  INTO Test1
VALUES  (110, 'C2', '') ;





INSERT  INTO Test1
VALUES  (120, 'C2', ''),
        (130, 'C2', ''),
        (140, 'C2', ''),
        (900, 'C2', ''),
        (1000, 'C2', ''),
        (1100, 'C2', ''),
        (1200, 'C2', '') ;


DBCC IND(AdventureWorks2008R2,'dbo.Test1',-1)


--createfragmented

IF (SELECT  OBJECT_ID('Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ;
GO
CREATE TABLE dbo.Test1
    (C1 INT,
     C2 INT,
     C3 INT,
     c4 CHAR(2000)
    ) ;
    
CREATE CLUSTERED INDEX i1 ON dbo.Test1 (C1) ;

WITH    Nums
          AS (SELECT    1 AS n
              UNION ALL
              SELECT    n + 1
              FROM      Nums
              WHERE     n < 21
             )
    INSERT  INTO dbo.Test1
            (C1, C2, C3, c4)
            SELECT  n,
                    n,
                    n,
                    'a'
            FROM    Nums ;
            
WITH    Nums
          AS (SELECT    1 AS n
              UNION ALL
              SELECT    n + 1
              FROM      Nums
              WHERE     n < 21
             )
    INSERT  INTO dbo.Test1
            (C1, C2, C3, c4)
            SELECT  41 - n,
                    n,
                    n,
                    'a'
            FROM    Nums;


--fragmentstats
--Reads 6 rows
SELECT  *
FROM    dbo.Test1
WHERE   C1 BETWEEN 21 AND 25 ; 

--Reads all rows
SELECT  *
FROM    dbo.Test1
WHERE   C1 BETWEEN 1 AND 40 ; 


ALTER INDEX i1 ON dbo.Test1 REBUIID ;



--singlestat
--Read 1 row
SELECT  *
FROM    Test1
WHERE   C1 = 10 ;






SELECT  ddips.avg_fragmentation_in_percent,
        ddips.fragment_count,
        ddips.page_count,
        ddips.avg_page_space_used_in_percent,
        ddips.record_count,
        ddips.avg_record_size_in_bytes
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2008R2'),
                                       OBJECT_ID(N'dbo.Test1'), NULL, NULL,
                                       'Sampled') AS ddips ;



SELECT  ddips.*
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2008R2'),
                                       OBJECT_ID(N'dbo.Test1'), NULL, NULL,
                                       'Detailed') AS ddips ;









--createsmallfragmented
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ;
GO

CREATE TABLE dbo.Test1
    (C1 INT,
     C2 INT,
     C3 INT,
     C4 CHAR(2000)
    ) ;
    
DECLARE @n INT = 1 ;

WHILE @n <= 28 
    BEGIN
        INSERT  INTO dbo.Test1
        VALUES  (@n, @n, @n, 'a') ;
        SET @n = @n + 1 ;
    END
    
CREATE CLUSTERED INDEX FirstIndex ON dbo.Test1(C1) ;




CREATE UNIQUE CLUSTERED INDEX FirstIndex 
ON dbo.Test1(C1) 
WITH (DROP_EXISTING = ON) ;


ALTER INDEX i1 ON dbo.Test1 REBUIID;



ALTER INDEX ALL ON dbo.Test1 REBUIID;
ALTER INDEX i1 ON dbo.Test1 REORGANIZE;



--filltest
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1; 
GO
CREATE TABLE dbo.Test1 (C1 INT, C2 CHAR(999)) ;

WITH    Nums
          AS (SELECT    1 AS n
              UNION ALL
              SELECT    n + 1
              FROM      Nums
              WHERE     n < 24
             )
    INSERT  INTO dbo.Test1
            (C1, C2)
            SELECT  n * 100,
                    'a'
            FROM    Nums ;


CREATE CLUSTERED INDEX FillIndex ON Test1(C1);


ALTER INDEX FillIndex ON dbo.Test1 REBUIID 
WITH  (FILLFACTOR= 75) ;



INSERT  INTO dbo.Test1
VALUES  (110, 'a'),  --25th row 
        (120, 'a') ;  --26th row



SELECT  ddips.avg_fragmentation_in_percent,
        ddips.fragment_count,
        ddips.page_count,
        ddips.avg_page_space_used_in_percent,
        ddips.record_count,
        ddips.avg_record_size_in_bytes
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2008R2'),
                                       OBJECT_ID(N'dbo.Test1'), NULL, NULL,
                                       'Sampled') AS ddips ;        


INSERT  INTO dbo.Test1
VALUES  (130, 'a') ;  --27th row




--Chapter 9

CREATE TABLE dbo.Test1 (c1 INT) ;
INSERT  INTO dbo.Test1
VALUES  (1) ;
CEILEKT * FROM dbo.t1 --Error:  I meant,  SELECT * FROM t1




--algebrizertest
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO
CREATE TABLE dbo.Test1 (c1 INT) ;
INSERT  INTO dbo.Test1
VALUES  (1) ;
SELECT  'Before Error',
        c1
FROM    dbo.Test1 AS t ; 
SELECT  'error',
        c1
FROM    dbo.no_Test1 ;
  --Error:  Table doesn't exist 
SELECT  'after error' c1
FROM    dbo.Test1 AS t ;





SELECT  *
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.SalesOrderID BETWEEN 62500 AND 62550 ;



--nontrivialquery
SELECT  soh.SalesOrderNumber,
        sod.OrderQty,
        sod.LineTotal,
        sod.UnitPrice,
        sod.UnitPriceDiscount,
        p.[Name] AS ProductName,
        p.ProductNumber,
        ps.[Name] AS ProductSubCategoryName,
        pc.[Name] AS ProductCategoryName
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
JOIN    Production.Product AS p
        ON sod.ProductID = p.ProductID
JOIN    Production.ProductModel AS pm
        ON p.ProductModelID = pm.ProductModelID
JOIN    Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
JOIN    Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE   soh.CustomerID = 29658 ;





SELECT  deqoi.counter,
        deqoi.occurrence,
        deqoi.value
FROM    sys.dm_exec_query_optimizer_info AS deqoi ;


USE master ;
EXEC sp_configure 
    'show advanced option',
    '1' ;
RECONFIGURE ;
EXEC sp_configure 
    'affinity mask',
    15 ;
 --Bit map: 00001111
RECONFIGURE ;



USE master ;
EXEC sp_configure 
    'show advanced option',
    '1' ;
RECONFIGURE ;
EXEC sp_configure 
    'max degree of parallelism',
    2 ;
RECONFIGURE ;


SELECT  *
FROM    dbo.t1
WHERE   C1 = 1
OPTION  (MAXDOP 2) ;



--parallelism
USE master ;
EXEC sp_configure 
    'show advanced option',
    '1' ;
RECONFIGURE ;
EXEC sp_configure 
    'cost threshoID for parallelism',
    35 ;
RECONFIGURE ;



SELECT  *
FROM    sys.dm_exec_cached_plans ;



USE AdventureWorks2008R2;


--adhoc
SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID = 29690
        AND sod.ProductID = 711 ;
  
        



--spBasicSalesInfo
IF (SELECT  OBJECT_ID('spBasicSalesInfo')
   ) IS NOT NULL 
    DROP PROC dbo.spBasicSalesInfo ;
GO
CREATE PROC dbo.spBasicSalesInfo
    @ProductID INT,
    @CustomerID INT
AS 
SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID = @CustomerID
        AND sod.ProductID = @ProductID ;


SELECT    c.usecounts
	,c.cacheobjtype 
	,c.objtype 
FROM       sys.dm_exec_cached_plans c
	CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t 
WHERE      t.text =  'SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID = 29690
        AND sod.ProductID = 711 ;' ;



SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID = 29500
        AND sod.ProductID = 711 ;





SELECT  c.usecounts,
        c.cacheobjtype,
        c.objtype,
        t.text,
		c.plan_handle      
FROM    sys.dm_exec_cached_plans c
CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t
WHERE   t.text LIKE 'SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID%' ;




sp_configure 
    'optimize for ad hoc workloads',
    1 ;
GO
RECONFIGURE ;


DBCC FREEPROCCACHE();


SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID = 29690
        AND sod.ProductID = 711 ;
  
  

SELECT  c.usecounts,
        c.cacheobjtype,
        c.objtype,
        t.text,
		c.size_in_bytes
FROM    sys.dm_exec_cached_plans c
CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t
WHERE   t.text LIKE 'SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
  
        ON soh.SalesOrderID = sod.SalesOrderID%' ;            		        


sp_configure 
    'optimize for ad hoc workloads',
    0 ;
GO
RECONFIGURE ;


DBCC freeproccache();


SELECT  a.*
FROM    Person.Address AS a
WHERE   a.AddressID = 42 ;        



SELECT  a.*
FROM    Person.Address AS a
WHERE   a.[AddressID] = 52 ; --previous value was 42

DBCC freeproccache();
GO
SELECT  a.*
FROM    Person.Address AS a
WHERE   a.AddressID BETWEEN 40 AND 60 ;
GO
SELECT  a.*
FROM    Person.Address AS a
WHERE   a.AddressID >= 40
        AND a.AddressID <= 60 ;
GO
SELECT  c.usecounts,
        c.cacheobjtype,
        c.objtype,
        t.text
FROM    sys.dm_exec_cached_plans c
CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t 
WHERE text NOT LIKE '()
 select table_id, item_guid, oplsn_fseqno%';
 
 
 
ALTER DATABASE AdventureWorks2008R2 SET PARAMETERIZATION FORCED ;
 
 
DBCC freeproccache()
GO
SELECT ea.EmailAddress,
    e.BirthDate,
    a.City
FROM   Person.Person AS p
JOIN   HumanResources.Employee AS e
    ON p.BusinessEntityID = e.BusinessEntityID
JOIN   Person.BusinessEntityAddress AS bea
    ON e.BusinessEntityID = bea.BusinessEntityID
JOIN   Person.Address AS a
    ON bea.AddressID = a.AddressID
JOIN   Person.StateProvince AS sp
    ON a.StateProvinceID = sp.StateProvinceID
JOIN   Person.EmailAddress AS ea
    ON p.BusinessEntityID = ea.BusinessEntityID
WHERE  ea.EmailAddress LIKE 'david%'
    AND sp.StateProvinceCode = 'WA' ;
GO
SELECT  c.usecounts,
        c.cacheobjtype,
        c.objtype,
        t.text
FROM    sys.dm_exec_cached_plans c
CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t 
WHERE text NOT LIKE '()
 select table_id, item_guid, oplsn_fseqno%'; 







DBCC freeproccache();
GO

EXEC dbo.spBasicSalesInfo 
    @CustomerID = 29690,
    @ProductID = 711 ;
GO
SELECT  c.usecounts,
        c.cacheobjtype,
        c.objtype,
        t.text
FROM    sys.dm_exec_cached_plans c
CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t 
WHERE text NOT LIKE '()
 select table_id, item_guid, oplsn_fseqno%'; 



EXEC dbo.spBasicSalesInfo 
    @CustomerID = 29690,
    @ProductID = 777 ;
GO
SELECT  c.usecounts,
        c.cacheobjtype,
        c.objtype,
        t.text
FROM    sys.dm_exec_cached_plans c
CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t 
WHERE text NOT LIKE '()
 select table_id, item_guid, oplsn_fseqno%';     




IF(SELECT OBJECT_ID('dbo.MyNewProc'))  IS NOT NULL
DROP PROCEDURE dbo.MyNewProc 
GO
CREATE PROCEDURE dbo.MyNewProc 
AS
SELECT MyID 
FROM dbo.NotHere ; --Table no_tl doesn't exist 
GO



IF (SELECT  OBJECT_ID('dbo.RestrictedAccess')
   ) IS NOT NULL 
    DROP TABLE dbo.RestrictedAccess ;
GO
CREATE TABLE dbo.RestrictedAccess (ID INT, Status VARCHAR(7)) ;
INSERT  INTO t1
VALUES  (1, 'New') ;
GO 
IF (SELECT  OBJECT_ID('dbo.MarkDeleted')
   ) IS NOT NULL 
    DROP PROCEDURE dbo.MarkDeleted ;
GO
CREATE PROCEDURE dbo.MarkDeleted @ID INT
AS 
UPDATE  dbo.RestrictedAccess
SET     Status = 'Deleted'
WHERE   ID = @ID ;
GO

--Prevent user u1 from deleting rows 
DENY DELETE ON dbo.RestrictedAccess TO UserOne ;

--Allow user u1 to mark a row as 'deleted' 
GRANT EXECUTE ON dbo.MarkDeleted TO UserOne ;
GO


CREATE PROCEDURE dbo.MarkDeleted 
@ID INT
AS
DECLARE @SQL NVARCHAR(MAX);

SET @SQL = 'UPDATE  dbo.RestrictedAccess
SET     Status = ''Deleted''
WHERE   ID = ' + @ID ;

EXEC sys.sp_executesql @SQL;
GO

GRANT EXECUTE ON dbo.MarkDeleted TO UserOne ;



DBCC freeproccache();
GO
--executesql
DECLARE @query NVARCHAR(MAX),
    @paramlist NVARCHAR(MAX) ;

SET @query = N'SELECT    soh.SalesOrderNumber ,soh.OrderDate ,sod.OrderQty ,sod.LineTotal FROM       Sales.SalesOrderHeader AS soh
JOIN Sales.SalesOrderDetail AS sod ON soh.SalesOrderID = sod.SalesOrderID WHERE      soh.CustomerID = @CustomerID
AND sod.ProductID = @ProductID' ;

SET @paramlist = N'@CustomerID INT, @ProductID INT' ;

EXEC sp_executesql 
    @query,
    @paramlist,
    @CustomerID = 29690,
    @ProductID = 711 ;
GO
SELECT  c.usecounts,
        c.cacheobjtype,
        c.objtype,
        t.text
FROM    sys.dm_exec_cached_plans c
CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t 
WHERE text LIKE '(@CustomerID%';  





DBCC freeproccache();
GO
--executesql
DECLARE @query NVARCHAR(MAX),
    @paramlist NVARCHAR(MAX) ;

SET @query = N'SELECT    soh.SalesOrderNumber ,soh.OrderDate ,sod.OrderQty ,sod.LineTotal FROM       Sales.SalesOrderHeader AS soh
JOIN Sales.SalesOrderDetail AS sod ON soh.SalesOrderID = sod.SalesOrderID WHERE      soh.CustomerID = @CustomerID
AND sod.ProductID = @ProductID' ;

SET @paramlist = N'@CustomerID INT, @ProductID INT' ;

EXEC sp_executesql 
    @query,
    @paramlist,
    @CustomerID = 29690,
    @ProductID = 777 ;
GO
SELECT  c.usecounts,
        c.cacheobjtype,
        c.objtype,
        t.text
FROM    sys.dm_exec_cached_plans c
CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t 
WHERE text NOT LIKE '()
 select table_id, item_guid, oplsn_fseqno%'; 
 
 
 
 


DECLARE @query NVARCHAR(MAX),
    @paramlist NVARCHAR(MAX) ;

SET @query = N'SELECT    soh.SalesOrderNumber ,soh.OrderDate ,sod.OrderQty ,sod.LineTotal FROM       Sales.SalesOrderHeader AS soh
JOIN Sales.SalesOrderDetail AS sod ON soh.SalesOrderID = sod.SalesOrderID where      soh.CustomerID = @CustomerID
AND sod.ProductID = @ProductID' ;

SET @paramlist = N'@CustomerID INT, @ProductID INT' ;

EXEC sp_executesql 
    @query,
    @paramlist,
    @CustomerID = 29690,
    @ProductID = 777 ;
GO
SELECT  c.usecounts,
        c.cacheobjtype,
        c.objtype,
        t.text
FROM    sys.dm_exec_cached_plans c
CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) t 
WHERE text NOT LIKE '()
 select table_id, item_guid, oplsn_fseqno%';


SELECT  decp.usecounts,
        decp.cacheobjtype,
        decp.objtype,
        dest.text,
        deqs.creation_time,
        deqs.execution_count,
	deqs.query_hash,
	deqs.query_plan_hash	      
FROM    sys.dm_exec_cached_plans AS decp
CROSS APPLY sys.dm_exec_sql_text(decp.plan_handle) AS dest
JOIN    sys.dm_exec_query_stats AS deqs
        ON decp.plan_handle = deqs.plan_handle
WHERE   dest.text LIKE '(@CustomerID INT, @ProductID INT)%';







--spaddressbycity
IF (SELECT  OBJECT_ID('dbo.spAddressByCity')
   ) IS NOT NULL 
    DROP PROC dbo.spAddressByCity 
GO
CREATE PROC dbo.spAddressByCity @City NVARCHAR(30)
AS 
SELECT  a.AddressID,
        a.AddressLine1,
        AddressLine2,
        a.City,
        sp.[Name] AS StateProvinceName,
        a.PostalCode
FROM    Person.Address AS a
JOIN    Person.StateProvince AS sp
        ON a.StateProvinceID = sp.StateProvinceID
WHERE   a.City = @City ;



DBCC FREEPROCCACHE()


EXEC dbo.spAddressByCity 
    @City = N'London' ;

EXEC dbo.spAddressByCity 
    @City = N'Mentor' ;

SELECT  dest.text,
        deqs.execution_count,
        deqs.creation_time
FROM    sys.dm_exec_query_stats AS deqs
CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest
WHERE   dest.text LIKE 'CREATE PROC dbo.spAddressByCity%';




DBCC FREEproccache()

--queryhash
SELECT  *
FROM    Production.Product AS p
JOIN    Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
JOIN    Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE   pc.[Name] = 'Bikes'
        AND ps.[Name] = 'Touring Bikes' ;


SELECT  *
FROM    Production.Product AS p
JOIN    Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
JOIN    Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE   pc.[Name] = 'Bikes'
        AND ps.[Name] = 'Road Bikes' ;





SELECT  deqs.execution_count,
        deqs.query_hash,
        deqs.query_plan_hash,
        dest.text
FROM    sys.dm_exec_query_stats AS deqs
CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) dest
WHERE   dest.text LIKE 'SELECT  *
FROM    Production.Product AS p%' ;
OR dest.text LIKE 'SELECT  p.ProductID
FROM    Production.Product AS p%';




SELECT  p.ProductID
FROM    Production.Product AS p
JOIN    Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
JOIN    Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE   pc.[Name] = 'Bikes'
        AND ps.[Name] = 'Touring Bikes' ;




--queryplanhash

SELECT  p.[Name],
        tha.TransactionDate,
        tha.TransactionType,
        tha.Quantity,
        tha.ActualCost
FROM    Production.TransactionHistoryArchive tha
JOIN    Production.Product p
        ON tha.ProductID = p.ProductID
WHERE   p.ProductID = 461 ;


SELECT  p.[Name],
        tha.TransactionDate,
        tha.TransactionType,
        tha.Quantity,
        tha.ActualCost
FROM    Production.TransactionHistoryArchive tha
JOIN    Production.Product p
        ON tha.ProductID = p.ProductID
WHERE   p.ProductID = 712 ;




SELECT  deqs.execution_count,
        deqs.query_hash,
        deqs.query_plan_hash,
        dest.text
FROM    sys.dm_exec_query_stats AS deqs
CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) dest
WHERE   dest.text LIKE 'SELECT  p.[Name],
        tha.TransactionDate%';



DECLARE @n VARCHAR(3) = '776',
    @sql VARCHAR(MAX) ;

SET @sql = 'SELECT * FROM Sales.SalesOrderDetail sod  '
    + 'JOIN Sales.SalesOrderHeader soh  '
    + 'ON sod.SalesOrderID=soh.SalesOrderID ' + 'WHERE    sod.ProductID='''
    + @n + '''' ;

--Execute the dynamic query using EXECUTE statement 
EXECUTE  (@sql) ;

SELECT  deqs.execution_count,
        deqs.query_hash,
        deqs.query_plan_hash,
        dest.text,
		deqp.query_plan
FROM    sys.dm_exec_query_stats AS deqs
CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) dest
CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS deqp



DECLARE @n NVARCHAR(3) = '776',
    @sql NVARCHAR(MAX),
    @paramdef NVARCHAR(6) ;
    
SET @sql = 'SELECT * FROM Sales.SalesOrderDetail sod  '
    + 'JOIN Sales.SalesOrderHeader soh  '
    + 'ON sod.SalesOrderID=soh.SalesOrderID ' + 'WHERE    sod.ProductID=@1' ;
SET @paramdef = N'@1 INT' ;

--Execute the dynamic query using sp_executesql system stored procedure 
EXECUTE sp_executesql 
    @sql,
    @paramdef,
    @1 = @n ;





--Chapter 10
IF (SELECT  OBJECT_ID('dbo.spWorkOrder')
   ) IS NOT NULL 
    DROP PROCEDURE dbo.spWorkOrder ; 
GO
CREATE PROCEDURE dbo.spWorkOrder
AS 
SELECT  wo.WorkOrderID,
        wo.ProductID,
        wo.StockedQty
FROM    Production.WorkOrder AS wo
WHERE   wo.StockedQty BETWEEN 500 AND 700 ;




EXEC spWorkOrder;





CREATE INDEX IX_Test ON Production.WorkOrder(StockedQty,ProductID) ;



IF (SELECT  OBJECT_ID('dbo.spWorkOrderAll')
   ) IS NOT NULL 
    DROP PROCEDURE dbo.spWorkOrderAll ; 
GO
CREATE PROCEDURE dbo.spWorkOrderAll
AS 
SELECT  *
FROM    Production.WorkOrder AS wo ;



DROP INDEX Production.WorkOrder.IX_Test ;



EXEC dbo.spWorkOrderAll ;
GO
CREATE INDEX IX_Test ON Production.WorkOrder(StockedQty,ProductID) ;
GO
EXEC dbo.spWorkOrderAll ; --After creation of index IX_Test





IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL 
    DROP PROC dbo.TestProc ; 
GO
CREATE PROC dbo.TestProc
AS 
CREATE TABLE #Test1 (C1 INT) ;
INSERT  INTO #Test1
        (C1)
VALUES  (42) ; -- data change causes recompile 
GO


EXEC dbo.TestProc;





SELECT  DATABASEPROPERTYEX('AdventureWorks2008R2', 'IsAutoUpdateStatistics') ;




--statschanges
IF EXISTS ( SELECT  *
            FROM    sys.objects AS o
            WHERE   o.object_id = OBJECT_ID(N'[dbo].[NewOrders]')
                    AND o.type IN (N'U') ) 
    DROP TABLE [dbo].[NewOrders] ; 
GO
SELECT  *
INTO    dbo.NewOrders
FROM    Sales.SalesOrderDetail ; 
GO
CREATE INDEX IX_NewOrders_ProductID ON dbo.NewOrders (ProductID) ;
GO
IF EXISTS ( SELECT  *
            FROM    sys.objects
            WHERE   object_id = OBJECT_ID(N'[dbo].[spNewOrders]')
                    AND type IN (N'P', N'PC') ) 
    DROP PROCEDURE [dbo].[spNewOrders] ; 
GO
CREATE PROCEDURE dbo.spNewOrders
AS 
SELECT  nwo.OrderQty,
        nwo.CarrierTrackingNumber
FROM    dbo.NewOrders nwo
WHERE   ProductID = 897 ; 
GO
SET STATISTICS XML ON ;
EXEC dbo.spNewOrders ;
SET STATISTICS XML OFF ; 
GO


--statschanges step 2
UPDATE  dbo.NewOrders
SET     ProductID = 897
WHERE   ProductID BETWEEN 800 AND 900 ;
GO
SET STATISTICS XML ON ;
EXEC dbo.spNewOrders ;
SET STATISTICS XML OFF ; 
GO




--regular
IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL 
    DROP PROC dbo.TestProc ; 
GO
CREATE PROC dbo.TestProc 
AS 
CREATE TABLE dbo.ProcTest1 (C1 INT) ;  --Ensure table doesn't exist 
SELECT  *
FROM    dbo.ProcTest1 ;   --Causes recompilation 
DROP TABLE dbo.ProcTest1 ; 
GO

EXEC dbo.TestProc ;   --First execution 
EXEC dbo.TestProc ;   --Second execution




IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL 
    DROP PROC dbo.TestProc ; 
GO
CREATE PROC dbo.TestProc 
AS 
CREATE TABLE #ProcTest1 (C1 INT) ;  --Ensure table doesn't exist 
SELECT  *
FROM    #ProcTest1 ;   --Causes recompilation 
DROP TABLE #ProcTest1 ; 
GO

EXEC dbo.TestProc ;   --First execution 
EXEC dbo.TestProc ;   --Second execution







IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL 
    DROP PROC dbo.TestProc ; 
GO
CREATE PROC dbo.TestProc
AS 
SELECT  'a' + NULL + 'b' ; --1st 
SET CONCAT_NULL_YIELDS_NULL OFF ; 
SELECT  'a' + NULL + 'b' ; --2nd 
SET ANSI_NULLS OFF ; 
SELECT  'a' + NULL + 'b' ;
 --3rd 
GO
EXEC dbo.TestProc ; --First execution 
EXEC dbo.TestProc ; --Second execution








DBCC FREEPROCCACHE ;
 --Clear the procedure cache
GO
DECLARE @query NVARCHAR(MAX) ;
DECLARE @param NVARCHAR(MAX) ;
SET @query = N'SELECT    soh.SalesOrderNumber ,soh.OrderDate ,sod.OrderQty ,sod.LineTotal FROM       Sales.SalesOrderHeader AS soh
JOIN Sales.SalesOrderDetail AS sod ON soh.SalesOrderID = sod.SalesOrderID WHERE      soh.CustomerID >= @CustomerId;'
SET @param = N'@CustomerId INT' ;
EXEC sp_executesql 
    @query,
    @param,
    @CustomerId = 1 ;
EXEC sp_executesql 
    @query,
    @param,
    @CustomerId = 30118 ;








IF (SELECT  OBJECT_ID('dbo.CustomerList')
   ) IS NOT NULL 
    DROP PROC dbo.CustomerList 
GO 
IF (SELECT  OBJECT_ID('dbo. CustomerList')
   ) IS NOT NULL 
    DROP PROC dbo. CustomerList 
GO
CREATE PROCEDURE dbo.CustomerList @CustomerID INT
AS 
SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= @CustomerId 
GO



EXEC CustomerList 
    @CustomerID = 1 ;
EXEC CustomerList 
    @CustomerID = 30118 ;




EXEC dbo.CustomerList 
    @CustomerID = 1 
    WITH RECOMPILE ;



IF (SELECT  OBJECT_ID('dbo.spCustomerList')
   ) IS NOT NULL 
    DROP PROC dbo.spCustomerList ;
GO
CREATE PROCEDURE dbo.spCustomerList @CustomerId INT
AS 
SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= @CustomerId OPTION (RECOMPILE); 


GO


EXEC spCustomerList 
    @CustomerId = 1 ;
EXEC spCustomerList 
    @CustomerId = 30118 ;    





--ddl
IF (SELECT  OBJECT_ID('dbo.spTempTable')
   ) IS NOT NULL 
    DROP PROC dbo.spTempTable 
GO
CREATE PROC dbo.spTempTable
AS 
CREATE TABLE #MyTempTable (ID INT, Dsc NVARCHAR(50))
INSERT  INTO #MyTempTable
        (ID,
         Dsc
        )
        SELECT  pm.ProductModelID,
                pm.[Name]
        FROM    Production.ProductModel AS pm ;   --Needs 1st recompilation
SELECT  *
FROM    #MyTempTable AS mtt ;
CREATE CLUSTERED INDEX iTest ON #MyTempTable (ID) ;
SELECT  *
FROM    #MyTempTable AS mtt ; --Needs 2nd recompilation
CREATE TABLE #t2 (c1 INT) ;
SELECT  *
FROM    #t2 ; --Needs 3rd recompilation 
GO

EXEC dbo.spTempTable ; --First execution












IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO
CREATE TABLE dbo.Test1 (C1 INT, C2 CHAR(50)) ;
INSERT  INTO dbo.Test1
VALUES  (1, '2') ;
CREATE NONCLUSTERED INDEX IndexOne ON dbo.Test1 (C1) ;

--Create a stored procedure referencing the previous table 
IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL 
    DROP PROC dbo.TestProc ; 
GO 
CREATE PROC dbo.TestProc
AS 
SELECT  *
FROM    dbo.Test1 AS t
WHERE   t.C1 = 1
OPTION  (KEEPFIXED PLAN) ;
GO

--First execution of stored procedure with 1 row in the table 
EXEC dbo.TestProc ;
 --First execution

--Add many rows to the table to cause statistics change 
WITH    Nums
          AS (SELECT    1 AS n
              UNION ALL
              SELECT    n + 1
              FROM      Nums
              WHERE     n < 1000
             )
    INSERT  INTO dbo.Test1
            (C1,
             C2
            )
            SELECT  1,
                    n
            FROM    Nums
    OPTION  (MAXRECURSION 1000) ; 
GO
--Reexecute the stored procedure with a change in statistics 
EXEC dbo.TestProc ; --With change in data distribution









EXEC sp_autostats 
    'dbo.Test1',
    'OFF' ;






IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL 
    DROP PROC dbo.TestProc ; 
GO
CREATE PROC dbo.TestProc
AS 
CREATE TABLE #TempTable (C1 INT) ;
    INSERT  INTO #TempTable
            (C1)
    VALUES  (42) ;
   -- data change causes recompile 
GO

EXEC dbo.TestProc ;   --First execution



IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL 
    DROP PROC dbo.TestProc ; 
GO
CREATE PROC dbo.TestProc
AS 
DECLARE @TempTable TABLE (C1 INT) ; 
INSERT  INTO @TempTable
        (C1)
VALUES  (42) ;
 --Recompilation not needed
GO

EXEC dbo.TestProc ; --First execution



--rollback
DECLARE @t1 TABLE (c1 INT) ; 
INSERT  INTO @t1
VALUES  (1)
BEGIN TRAN
INSERT  INTO @t1
VALUES  (2)
ROLLBACK
SELECT  *
FROM    @t1 --Returns 2 rows









IF (SELECT  OBJECT_ID('dbo.TestProc')
   ) IS NOT NULL 
    DROP PROC dbo.TestProc 
GO
CREATE PROC dbo.TestProc
AS 
SELECT  'a' + NULL + 'b' ; --1st SET CONCAT_NULL_YIELDS_NULL OFF

SELECT  'a' + NULL + 'b' ; --2nd
SET ANSI_NULLS OFF
SELECT  'a' + NULL + 'b' ;
 --3rd
GO

SET CONCAT_NULL_YIELDS_NULL OFF ; 
SET ANSI_NULLS OFF ;

EXEC dbo.TestProc ;

SET CONCAT_NULL_YIELDS_NULL ON ;
 --Reset to default
SET ANSI_NULLS ON ;	--Reset to default







IF (SELECT  OBJECT_ID('dbo.spCustomerList')
   ) IS NOT NULL 
    DROP PROC dbo.spCustomerList 
GO
CREATE PROCEDURE dbo.spCustomerList @CustomerID INT
AS 
SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= @CustomerID
OPTION  (OPTIMIZE FOR (@CustomerID = 1)) ; 
GO
    


EXEC dbo.spCustomerList 
    @CustomerID = 7920 
    WITH RECOMPILE ; 
EXEC dbo.spCustomerList 
    @CustomerID = 30118 
    WITH RECOMPILE ;    










IF (SELECT  OBJECT_ID('dbo.spCustomerList')
   ) IS NOT NULL 
    DROP PROC dbo.spCustomerList 
GO 
IF (SELECT  OBJECT_ID('dbo.spCustomerList')
   ) IS NOT NULL 
    DROP PROC dbo.spCustomerList 
GO
CREATE PROCEDURE dbo.spCustomerList @CustomerID INT
AS 
SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= @CustomerID 
GO


sp_create_plan_guide 
    @name = N'MyGuide',
    @stmt = N'SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= @CustomerID',
    @type = N'OBJECT',
    @module_or_batch = N'dbo.spCustomerList',
    @params = NULL,
    @hints = N'OPTION (OPTIMIZE FOR (@CustomerID = 1))' ;









EXEC dbo.spCustomerList 
    @CustomerID = 7920 
    WITH RECOMPILE ; 
EXEC dbo.spCustomerList 
    @CustomerID = 30118 
    WITH RECOMPILE ;    









SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= 1 ;






--badguide
EXECUTE sp_create_plan_guide 
    @name = N'MyBadSOLGuide',
    @stmt = N'SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
JOIN Sales.SalesOrderDetail AS sod
ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= @Customerld',
    @type = N'SOL',
    @module_or_batch = NULL,
    @params = N'@Customerld int',
    @hints = N'OPTION  (TABLE HINT(soh,  FORCESEEK))' ;






EXECUTE sp_control_plan_guide 
    @operation = 'Drop',
    @name = N'MyBadSQLGuide' ;






EXECUTE sp_create_plan_guide 
    @name = N'MyGoodSQLGuide',
    @stmt = N'SELECT  soh.SalesOrderNumber,
        soh.OrderDate,
        sod.OrderQty,
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.CustomerID >= 1 ;',
    @type = N'SQL',
    @module_or_batch = NULL,
    @params = NULL,
    @hints = N'OPTION  (TABLE HINT(soh,  FORCESEEK))' ;    


EXECUTE sp_control_plan_guide 
    @operation = 'Drop',
    @name = N'MyGoodSQLGuide' ;    





DBCC FREEPROCCACHE();


EXECUTE sp_control_plan_guide 
    @operation = 'Drop',
    @name = N'MyGoodSQLGuide' ;    





DECLARE @plan_handle VARBINARY(64),
    @start_offset INT ;

SELECT  @plan_handle = deqs.plan_handle,
        @start_offset = deqs.statement_start_offset
FROM    sys.dm_exec_query_stats AS deqs
CROSS APPLY sys.dm_exec_sql_text(sql_handle)
CROSS APPLY sys.dm_exec_text_query_plan(deqs.plan_handle,
                                        deqs.statement_start_offset,
                                        deqs.statement_end_offset) AS qp
WHERE   text LIKE N'SELECT soh.SalesOrderNumber%'

EXECUTE sp_create_plan_guide_from_handle 
    @name = N'ForcedPlanGuide',
    @plan_handle = @plan_handle,
    @statement_start_offset = @start_offset ; 
GO








--Chapter 11

SELECT  [Name],
        TerritoryID
FROM    Sales.SalesTerritory AS st
WHERE   st.[Name] = 'Australia' ;





SELECT  *
FROM    Sales.SalesTerritory AS st
WHERE   st.[Name] = 'Australia' ;





SELECT  sod.*
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.SalesOrderID IN (51825, 51826, 51827, 51828) ;


SELECT  sod.*
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.SalesOrderID BETWEEN 51825 AND 51828 ;






SELECT  c.CurrencyCode
FROM    Sales.Currency AS c
WHERE   c.[Name] LIKE 'Ice%' ;





SELECT  c.CurrencyCode
FROM    Sales.Currency AS c
WHERE   c.[Name] >= N'IcF'
        AND c.[Name] < N'IcF' ;





SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh
WHERE   poh.PurchaseOrderID >= 2975 ;
SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh
WHERE   poh.PurchaseOrderID !< 2975 ;








SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh
WHERE   poh.PurchaseOrderID * 2 = 3400 ;


SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh
WHERE   poh.PurchaseOrderID = 3400 / 2 ;






SELECT  d.Name
FROM    HumanResources.Department AS d
WHERE   SUBSTRING(d.[Name], 1, 1) = 'F' ;



SELECT  d.Name
FROM    HumanResources.Department AS d
WHERE   d.[Name] LIKE 'F%' ;







IF EXISTS ( SELECT  *
            FROM    sys.indexes
            WHERE   object_id = OBJECT_ID(N'[Sales].[SalesOrderHeader]')
                    AND name = N'IndexTest' ) 
    DROP INDEX IndexTest ON [Sales].[SalesOrderHeader] ;
GO
CREATE INDEX IndexTest ON Sales.SalesOrderHeader(OrderDate) ;






SELECT  soh.SalesOrderID,
        soh.OrderDate
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   DATEPART(yy, soh.OrderDate) = 2008
        AND DATEPART(mm, soh.OrderDate) = 4;

 
 

SELECT  soh.SalesOrderID,
        soh.OrderDate
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE   soh.OrderDate >= '2008-04-01'
        AND soh.OrderDate < '2008-05-01' ;
  
  
  
  
 DROP INDEX Sales.SalesOrderHeader.IndexTest ;
 
 
 
 
 --join
SELECT s.[Name] AS StoreName,
    p.[LastName] + ', ' + p.[FirstName]
FROM   [Sales].[Store] s
JOIN   [Sales].SalesPerson AS sp
    ON s.SalesPersonID = sp.BusinessEntityID
JOIN   HumanResources.Employee AS e
    ON sp.BusinessEntityID = e.BusinessEntityID
JOIN   Person.Person AS p
    ON e.BusinessEntityID = p.BusinessEntityID ;


SELECT  s.[Name] AS StoreName,
        p.[LastName] + ',   ' + p.[FirstName]
FROM    [Sales].[Store] s
JOIN    [Sales].SalesPerson AS sp
        ON s.SalesPersonID = sp.BusinessEntityID
JOIN    HumanResources.Employee AS e
        ON sp.BusinessEntityID = e.BusinessEntityID
JOIN    Person.Person AS p
        ON e.BusinessEntityID = p.BusinessEntityID
OPTION  (LOOP JOIN) ;



SELECT  s.[Name] AS StoreName,
        p.[LastName] + ',   ' + p.[FirstName]
FROM    [Sales].[Store] s
INNER LOOP JOIN [Sales].SalesPerson AS sp
        ON s.SalesPersonID = sp.BusinessEntityID
JOIN    HumanResources.Employee AS e
        ON sp.BusinessEntityID = e.BusinessEntityID
JOIN    Person.Person AS p
        ON e.BusinessEntityID = p.BusinessEntityID ;    









SELECT *
FROM Purchasing.PurchaseOrderHeader AS poh 
WHERE   poh.PurchaseOrderID  = 3400/2 ; 

SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh 
WHERE   poh.PurchaseOrderID * 2 = 3400 ;    

SELECT  *
FROM    Purchasing.PurchaseOrderHeader AS poh WITH (INDEX (PK_PurchaseOrderHeader_PurchaseOrderID))
WHERE   poh.PurchaseOrderID * 2 = 3400 ;
 





SELECT  p.FirstName
FROM    Person.Person AS p
WHERE   p.FirstName < 'B'
        OR p.FirstName >= 'C' ;

SELECT  p.MiddleName
FROM    Person.Person AS p
WHERE   p.MiddleName < 'B'
        OR p.MiddleName >= 'C' ;



SELECT  p.FirstName
FROM    Person.Person AS p
WHERE   p.FirstName < 'B'
        OR p.FirstName >= 'C' ;

SELECT  p.MiddleName
FROM    Person.Person AS p
WHERE   p.MiddleName < 'B'
        OR p.MiddleName >= 'C'
        OR p.MiddleName IS NULL ;



CREATE INDEX TestIndex1
ON Person.Person (MiddleName) ; 

CREATE INDEX TestIndex2 
ON Person.Person (FirstName) ;



DROP INDEX TestIndex1 ON Person.Person ;

DROP INDEX TestIndex2 ON Person.Person ;





IF EXISTS ( SELECT  *
            FROM    sys.foreign_keys
            WHERE   object_id = OBJECT_ID(N'[Person].[FK_Address_StateProvince_StateProvinceID]')
                    AND parent_object_id = OBJECT_ID(N'[Person].[Address]') ) 
    ALTER TABLE  [Person].[Address] DROP CONSTRAINT [FK_Address_StateProvince_StateProvinceID] ;



--Prod
SELECT  a.AddressID,
        sp.StateProvinceID
FROM    Person.Address AS a
JOIN    Person.StateProvince AS sp
        ON a.StateProvinceID = sp.StateProvinceID
WHERE   a.AddressID = 27234 ;


--Prod2
SELECT  a.AddressID,
        a.StateProvinceID
FROM    Person.Address AS a
JOIN    Person.StateProvince AS sp
        ON a.StateProvinceID = sp.StateProvinceID
WHERE   a.AddressID = 27234 ;



ALTER TABLE [Person].[Address]
WITH CHECK ADD CONSTRAINT [FK_Address_StateProvince_StateProvinceID] 
FOREIGN KEY ([StateProvinceID]) 
REFERENCES [Person].[StateProvince] ([StateProvinceID]) ;





IF EXISTS ( SELECT  *
            FROM    sys.objects
            WHERE   object_id = OBJECT_ID(N'dbo.Test1') ) 
    DROP TABLE  dbo.Test1 ; 

CREATE TABLE dbo.Test1
    (Id INT IDENTITY(1, 1),
     MyKey VARCHAR(50),
     MyValue VARCHAR(50)
    ) ; 
CREATE UNIQUE CLUSTERED INDEX Test1PrimaryKey ON dbo.Test1  ([Id] ASC) ; 
CREATE UNIQUE NONCLUSTERED INDEX TestIndex ON dbo.Test1 (MyKey) ; 
GO

SELECT TOP 10000
        IDENTITY( INT,1,1 ) AS n
INTO    #Tally
FROM    Master.dbo.SysColumns scl,
        Master.dbo.SysColumns sc2 ;

INSERT  INTO dbo.Test1
        (MyKey,
         MyValue
        )
        SELECT TOP 10000
                'UniqueKey' + CAST(n AS VARCHAR),
                'Description'
        FROM    #Tally ;

DROP TABLE #Tally ;

SELECT  t.MyValue
FROM    dbo.Test1 AS t
WHERE   t.MyKey = 'UniqueKey333' ;

SELECT  t.MyValue
FROM    dbo.Test1 AS t
WHERE   t.MyKey = N'UniqueKey333' ;







DECLARE @n INT ;
SELECT  @n = COUNT(*)
FROM    Sales.SalesOrderDetail AS sod
WHERE   sod.OrderQty = 1 ;
IF @n > 0 
    PRINT 'Record Exists' ;



IF EXISTS ( SELECT  sod.*
            FROM    Sales.SalesOrderDetail AS sod
            WHERE   sod.OrderQty = 1 ) 
    PRINT 'Record Exists';










SELECT  *
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.SalesOrderNumber LIKE '%47808'
UNION
SELECT  *
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.SalesOrderNumber LIKE '%65748' ;



SELECT  *
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.SalesOrderNumber LIKE '%47808'
UNION ALL
SELECT  *
FROM    Sales.SalesOrderHeader AS soh
WHERE   soh.SalesOrderNumber LIKE '%65748' ;










SELECT  MIN(sod.UnitPrice)
FROM    Sales.SalesOrderDetail AS sod ;


CREATE INDEX TestIndex ON Sales.SalesOrderDetail (UnitPrice ASC) ;






--batch
DECLARE @id INT = 1 ;
SELECT  pod.*
FROM    Purchasing.PurchaseOrderDetail AS pod
JOIN    Purchasing.PurchaseOrderHeader AS poh
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
WHERE   poh.PurchaseOrderID >= @id ;




SELECT  pod.*
FROM    Purchasing.PurchaseOrderDetail AS pod
JOIN    Purchasing.PurchaseOrderHeader AS poh
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
WHERE   poh.PurchaseOrderID >= 1 ;





--batchproc
CREATE PROCEDURE spProductDetails (@id INT)
AS 
SELECT  pod.*
FROM    Purchasing.PurchaseOrderDetail AS pod
JOIN    Purchasing.PurchaseOrderHeader AS poh
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
WHERE   poh.PurchaseOrderID >= @id ;
 
GO 
EXEC spProductDetails 
    @id = 1 ;






--spdont

IF EXISTS ( SELECT  *
            FROM    sys.objects
            WHERE   object_id = OBJECT_ID(N'[dbo].[sp_Dont]')
                    AND type IN (N'P', N'PC') ) 
    DROP PROCEDURE  [dbo].[sp_Dont]
GO
CREATE PROC [sp_Dont]
AS 
PRINT 'Done!' 
GO
--Add plan of sp_Dont to procedure cache
EXEC AdventureWorks2008R2.dbo.[sp_Dont] ; 
GO
--Use the above cached plan of sp_Dont
EXEC AdventureWorks2008R2.dbo.[sp_Dont] ; 
GO





CREATE PROC sp_addmessage @param1 NVARCHAR(25) 
AS
PRINT  '@param1 =  '  + @param1 ;
GO

EXEC AdventureWorks2008R2.dbo.[sp_addmessage]   'AdventureWorks';



DROP PROCEDURE sp_addmessage;








--logging

--Create a test table 
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO
CREATE TABLE dbo.Test1 (C1 TINYINT) ; 
GO

--Insert 10000 rows 
DECLARE @Count INT = 1 ;
WHILE @Count <= 10000 
    BEGIN
        INSERT  INTO dbo.Test1
                (C1)
        VALUES  (@Count % 256) ;
        SET @Count = @Count + 1 ;
    END



--Insert 10000 rows DECLARE @Count INT = 1;
DECLARE @Count INT = 1 ;
BEGIN TRANSACTION
WHILE @Count <= 10000 
    BEGIN
        INSERT  INTO dbo.Test1
                (C1)
        VALUES  (@Count % 256) ;
        SET @Count = @Count + 1 ;
        END
COMMIT





DBCC SQLPERF(LOGSPACE);



SELECT TOP 10000
        IDENTITY( INT,1,1 ) AS n
INTO    #Tally
FROM    Master.dbo.SysColumns sc1,
        Master.dbo.SysColumns sc2
BEGIN TRANSACTION
INSERT  INTO dbo.t1
        (c1)
        SELECT  (n % 256)
        FROM    #Tally ;
COMMIT TRANSACTION
DROP TABLE #Tally ;







ALTER DATABASE AdventureWorks2008R2
ADD FILEGROUP READONLYFILEGROUP ;
GO
ALTER DATABASE AdventureWorks2008R2
ADD FILE(NAME=ReadOnlyFile,  FILENAME='C:\Data\adw_l.ndf')
TO FILEGROUP READONLYFILEGROUP ;
GO



CREATE TABLE Tl (Cl INT, C2 INT)
ON  READONLYFILEGROUP ; 

CREATE CLUSTERED INDEX II ON Tl(Cl);

INSERT  INTO Tl
VALUES  (1, 1);

--Or move existing table(s) to the new filegroup 
CREATE CLUSTERED INDEX II ON Tl(Cl) 
WITH DROP_EXISTING ON READONLYFILEGROUP ;



ALTER DATABASE AdventureWorks2008R2
MODIFY FILEGROUP READONLYFILEGROUP READONLY;












--Chapter 12

--atomicity
--Create a test table 
IF (SELECT  OBJECT_ID('dbo.ProductTest')
   ) IS NOT NULL 
    DROP TABLE dbo.ProductTest ;
GO 
CREATE TABLE dbo.ProductTest
    (ProductID INT CONSTRAINT ValueEqualsOne CHECK (ProductID = 1)
    ) ;
GO
--All ProductIDs are added into t1 as a logical unit of work 
INSERT  INTO dbo.ProductTest
        SELECT  p.ProductID
        FROM    Production.Product AS p ;
GO
SELECT  *
FROM    dbo.ProductTest ; --Returns 0 rows


--logical
BEGIN TRAN
 --Start:  Logical unit of work
--First:
INSERT  INTO dbo.ProductTest
        SELECT  p.ProductID
        FROM    Production.Product AS p ;
--Second:
INSERT  INTO dbo.ProductTest
VALUES  (1) ;
COMMIT --End:   Logical unit of work 
GO


SELECT  *
FROM    dbo.ProductTest ; --Returns a row with t1.c1 = 1






SET XACT_ABORT ON 
GO
BEGIN TRAN
 --Start:  Logical unit of work
--First:
INSERT  INTO dbo.ProductTest
        SELECT  p.ProductID
        FROM    Production.Product AS p
--Second:
INSERT  INTO dbo.ProductTest
VALUES  (1)
COMMIT
 --End:   Logical unit of work GO
SET XACT_ABORT OFF 
GO




BEGIN TRY
    BEGIN TRAN --Start: Logical unit of work --First: 
    INSERT  INTO dbo.t1
            SELECT  p.ProductID
            FROM    Production.Product AS p

    Second:
    INSERT  INTO dbo.t1
    VALUES  (1)
    COMMIT --End: Logical unit of work 
END TRY 
BEGIN CATCH
    ROLLBACK
    PRINT 'An error occurred'
    RETURN 
END CATCH




--rowlock
--Create a test table
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ;
GO
CREATE TABLE dbo.Test1 (C1 INT) ; 
INSERT  INTO dbo.Test1
VALUES  (1) ;
GO

BEGIN TRAN
DELETE  dbo.Test1
WHERE   C1 = 1 ; 

SELECT  dtl.request_session_id,
        dtl.resource_database_id,
        dtl.resource_associated_entity_id,
        dtl.resource_type,
        dtl.resource_description,
        dtl.request_mode,
        dtl.request_status
FROM    sys.dm_tran_locks AS dtl
WHERE   dtl.request_session_id = @@SPID ;
ROLLBACK



SELECT  OBJECT_NAME(192719739),
        DB_NAME(9) ;



CREATE CLUSTERED INDEX TestIndex ON dbo.Test1(C1) ;


BEGIN TRAN
DELETE  dbo.Test1
WHERE   C1 = 1 ; 

SELECT  dtl.request_session_id,
        dtl.resource_database_id,
        dtl.resource_associated_entity_id,
        dtl.resource_type,
        dtl.resource_description,
        dtl.request_mode,
        dtl.request_status
FROM    sys.dm_tran_locks AS dtl
WHERE   dtl.request_session_id = @@SPID ;
ROLLBACK






BEGIN TRAN
SELECT  *
FROM    Production.Product AS p
WHERE   p.ProductID = 1 ;
--Other queries 
COMMIT



--updatelock
BEGIN TRANSACTION LockTran1
UPDATE  Sales.Currency
SET     Name = 'Euro'
WHERE   CurrencyCode = 'EUR' ;
--COMMIT

ROLLBACK

--updateloc2
--Execute from a second connection

BEGIN TRANSACTION LockTran2
--Retain an  (S) lock on the resource 
SELECT  *
FROM    Sales.Currency AS c WITH (REPEATABLEREAD)
WHERE   c.CurrencyCode = 'EUR' ;
--Allow sp_lock to be executed before second step of 
-- UPDATE statement is executed by transaction LockTran1 
WAITFOR DELAY  '00:01' ; 
COMMIT


--Execute from a third connection

SELECT  dtl.request_session_id,
        dtl.resource_database_id,
        dtl.resource_associated_entity_id,
        dtl.resource_type,
        dtl.resource_description,
        dtl.request_mode,
        dtl.request_status
FROM    sys.dm_tran_locks AS dtl ;





BEGIN TRAN
--1.Read data to be modified using (S)lock instead of (U)lock. 
--	Retain the (S)lock using REPEATABLEREAD locking hint, since 
--	the original (U)lock is retained until the conversion to 
--	(X)lock. 
SELECT  *
FROM    Sales.Currency AS c WITH (REPEATABLEREAD)
WHERE   c.CurrencyCode = 'EUR' ;
--Allow another equivalent update action to start concurrently 
WAITFOR DELAY '00:00:10' ;

--2. Modify the data by acquiring (X)lock 
UPDATE  Sales.Currency WITH (XLOCK)
SET     Name = 'EURO'
WHERE   CurrencyCode = 'EUR' ; 
COMMIT






--isix

BEGIN TRAN
DELETE  Sales.Currency
WHERE   CurrencyCode = 'ALL' ;

SELECT  tl.request_session_id,
        tl.resource_database_id,
        tl.resource_associated_entity_id,
        tl.resource_type,
        tl.resource_description,
        tl.request_mode,
        tl.request_status
FROM    sys.dm_tran_locks tl ;

ROLLBACK TRAN




ALTER DATABASE AdventureWorks2008R2
SET READ_COMMITTED_SNAPSHOT ON ;




--readcommitted
BEGIN TRANSACTION ;
SELECT  p.Color
FROM    Production.Product AS p
WHERE   p.ProductID = 711 ;




BEGIN TRANSACTION ;
UPDATE  Production.Product
SET     Color = 'Coyote'
WHERE   ProductID = 711 ;
SELECT  p.Color
FROM    Production.Product AS p
WHERE   p.ProductID = 711 ;
--COMMIT TRAN


ROLLBACK

ALTER DATABASE AdventureWorks2008R2
SET READ_COMMITTED_SNAPSHOT OFF




--repeatable
IF (SELECT  OBJECT_ID('dbo.MyProduct')
   ) IS NOT NULL 
    DROP TABLE dbo.MyProduct ; 
GO 
CREATE TABLE dbo.MyProduct
    (ProductID INT,
     Price MONEY
    ) ;
INSERT  INTO dbo.MyProduct
VALUES  (1, 15.0) ;




--repeatabletrans

--Transaction 1 from Connection 1 
DECLARE @Price INT ;
BEGIN TRAN NormailizePrice
SELECT  @Price = mp.Price
FROM    dbo.MyProduct AS mp
WHERE   mp.ProductID = 1 ;
/*Allow transaction 2 to execute*/ 
WAITFOR DELAY '00:00:10' ; 
IF @Price > 10 
    UPDATE  dbo.MyProduct
    SET     Price = Price - 10
    WHERE   ProductID = 1 ;
COMMIT 

--Transaction 2 from Connection 2
BEGIN TRAN ApplyDiscount
UPDATE  dbo.MyProduct
SET     Price = Price * 0.6 --Discount = 40%
WHERE   Price > 10 ;
COMMIT









SET TRANSACTION ISOLATION LEVEL REPEATABLE READ ; 
GO
--Transaction 1 from Connection 1 
DECLARE @Price INT ; 
BEGIN TRAN NormailizePrice
SELECT  @Price = Price
FROM    dbo.MyProduct AS mp
WHERE   mp.ProductID = 1 ;
/*Allow transaction 2 to execute*/ 
WAITFOR DELAY  '00:00:10' ; 
IF @Price > 10 
    UPDATE  dbo.MyProduct
    SET     Price = Price - 10
    WHERE   ProductID = 1 ;
COMMIT 
GO
SET TRANSACTION ISOLATION LEVEL READ COMMITTED --Back to default 
GO



--Transaction 1 from Connection 1 
DECLARE @Price INT ;
BEGIN TRAN NormailizePrice
SELECT  @Price = Price
FROM    dbo.MyProduct AS mp WITH (REPEATABLEREAD)
WHERE   mp.ProductID = 1 ;
/*Allow transaction 2 to execute*/
WAITFOR DELAY  '00:00:10'
IF @Price > 10 
    UPDATE  dbo.MyProduct
    SET     Price = Price - 10
    WHERE   ProductID = 1 ;
COMMIT 




--Transaction 1 from Connection 1 
DECLARE @Price INT ;
BEGIN TRAN NormailizePrice
SELECT  @Price = Price
FROM    dbo.MyProduct AS mp WITH (UPDLOCK)
WHERE   mp.ProductID = 1 ;
/*Allow transaction 2 to execute*/
WAITFOR DELAY  '00:00:10'
IF @Price > 10 
    UPDATE  dbo.MyProduct
    SET     Price = Price - 10
    WHERE   ProductID = 1 ;
COMMIT 





--serializable
IF (SELECT  OBJECT_ID('dbo.MyEmployees')
   ) IS NOT NULL 
    DROP TABLE dbo.MyEmployees ; 
GO 
CREATE TABLE dbo.MyEmployees
    (EmployeeID INT,
     GroupID INT,
     Salary MONEY
    ) ;
CREATE CLUSTERED INDEX i1 ON dbo.MyEmployees  (GroupID) ; 

--Employee 1 in group 10 
INSERT  INTO dbo.MyEmployees
VALUES  (1, 10, 1000) ;

--Employee 2 in group 10 
INSERT  INTO dbo.MyEmployees
VALUES  (2, 10, 1000) ; 

--Employees 3 & 4 in different groups 
INSERT  INTO dbo.MyEmployees
VALUES  (3, 20, 1000) ; 
INSERT  INTO dbo.MyEmployees
VALUES  (4, 9, 1000) ;




--bonus
DECLARE @Fund MONEY = 100,
    @Bonus MONEY,
    @NumberOfEmployees INT ;
    
BEGIN TRAN PayBonus 
SELECT  @NumberOfEmployees = COUNT(*)
FROM    dbo.MyEmployees
WHERE   GroupID = 10 ;

/*Allow transaction 2 to execute*/ 
WAITFOR DELAY  '00:00:10' ; 

IF @NumberOfEmployees > 0 
    BEGIN
        SET @Bonus = @Fund / @NumberOfEmployees ;
        UPDATE  dbo.MyEmployees
        SET     Salary = Salary + @Bonus
        WHERE   GroupID = 10 ;
        PRINT 'Fund balance =
' + CAST((@Fund - (@@ROWCOUNT * @Bonus)) AS VARCHAR(6)) + '   $' ; 
    END 
COMMIT




--newemployee
--Transaction 2 from Connection 2 
BEGIN TRAN NewEmployee
INSERT  INTO MyEmployees
VALUES  (5, 10, 1000) ;
COMMIT






SET TRANSACTION ISOLATION LEVEL SERIALIZABLE ;
GO
DECLARE @Fund MONEY = 100,
    @Bonus MONEY,
    @NumberOfEmployees INT ;
    
BEGIN TRAN PayBonus
SELECT  @NumberOfEmployees = COUNT(*)
FROM    dbo.MyEmployees
WHERE   GroupID = 10 ;

/*Allow transaction 2 to execute*/
WAITFOR DELAY  '00:00:10' ;
IF @NumberOfEmployees > 0 
    BEGIN
        SET @Bonus = @Fund / @NumberOfEmployees ;
        UPDATE  dbo.MyEmployees
        SET     Salary = Salary + @Bonus
        WHERE   GroupID = 10 ;
        
        PRINT 'Fund balance =
' + CAST((@Fund - (@@ROWCOUNT * @Bonus)) AS VARCHAR(6)) + '   $' ;
    END
COMMIT 
GO
--Back to default 
SET TRANSACTION ISOLATION LEVEL READ COMMITTED ;
GO









DECLARE @Fund MONEY = 100,
    @Bonus MONEY,
    @NumberOfEmployees INT ;
    
BEGIN TRAN PayBonus
SELECT  @NumberOfEmployees = COUNT(*)
FROM    dbo.MyEmployees WITH (HOLDLOCK)
WHERE   GroupID = 10 ;

/*Allow transaction 2 to execute*/
WAITFOR DELAY  '00:00:10' ;

IF @NumberOfEmployees > 0 
    BEGIN
        SET @Bonus = @Fund / @NumberOfEmployees
        UPDATE  dbo.MyEmployees
        SET     Salary = Salary + @Bonus
        WHERE   GroupID = 10 ;
        
        PRINT 'Fund balance =
' + CAST((@Fund - (@@ROWCOUNT * @Bonus)) AS VARCHAR(6)) + '   $' ;
    END
COMMIT 






--Transaction 2 from Connection 2 
BEGIN TRAN NewEmployee
INSERT  INTO dbo.MyEmployees
VALUES  (6, 15, 1000) ;
COMMIT


DELETE  dbo.MyEmployees
WHERE   GroupID > 10 ;



--Transaction 2 from Connection 2 
BEGIN TRAN NewEmployee
INSERT  INTO dbo.MyEmployees
VALUES  (7, 999, 1000) ;
COMMIT


IF (SELECT  OBJECT_ID('dbo.MyEmployees')
   ) IS NOT NULL 
    DROP TABLE dbo.MyEmployees ; 
GO 
CREATE TABLE dbo.MyEmployees
    (EmployeeID INT,
     GroupID INT,
     Salary MONEY
    ) ; 
CREATE CLUSTERED INDEX i1 ON dbo.MyEmployees  (EmployeeID) ;

--Employee 1 in group 10
INSERT  INTO dbo.MyEmployees
VALUES  (1, 10, 1000) ;

--Employee 2 in group 10
INSERT  INTO dbo.MyEmployees
VALUES  (2, 10, 1000) ;
  --Employees 3 & 4 in different groups 
INSERT  INTO dbo.MyEmployees
VALUES  (3, 20, 1000) ; 
INSERT  INTO dbo.MyEmployees
VALUES  (4, 9, 1000) ;









--indexlocktest
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO

CREATE TABLE dbo.Test1 (C1 INT, C2 DATETIME) ; 

INSERT  INTO dbo.Test1
VALUES  (1, GETDATE()) ;



--indexlock
BEGIN TRAN LockBehavior
UPDATE  dbo.Test1 WITH (REPEATABLEREAD)  --Hold all acquired locks
SET     C2 = GETDATE()
WHERE   C1 = 1 ;
--Observe lock behavior using sp_lock from another connection 
WAITFOR DELAY  '00:00:10' ; 
COMMIT




CREATE NONCLUSTERED INDEX iTest ON dbo.Test1(C1) ;




--Avoid KEY lock on the index rows 
ALTER INDEX iTest ON dbo.Test1 SET (ALLOW_ROW_LOCKS = OFF ,ALLOW_PAGE_LOCKS= OFF) ;

BEGIN TRAN LockBehavior
UPDATE  dbo.Test1 WITH (REPEATABLEREAD)  --Hold all acquired locks
SET     C2 = GETDATE()
WHERE   C1 = 1 ;

--Observe lock behavior using sys.dm_tran_locks from another connection 
WAITFOR DELAY  '00:00:10' ; 
COMMIT

ALTER INDEX iTest ON dbo.Test1 SET (ALLOW_ROW_LOCKS = ON ,ALLOW_PAGE_LOCKS= ON) ;



CREATE CLUSTERED INDEX iTest ON dbo.Test1(C1) WITH DROP_EXISTING ;




DECLARE @NumberOfEmployees int
BEGIN TRAN
SELECT  @NumberOfEmployees = COUNT(*)
FROM    dbo.MyEmployees WITH (HOLDLOCK)
WHERE   GroupID = 10;
WAITFOR DELAY '00:00:10'
ROLLBACK TRAN

DROP INDEX i1 ON dbo.MyEmployees





--blocker
SELECT  dtl.request_session_id AS WaitingSessionID,
        der.blocking_session_id AS BlockingSessionID,
        dowt.resource_description,
        der.wait_type,
        dowt.wait_duration_ms,
        DB_NAME(dtl.resource_database_id) AS DatabaseName,
        dtl.resource_associated_entity_id AS WaitingAssociatedEntity,
        dtl.resource_type AS WaitingResourceType,
        dtl.request_type AS WaitingRequestType,
        dest.[text] AS WaitingTSql,
        dtlbl.request_type BlockingRequestType,
        destbl.[text] AS BlockingTsql
FROM    sys.dm_tran_locks AS dtl
JOIN    sys.dm_os_waiting_tasks AS dowt
        ON dtl.lock_owner_address = dowt.resource_address
JOIN    sys.dm_exec_requests AS der
        ON der.session_id = dtl.request_session_id
CROSS APPLY sys.dm_exec_sql_text(der.sql_handle) AS dest
LEFT JOIN sys.dm_exec_requests derbl
        ON derbl.session_id = dowt.blocking_session_id
OUTER APPLY sys.dm_exec_sql_text(derbl.sql_handle) AS destbl
LEFT JOIN sys.dm_tran_locks AS dtlbl
        ON derbl.session_id = dtlbl.request_session_id ;


--blockit
IF (SELECT  OBJECT_ID('dbo.BlockTest')
   ) IS NOT NULL 
    DROP TABLE dbo.BlockTest ; 
GO 

CREATE TABLE dbo.BlockTest
    (C1 INT,
     C2 INT,
     C3 DATETIME
    ) ; 

INSERT  INTO dbo.BlockTest
VALUES  (11, 12, GETDATE()),
        (21, 22, GETDATE()) ;




--Listing 12.1
BEGIN TRAN User1
UPDATE  dbo.BlockTest
SET     C3 = GETDATE() ; 

--ROLLBACK

--Listing 12.2
BEGIN TRAN User2
SELECT  C2
FROM    dbo.BlockTest
WHERE   C1 = 11 ;
COMMIT





EXEC sp_configure 
    'blocked process threshold',
    5 ; 
RECONFIGURE ;





ALTER EVENT SESSION Blocking
ON SERVER
STATE = START;

WAITFOR DELAY '00:10';

ALTER EVENT SESSION Blocking
ON SERVER
STATE = STOP;








--Chapter 13

DECLARE @retry AS TINYINT = 1,
    @retrymax AS TINYINT = 2,
    @retrycount AS TINYINT = 0 ;
WHILE @retry = 1
    AND @retrycount <= @retrymax 
    BEGIN
        SET @retry = 0 ;
            
        BEGIN TRY
            UPDATE  HumanResources.Employee
            SET     LoginID = '54321'
            WHERE   BusinessEntityID = 100 ;
        END TRY
        BEGIN CATCH
            IF (ERROR_NUMBER() = 1205) 
                BEGIN
                    SET @retrycount = @retrycount + 1 ;
                        SET @retry = 1 ;
                    END
        END CATCH
    END



DBCC TRACEON (1222, -1) ;
DBCC TRACEON (1204, -1) ;


--deadlock1
BEGIN TRAN
UPDATE  Purchasing.PurchaseOrderHeader
SET     Freight = Freight * 0.9 -- 10% discount on shipping
WHERE   PurchaseOrderID = 1255 ;

--start this after running second connection
UPDATE  Purchasing.PurchaseOrderDetail
SET     OrderQty = 4
WHERE   ProductID = 448
        AND PurchaseOrderID = 1255 ;




--deadlock2
--in second connection
BEGIN TRANSACTION
UPDATE  Purchasing.PurchaseOrderDetail
SET     OrderQty = 2
WHERE   ProductID = 448
        AND PurchaseOrderID = 1255 ;








--Chapter 14

--firstcursor

--Associate a SELECT statement to a cursor and define the 
--cursor's characteristics 
DECLARE MyCursor CURSOR /*<cursor characteristics>*/
FOR
SELECT  adt.AddressTypeID,
        adt.Name,
        adt.ModifiedDate
FROM    Person.AddressType adt ;


--Open the cursor to access the result set returned by the 
--SELECT statement 
OPEN MyCursor ;

--Retrieve one row at a time from the result set returned by 
--the SELECT statement 
DECLARE @AddressTypeId INT,
    @Name VARCHAR(50),
    @ModifiedDate DATETIME ;

FETCH NEXT FROM MyCursor INTO @AddressTypeId, @Name, @ModifiedDate ;

WHILE @@FETCH_STATUS = 0 
    BEGIN
        PRINT 'NAME =   ' + @Name ;

--Optionally, modify the row through the cursor
        UPDATE  Person.AddressType
        SET     Name = Name + 'z'
        WHERE CURRENT OF MyCursor ;
        
        FETCH NEXT FROM MyCursor INTO @AddressTypeId, @Name, @ModifiedDate ;
    END 

--Close the cursor and release all resources assigned to the 
--cursor 
CLOSE MyCursor ;
DEALLOCATE MyCursor ;




UPDATE  Person.AddressType
SET     [Name] = LEFT([Name], LEN([Name]) - 1) ;





DECLARE MyCursor CURSOR READ_ONLY
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1 ;




DECLARE MyCursor CURSOR OPTIMISTIC
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1 ;


DECLARE MyCursor CURSOR SCROLL_LOCKS
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1 ;


DECLARE MyCursor CURSOR FAST_FORWARD
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1 ;

DECLARE MyCursor CURSOR STATIC
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1 ;



DECLARE MyCursor CURSOR KEYSET
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1 ;




DECLARE MyCursor CURSOR DYNAMIC
FOR
SELECT  adt.Name
FROM    Person.AddressType AS adt
WHERE   adt.AddressTypeID = 1 ;





USE AdventureWorks2008R2 ; 
GO
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO

CREATE TABLE dbo.Test1 (C1 INT, C2 CHAR(996)) ;

CREATE CLUSTERED INDEX Test1Index ON dbo.Test1 (C1) ;

INSERT  INTO dbo.Test1
VALUES  (1, '1') ,
        (2, '2') ; 
GO









--Add 100000 rows to the test table 
SELECT TOP 100000
        IDENTITY( INT,1,1 ) AS n
INTO    #Tally
FROM    Master.dbo.SysColumns scl,
        Master.dbo.SysColumns sc2 ;

INSERT  INTO dbo.Test1
        (C1, C2)
        SELECT  n,
                n
        FROM    #Tally AS t ; 
DROP TABLE #Tally ;
GO

SELECT  dtl.request_session_id,
        dtl.resource_database_id,
        dtl.resource_associated_entity_id,
        dtl.resource_type,
        dtl.resource_description,
        dtl.request_mode,
        dtl.request_status
FROM    sys.dm_tran_locks AS dtl ;





IF ( SELECT OBJECT_ID('dbo.spTotalLoss_CursorBased')
   ) IS NOT NULL 
    DROP PROC dbo.spTotalLoss_CursorBased ;
GO

CREATE PROC dbo.spTotalLoss_CursorBased
AS --Declare a T-SQL cursor with default settings,  i.e.,  fast 
--forward-only to retrieve products that have been discarded 
    DECLARE ScrappedProducts CURSOR
    FOR
        SELECT  p.ProductID ,
                wo.ScrappedQty ,
                p.ListPrice
        FROM    Production.WorkOrder AS wo
                JOIN Production.ScrapReason AS sr ON wo.ScrapReasonID = sr.ScrapReasonID
                JOIN Production.Product AS p ON wo.ProductID = p.ProductID ; 
		
--Open the cursor to process one product at a time 
    OPEN ScrappedProducts ;

    DECLARE @MoneyLostPerProduct MONEY = 0 ,
        @TotalLoss MONEY = 0 ;

--Calculate money lost per product by processing one product 
--at a time
    DECLARE @ProductId INT ,
        @UnitsScrapped SMALLINT ,
        @ListPrice MONEY ;

    FETCH NEXT FROM ScrappedProducts INTO @ProductId, @UnitsScrapped,
        @ListPrice ;

    WHILE @@FETCH_STATUS = 0 
        BEGIN
            SET @MoneyLostPerProduct = @UnitsScrapped * @ListPrice ; --Calculate total loss
            SET @TotalLoss = @TotalLoss + @MoneyLostPerProduct ;
        
            FETCH NEXT FROM ScrappedProducts INTO @ProductId, @UnitsScrapped,
                @ListPrice ;
        END

--Determine status
    IF ( @TotalLoss > 5000 ) 
        SELECT  'We are bankrupt!' AS Status ;
    ELSE 
        SELECT  'We are safe!' AS Status ;
--Close the cursor and release all resources assigned to the cursor
    CLOSE ScrappedProducts ;
    DEALLOCATE ScrappedProducts ; 
GO



EXEC spTotalLoss_CursorBased





SELECT  SUM(page_count)
FROM    sys.dm_db_index_physical_stats(DB_ID(N'AdventureWorks2008R2'),
OBJECT_ID(N'Production.WorkOrder'),DEFAULT, DEFAULT, DEFAULT) ; 

SELECT *
'


SELECT OBJECT_ID('Production.WorkOrder')





--Chapter 16

USE AdventureWorks2008R2;
GO

CREATE PROCEDURE dbo.spr_ShoppingCart
    @ShoppingCartId VARCHAR(50)
AS 
--provides the output from the shopping cart including the line total
SELECT  sci.Quantity,
        p.ListPrice,
        p.ListPrice * sci.Quantity AS LineTotal,
        p.[Name]
FROM    Sales.ShoppingCartItem AS sci
JOIN    Production.Product AS p
        ON sci.ProductID = p.ProductID
WHERE   sci.ShoppingCartID = @ShoppingCartId ; 
GO

CREATE PROCEDURE dbo.spr_ProductBySalesOrder @SalesOrderID INT
AS 
/*provides a list of products from a particular sales order, 
and provides line ordering by modified date but ordered by product name*/ 

SELECT  ROW_NUMBER() OVER (ORDER BY sod.ModifiedDate) AS LineNumber,
        p.[Name],
        sod.LineTotal
FROM    Sales.SalesOrderHeader AS soh
JOIN    Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
JOIN    Production.Product AS p
        ON sod.ProductID = p.ProductID
WHERE   soh.SalesOrderID = @SalesOrderID
ORDER BY p.[Name] ASC ; 
GO

CREATE PROCEDURE dbo.spr_PersonByFirstName
    @FirstName NVARCHAR(50)
AS 
--gets anyone by first name from the Person table
SELECT  p.BusinessEntityID,
        p.Title,
        p.LastName,
        p.FirstName,
        p.PersonType
FROM    Person.Person AS p
WHERE   p.FirstName = @FirstName ; 
GO


CREATE PROCEDURE dbo.spr_ProductTransactionsSinceDate
    @LatestDate DATETIME,
    @ProductName NVARCHAR(50)
AS 
--Gets the latest transaction against all products that have a transaction
SELECT  p.Name,
        th.ReferenceOrderID,
        th.ReferenceOrderLineID,
        th.TransactionType,
        th.Quantity
FROM    Production.Product AS p
JOIN    Production.TransactionHistory AS th
        ON p.ProductID = th.ProductID AND
           th.TransactionID = (SELECT TOP (1)
                                        th2.TransactionID
                               FROM     Production.TransactionHistory th2
                               WHERE    th2.ProductID = p.ProductID
                               ORDER BY th2.TransactionID DESC
                              )
WHERE   th.TransactionDate > @LatestDate AND
        p.Name LIKE @ProductName ; 
GO


CREATE PROCEDURE dbo.spr_PurchaseOrderBySalesPersonName @LastName NVARCHAR(50)
AS 
SELECT  poh.PurchaseOrderID,
        poh.OrderDate,
        pod.LineTotal,
        p.[Name] AS ProductName,
        e.JobTitle,
        per.LastName + ', ' + per.FirstName AS SalesPerson
FROM    Purchasing.PurchaseOrderHeader AS poh
JOIN    Purchasing.PurchaseOrderDetail AS pod
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
JOIN    Production.Product AS p
        ON pod.ProductID = p.ProductID
JOIN    HumanResources.Employee AS e
        ON poh.EmployeeID = e.BusinessEntityID
JOIN    Person.Person AS per
        ON e.BusinessEntityID = per.BusinessEntityID
WHERE   per.LastName LIKE @LastName
ORDER BY per.LastName,
        per.FirstName ; 
GO



EXEC dbo.spr_ShoppingCart 
    '20621' ;
GO
EXEC dbo.spr_ProductBySalesOrder 
    43867 ;
GO
EXEC dbo.spr_PersonByFirstName 
    'Gretchen' ;
GO
EXEC dbo.spr_ProductTransactionsSinceDate 
    @LatestDate = '9/1/2004',
    @ProductName = 'Hex Nut%' ;
GO
EXEC dbo.spr_PurchaseOrderBySalesPersonName 
    @LastName = 'Hill%' ;
GO









SELECT *
FROM sys.fn_xe_file_target_read_file('C:\Program Files\Microsoft SQL Server\MSSQL11.MSSQLSERVER\MSSQL\Log\Query Performance_0_129739039337880000*xel', null, null, null)



IF (SELECT  OBJECT_ID('dbo.ExEvents')
   ) IS NOT NULL 
    DROP TABLE dbo.ExEvents ;
GO 
WITH    xEvents
          AS (SELECT    object_name AS xEventName,
                        CAST (event_data AS xml) AS xEventData
              FROM      sys.fn_xe_file_target_read_file('D:\Apath\Query Performance Tuning*.xel',
                                                        NULL, NULL, NULL)
             )
    SELECT  xEventName,
            xEventData.value('(/event/data[@name=''duration'']/value)[1]',
                             'bigint') Duration,
            xEventData.value('(/event/data[@name=''physical_reads'']/value)[1]',
                             'bigint') PhysicalReads,
            xEventData.value('(/event/data[@name=''logical_reads'']/value)[1]',
                             'bigint') LogicalReads,
            xEventData.value('(/event/data[@name=''cpu_time'']/value)[1]',
                             'bigint') CpuTime,
            CASE xEventName
              WHEN 'sql_batch_completed'
              THEN xEventData.value('(/event/data[@name=''batch_text'']/value)[1]',
                                    'varchar(max)')
              WHEN 'rpc_completed'
              THEN xEventData.value('(/event/data[@name=''statement'']/value)[1]',
                                    'varchar(max)')
            END AS SQLText,
            xEventData.value('(/event/data[@name=''query_plan_hash'']/value)[1]',
                             'binary(8)') QueryPlanHash
    INTO    dbo.ExEvents
    FROM    xEvents ;


SELECT  *
FROM    dbo.ExEvents AS ee
ORDER BY ee.Duration DESC ;


SELECT  ee.BatchText,
        SUM(Duration) AS SumDuration,
        AVG(Duration) AS AvgDuration,
        COUNT(Duration) AS CountDuration
FROM    dbo.ExEvents AS ee
GROUP BY ee.BatchText ;



DBCC FREEPROCCACHE() ;
DBCC DROPCLEANBUFFERS ;
GO
SET STATISTICS TIME ON ;
GO
SET STATISTICS IO ON ;
GO
EXEC dbo.spr_PurchaseOrderBySalesPersonName 
    @LastName = 'Hill%' ;
GO
SET STATISTICS TIME OFF ;
GO
SET STATISTICS IO OFF ;
GO






DBCC SHOW_STATISTICS('Purchasing.PurchaseOrderHeader', 
'PK_PurchaseOrderHeader_PurchaseOrderID') ;




--showcontig
SELECT  s.avg_fragmentation_in_percent,
        s.fragment_count,
        s.page_count,
        s.avg_page_space_used_in_percent,
        s.record_count,
        s.avg_record_size_in_bytes,
        s.index_id
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2008R2'),
                                       OBJECT_ID(N'Purchasing.PurchaseOrderHeader'),
                                       NULL, NULL, 'Sampled') AS s
WHERE   s.record_count > 0
ORDER BY s.index_id ;




SELECT  s.avg_fragmentation_in_percent,
        s.fragment_count,
        s.page_count,
        s.avg_page_space_used_in_percent,
        s.record_count,
        s.avg_record_size_in_bytes,
        s.index_id
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2008R2'),
                                       OBJECT_ID(N'Purchasing.PurchaseOrderDetail'),
                                       NULL, NULL, 'Sampled') AS s
WHERE   s.record_count > 0
ORDER BY s.index_id ;
SELECT  s.avg_fragmentation_in_percent,
        s.fragment_count,
        s.page_count,
        s.avg_page_space_used_in_percent,
        s.record_count,
        s.avg_record_size_in_bytes,
        s.index_id
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2008R2'),
                                       OBJECT_ID(N'Production.Product'),
                                       NULL, NULL, 'Sampled') AS s
WHERE   s.record_count > 0
ORDER BY s.index_id ;
SELECT  s.avg_fragmentation_in_percent,
        s.fragment_count,
        s.page_count,
        s.avg_page_space_used_in_percent,
        s.record_count,
        s.avg_record_size_in_bytes,
        s.index_id
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2008R2'),
                                       OBJECT_ID(N'Person.Employee'),
                                       NULL, NULL, 'Sampled') AS s
WHERE   s.record_count > 0
ORDER BY s.index_id ;
SELECT  s.avg_fragmentation_in_percent,
        s.fragment_count,
        s.page_count,
        s.avg_page_space_used_in_percent,
        s.record_count,
        s.avg_record_size_in_bytes,
        s.index_id
FROM    sys.dm_db_index_physical_stats(DB_ID('AdventureWorks2008R2'),
                                       OBJECT_ID(N'Person.Person'),
                                       NULL, NULL, 'Sampled') AS s
WHERE   s.record_count > 0
ORDER BY s.index_id ;




DECLARE @DBName NVARCHAR(255),
    @TableName NVARCHAR(255),
    @SchemaName NVARCHAR(255),
    @IndexName NVARCHAR(255),
    @PctFrag DECIMAL,
    @Defrag NVARCHAR(MAX)
IF EXISTS ( SELECT  *
            FROM    sys.objects
            WHERE   OBJECT_ID = OBJECT_ID(N'#Frag') ) 
    DROP TABLE #Frag
CREATE TABLE #Frag (
     DBName NVARCHAR(255),
     TableName NVARCHAR(255),
     SchemaName NVARCHAR(255),
     IndexName NVARCHAR(255),
     AvgFragment DECIMAL
    )
	
EXEC sys.sp_MSforeachdb
    'INSERT INTO #Frag ( DBName, TableName, SchemaName, IndexName, AvgFragment ) 
	SELECT ''?'' AS DBName ,t.Name AS TableName ,sc.Name AS SchemaName ,i.name AS 
	IndexName ,s.avg_fragmentation_in_percent FROM  
	?.sys.dm_db_index_physical_stats(DB_ID(''?''), NULL, NULL,
	NULL, ''Sampled'') AS s JOIN ?.sys.indexes i ON s.Object_Id = i.Object_id
	AND s.Index_id = i.Index_id JOIN ?.sys.tables t ON i.Object_id = t.Object_Id 
	JOIN ?.sys.schemas sc ON t.schema_id = sc.SCHEMA_ID WHERE s.avg_fragmentation_in_percent > 20 
	AND t.TYPE = ''U'' AND s.page_count > 8 ORDER BY TableName,IndexName';
DECLARE cList CURSOR
FOR
SELECT  *
FROM    #Frag;
OPEN cList;
FETCH NEXT FROM cList
INTO @DBName, @TableName, @SchemaName, @IndexName, @PctFrag;
WHILE @@FETCH_STATUS = 0 
    BEGIN
        IF @PctFrag BETWEEN 20.0 AND 40.0 
            BEGIN
                SET @Defrag = N'ALTER INDEX ' + @IndexName + ' ON ' + @DBName +
                    '.' + @SchemaName + '.' + @TableName + ' REORGANIZE';
                EXEC sp_executesql 
                    @Defrag;
                PRINT 'Reorganize index: ' + @DBName + '.' + @SchemaName + '.' +
                    @TableName + '.' + @IndexName;
            END
        ELSE 
            IF @PctFrag > 40.0 
                BEGIN
                    SET @Defrag = N'ALTER INDEX ' + @IndexName + '  ON  ' +
                        @DBName + '.' + @SchemaName + '.' + @TableName +
                        '   REBUILD';
                    EXEC sp_executesql 
                        @Defrag;
                    PRINT 'Rebuild index:   ' + @DBName + '.' + @SchemaName +
                        '.' + @TableName + '.' + @IndexName;
                END
        FETCH NEXT FROM cList
INTO @DBName, @TableName, @SchemaName, @IndexName, @PctFrag;
    END
CLOSE cList;
DEALLOCATE cList;
DROP TABLE #Frag;






--alterindex
CREATE NONCLUSTERED INDEX [IX_PurchaseOrderHeader_EmployeeID] 
ON [Purchasing].[PurchaseOrderHeader]  ([EmployeeID] ASC)
INCLUDE  (OrderDate)
WITH (
DROP_EXISTING = ON) ON [PRIMARY] ;
GO



DBCC FREEPROCCACHE() ;
DBCC DROPCLEANBUFFERS ;
GO
EXEC dbo.spr_PurchaseOrderBySalesPersonName 
    @LastName = 'Hill%' ;






ALTER PROCEDURE dbo.spr_PurchaseOrderBySalesPersonName @LastName NVARCHAR(50)
AS 
SELECT  poh.PurchaseOrderID,
        poh.OrderDate,
        pod.LineTotal,
        p.[Name] AS ProductName,
        e.JobTitle,
        per.LastName + ',   ' + per.FirstName AS Salesperson
FROM    Purchasing.PurchaseOrderHeader AS poh
INNER LOOP JOIN Purchasing.PurchaseOrderDetail AS pod
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
JOIN    Production.Product AS p
        ON pod.ProductID = p.ProductID
JOIN    HumanResources.Employee AS e
        ON poh.EmployeeID = e.BusinessEntityID
JOIN    Person.Person AS per
        ON e.BusinessEntityID = per.BusinessEntityID
WHERE   per.LastName LIKE @LastName
ORDER BY per.LastName,
        per.FirstName;



ALTER PROCEDURE [dbo].[spr_PurchaseOrderBySalesPersonName] @LastName NVARCHAR(50)
AS 
SELECT  poh.PurchaseOrderID,
        poh.OrderDate,
        pod.LineTotal,
        p.[Name] AS ProductName,
        e.JobTitle,
        per.LastName + ', ' + per.FirstName AS Salesperson
FROM    Purchasing.PurchaseOrderHeader AS poh
INNER MERGE JOIN Purchasing.PurchaseOrderDetail AS pod
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
INNER MERGE JOIN Production.Product AS p
        ON pod.ProductID = p.ProductID
JOIN    HumanResources.Employee AS e
        ON poh.EmployeeID = e.BusinessEntityID
JOIN    Person.Person AS per
        ON e.BusinessEntityID = per.BusinessEntityID
WHERE   per.LastName LIKE @LastName
ORDER BY per.LastName,
        per.FirstName ;




ALTER PROCEDURE dbo.spr_PurchaseOrderBySalesPersonName @LastName NVARCHAR(50)
AS 
SELECT  poh.PurchaseOrderID,
        poh.OrderDate,
        pod.LineTotal,
        p.[Name] AS ProductName,
        e.JobTitle,
        per.LastName + ', ' + per.FirstName AS SalesPerson
FROM    Purchasing.PurchaseOrderHeader AS poh
JOIN    Purchasing.PurchaseOrderDetail AS pod
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
JOIN    Production.Product AS p
        ON pod.ProductID = p.ProductID
JOIN    HumanResources.Employee AS e
        ON poh.EmployeeID = e.BusinessEntityID
JOIN    Person.Person AS per
        ON e.BusinessEntityID = per.BusinessEntityID
WHERE   per.LastName LIKE @LastName
ORDER BY per.LastName,
        per.FirstName ;




CREATE INDEX IX_Test 
ON Purchasing.PurchaseOrderDetail 
(PurchaseOrderID, ProductID,  LineTotal);


DBCC FREEPROCCACHE() ;
DBCC DROPCLEANBUFFERS ;
GO
EXEC dbo.spr_PurchaseOrderBySalesPersonName 
    @LastName = 'Hill%' ;



ALTER PROCEDURE dbo.spr_PurchaseOrderBySalesPersonName
    @BusinessEntityID int
AS 
SELECT  poh.PurchaseOrderID,
        poh.OrderDate,
        pod.LineTotal,
        p.[Name] AS ProductName,
        e.JobTitle,
        per.LastName + ', ' + per.FirstName AS SalesPerson
FROM    Purchasing.PurchaseOrderHeader AS poh
JOIN    Purchasing.PurchaseOrderDetail AS pod
        ON poh.PurchaseOrderID = pod.PurchaseOrderID
JOIN    Production.Product AS p
        ON pod.ProductID = p.ProductID
JOIN    HumanResources.Employee AS e
        ON poh.EmployeeID = e.BusinessEntityID
JOIN    Person.Person AS per
        ON e.BusinessEntityID = per.BusinessEntityID
WHERE   e.BusinessEntityID = @BusinessEntityID
ORDER BY per.LastName,
        per.FirstName ;


--DBCC FREEPROCCACHE() ;
DBCC DROPCLEANBUFFERS ;
GO
EXEC dbo.spr_PurchaseOrderBySalesPersonName @BusinessEntityID = 260;





EXEC dbo.spr_ShoppingCart 
    '20621' ;
GO
EXEC dbo.spr_ProductBySalesOrder 
    43867 ;
GO
EXEC dbo.spr_PersonByFirstName 
    'Gretchen' ;
GO
EXEC dbo.spr_ProductTransactionsSinceDate 
    @LatestDate = '9/1/2004',
    @ProductName = 'Hex Nut%' ;
GO
EXEC dbo.spr_PurchaseOrderBySalesPersonName 
   @BusinessEntityID = 260 ;
GO


DBCC freeproccache()


--errors
INSERT  INTO Purchasing.PurchaseOrderDetail
        (PurchaseOrderID,
         DueDate,
         OrderQty,
         ProductID,
         UnitPrice,
         ReceivedQty,
         RejectedQty,
         ModifiedDate
        )
VALUES  (1066,
         '1/1/2009',
         1,
         42,
         98.6,
         5,
         4,
         '1/1/2009'
        ) ;
GO

SELECT  p.[Name],
        ps.[Name]
FROM    Production.Product AS p,
        Production.ProductSubcategory AS ps  ; 
GO










--Chapter 17


CREATE    NONCLUSTERED INDEX [AK_Product_Name] 
ON [Production].[Product]  ([Name] ASC) WITH  (
DROP_EXISTING = ON) 
ON    [PRIMARY] ; 
GO




SELECT DISTINCT
        (p.[Name])
FROM    Production.Product AS p ;



CREATE UNIQUE NONCLUSTERED INDEX [AK_Product_Name] 
ON [Production].[Product]([Name] ASC) 
WITH (
DROP_EXISTING = ON) 
ON    [PRIMARY] ; 
GO




--Create two test tables 
IF (SELECT  OBJECT_ID('dbo.Test1')
   ) IS NOT NULL 
    DROP TABLE dbo.Test1 ; 
GO
CREATE TABLE dbo.Test1 (
     C1 INT,
     C2 INT CHECK (C2 BETWEEN 10 AND 20)
    ) ;
INSERT  INTO dbo.Test1
VALUES  (11, 12) ; 
GO
IF (SELECT  OBJECT_ID('dbo.Test2')
   ) IS NOT NULL 
    DROP TABLE dbo.Test2 ; 
GO
CREATE TABLE dbo.Test2 (C1 INT, C2 INT) ;
INSERT  INTO dbo.Test2
VALUES  (101, 102) ;




SELECT  T1.C1,
        T1.C2,
        T2.C2
FROM    dbo.Test1 AS T1
JOIN    dbo.Test2 AS T2
        ON T1.C1 = T2.C2 AND
           T1.C2 = 20 ;

GO
SELECT  T1.C1,
        T1.C2,
        T2.C2
FROM    dbo.Test1 AS T1
JOIN    dbo.Test2 AS T2
        ON T1.C1 = T2.C2 AND
           T1.C2 = 30 ;




SELECT  p.*
FROM    Production.Product AS p
WHERE   p.[Name] LIKE '%Caps' ;








SELECT  soh.SalesOrderNumber
FROM    Sales.SalesOrderHeader AS soh
WHERE   'S05' = LEFT(SalesOrderNumber, 3) ;

SELECT  soh.SalesOrderNumber
FROM    Sales.SalesOrderHeader AS soh
WHERE   SalesOrderNumber LIKE 'S05%' ;



