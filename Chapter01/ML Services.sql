-- Configuration

USE master
GO

EXEC SP_CONFIGURE 'external scripts enabled', 1
RECONFIGURE
GO

EXEC sp_execute_external_script @language =N'R',
	@script=N'OutputDataSet <-InputDataSet',
	@input_data_1 =N'SELECT 1 as CheckToSeeIfRIsWorking'
WITH RESULT SETS (([CheckToSeeIfRIsWorking] int not null));
GO

EXEC sp_execute_external_script  @language =N'Python',
	@script=N'OutputDataSet = InputDataSet',
	@input_data_1 = N'SELECT 1 AS CheckToSeeIfPythonIsWorking'
WITH RESULT SETS ((CheckToSeeIfPythonIsWorking int not null));
GO

SELECT * 
FROM sys.resource_governor_resource_pools 
WHERE name = 'default';

SELECT * 
FROM sys.resource_governor_external_resource_pools 
WHERE name = 'default';
GO

EXEC SP_CONFIGURE 'show advanced options',1
RECONFIGURE
GO

EXEC SP_CONFIGURE 'max server memory (MB)' , 20000
RECONFIGURE
GO

ALTER EXTERNAL RESOURCE POOL "default" 
WITH (max_memory_percent = 40);

ALTER RESOURCE GOVERNOR	RECONFIGURE;
GO

SELECT * 
FROM sys.resource_governor_external_resource_pools 
WHERE name = 'default';
GO

-- Security
USE master
GO
CREATE LOGIN [Analyst] WITH PASSWORD=N'Str0ngP@$$w0rd'
	,DEFAULT_DATABASE=[ML]
	,CHECK_EXPIRATION=ON
	,CHECK_POLICY=ON;
GO

USE ML
GO
CREATE USER [Analyst] FOR LOGIN [Analyst];
GO

EXECUTE AS USER = 'Analyst';
GO
EXEC sp_execute_external_script
@language = N'R'
,@script = N'OutputDataSet<- InputDataSet'
,@input_data_1 = N'SELECT 1;'
GO
REVERT;
GO

GRANT EXECUTE ANY EXTERNAL SCRIPT 
TO [Analyst];
GO

EXECUTE AS USER = 'Analyst';
GO
SELECT *
FROM [BenfordFraud].[Invoices];
GO
REVERT;
GO

GRANT SELECT, INSERT, UPDATE, DELETE 
ON SCHEMA::[BenfordFraud]
TO [Analyst];
GO

SET STATISTICS TIME ON
GO
SELECT COUNT(*)
FROM [OnlineSales].[inventory];
GO
SET STATISTICS TIME OFF
GO

EXEC sp_estimate_data_compression_savings 
	@schema_name = 'OnlineSales',
	@object_name ='inventory',
	@index_id = NULL,
	@partition_number = NULL,
	@data_compression = 'NONE';
GO

EXEC sp_estimate_data_compression_savings 
	@schema_name = 'OnlineSales',
	@object_name ='inventory',
	@index_id = NULL,
	@partition_number = NULL,
	@data_compression = 'COLUMNSTORE_ARCHIVE';
GO
DROP INDEX [CCI_Invoices]
ON [BenfordFraud].[Invoices];
GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Invoices]
ON [BenfordFraud].[Invoices];
GO

-- Get list of installed R packages:
if (select CAST(SERVERPROPERTY('IsAdvancedAnalyticsInstalled') as int) & CAST(value_in_use as int)
   from sys.configurations
  where name = 'external scripts enabled') = 1
begin
 exec sp_execute_external_script
   @language = N'R'
  ,@script = N'
 OutputDataSet <- data.frame(installed.packages()[,c("Package", "Version", "Depends", "License", "Built", "LibPath")]);'
 with result sets 
  ((Package nvarchar(255), Version nvarchar(100), Depends nvarchar(4000), License nvarchar(1000), Built nvarchar(100), LibPath nvarchar(2000)));
end;
go

-- Get list of installed Python packages:

if (select CAST(SERVERPROPERTY('IsAdvancedAnalyticsInstalled') as int) & CAST(value_in_use as int)
   from sys.configurations
  where name = 'external scripts enabled') = 1
 and exists(select * from sys.dm_external_script_execution_stats where language = 'Python')
begin
 -- Get list of Python packages installed with SQL Server
 exec sp_execute_external_script
  @language = N'Python'
  , @script = N'
import pip
OutputDataSet = pandas.DataFrame([(i.key, i.version, i.location) for i in pip.get_installed_distributions()])'
 with result sets ((Package nvarchar(128), Version nvarchar(128), Location nvarchar(1000)));
end;

-- Will cause an error on 2016/2017
EXECUTE sp_execute_external_script
 @language = N'R'
,@script = N'install.packages("AUC")';
GO

-- Path to libraries on your server
EXECUTE sp_execute_external_script
@language = N'R'
,@script = N'OutputDataSet <- data.frame(.libPaths());'
WITH RESULT SETS (([DefaultLibraryName] VARCHAR(MAX) NOT NULL));
GO

--Using the rxInstallPackages
EXECUTE	sp_execute_external_script	
 @language = N'R'
,@script = N'
packagesToInstall <- c("Hmisc","AUC")
library(MicrosoftML)
RSqlServerCC <- RxInSqlServer(connectionString = "Driver=SQL Server; Server=MS;Database=ML;Trusted_Connection=True;")
rxInstallPackages(pkgs = packagesToInstall, scope = "shared");';
GO

--Using EXTERNAL LIBRARY
CREATE EXTERNAL LIBRARY AUC 
FROM (CONTENT = 'C:\ML\Chapter01\AUC_0.3.0.zip') 
WITH (LANGUAGE = 'R'); 
GO

EXEC sp_execute_external_script 
	@language =N'R', 
	@script=N'library(AUC)'
GO
