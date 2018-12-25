USE ML
GO

-- Feature engineering
SELECT [Name],
	SUBSTRING([Name], CHARINDEX(',', [Name])+1
		,CHARINDEX('.',[Name],CHARINDEX(',', [Name])+1) - CHARINDEX(',', [Name])-1)	AS Title
FROM [Titanic].[Train];
GO

SELECT VendorNumber, InvoiceAmount
		,ROUND(CASE
					WHEN InvoiceAmount >= 1000000000 THEN InvoiceAmount / 1000000000
					WHEN InvoiceAmount >= 100000000 THEN InvoiceAmount / 100000000
					WHEN InvoiceAmount >= 10000000 THEN InvoiceAmount / 10000000
					WHEN InvoiceAmount >= 1000000 THEN InvoiceAmount / 1000000
					WHEN InvoiceAmount >= 100000 THEN InvoiceAmount / 100000
					WHEN InvoiceAmount >= 10000 THEN InvoiceAmount / 10000
					WHEN InvoiceAmount >= 1000 THEN InvoiceAmount / 1000
					WHEN InvoiceAmount >= 100 THEN InvoiceAmount / 100
					WHEN InvoiceAmount >= 10 THEN InvoiceAmount / 10
					WHEN InvoiceAmount < 10 THEN InvoiceAmount
				END, 0, 1) as Digits
			, COUNT(*) OVER(PARTITION BY VendorNumber) as #Transactions
FROM [BenfordFraud].[Invoices]
ORDER BY NEWID();
GO

ALTER FUNCTION [NYCTaxi].[fnCalculateDistance] (@Lat1 float, @Long1 float, @Lat2 float, @Long2 float)
RETURNS float
AS
BEGIN
  DECLARE @distance decimal(28, 10)
  -- Convert to radians
  SET @Lat1 = @Lat1 / 57.2958
  SET @Long1 = @Long1 / 57.2958
  SET @Lat2 = @Lat2 / 57.2958
  SET @Long2 = @Long2 / 57.2958
  -- Calculate distance
  SET @distance = (SIN(@Lat1) * SIN(@Lat2)) + (COS(@Lat1) * COS(@Lat2) * COS(@Long2 - @Long1))
  --Convert to miles
  IF @distance <> 0
  BEGIN
    SET @distance = 3958.75 * ATAN(SQRT(1 - POWER(@distance, 2)) / @distance);
  END
  RETURN @distance
END
GO

-- Datetime features extraction
DECLARE @date DATETIME  = GETDATE();
SELECT
  [Date]        = @date,
  [Year]        = DATEPART(YEAR, @date),
  [DayOfYear]   = CONVERT(SMALLINT, DATEPART(DAYOFYEAR, @date)),
  [Quarter]     = CONVERT(TINYINT, DATEPART(QUARTER, @date)),
  [Month]       = CONVERT(TINYINT, DATEPART(MONTH, @date)),
  [Week]	    = CONVERT(TINYINT, DATEPART(WEEK, @date)),
  [Day]         = CONVERT(TINYINT,  DATEPART(DAY, @date)),
  [Weekday]     = CONVERT(TINYINT, DATEPART(WEEKDAY, @date)),
  [IsWeekend]   = CONVERT(BIT, CASE WHEN  DATEPART(WEEKDAY, @date) IN (1,7) THEN 1 ELSE 0 END),
  [AM]			= CONVERT(TINYINT,CASE WHEN DATEPART(HOUR,@date) >= 12 THEN 0 ELSE 1 END), 
  [Hour]		= CONVERT(TINYINT,CASE WHEN DATEPART(HOUR, @date) > 12 THEN DATEPART(HOUR, @date) - 12 ELSE DATEPART(HOUR,@date) END),
  [Minute]		= CONVERT(TINYINT,DATEPART(MINUTE, @date)),
  [Second]		= CONVERT(TINYINT,DATEPART(SECOND,@date));
 GO

-- Imputing missing data
DELETE FROM [Titanic].[Train]
WHERE [Survived] IS NULL;
GO

SELECT *
INTO Titanic.#Temp
FROM [Titanic].[Train];

ALTER TABLE Titanic.#Temp
ADD Embarked_IsMissing bit DEFAULT 0;

UPDATE Titanic.#Temp
SET Embarked_IsMissing = 0;
 
UPDATE Titanic.#Temp
SET [Embarked] = 'S', Embarked_IsMissing = 1
WHERE [Embarked] IS NULL;

SELECT PassengerId, Name, Embarked
FROM Titanic.#Temp  	
WHERE Embarked_IsMissing = 1;

DROP TABLE Titanic.#Temp;  
GO

-- Data cleaning
BEGIN TRAN	
DELETE FROM [NYCTaxi].[Training] 
WHERE [tip_amount] < 0
OR [total_amount] < 0
OR [total_amount] > 200
OR [trip_distance] <0
OR [tip_amount] > [total_amount]
OR [pickup_datetime] < [dropoff_datetime]
OR [trip_time_in_secs] < 1
OR [trip_time_in_secs] > 20000
ROLLBACK TRAN
GO

-- Dealing with outlieres 
SELECT *
INTO Titanic.#Temp
FROM [Titanic].[Train];

DELETE FROM Titanic.#Temp
WHERE [Age]	IS NULL OR [Embarked]  IS NULL;
GO

ALTER TABLE Titanic.#Temp
DROP COLUMN [Cabin];
GO

DROP TABLE Titanic.#Temp;  
GO

SELECT *
INTO Titanic.#Temp
FROM [Titanic].[Train];
GO

UPDATE Titanic.#Temp
SET [Age] = (SELECT DISTINCT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY Age) OVER ()
			FROM Titanic.#Temp
			WHERE [Sex] ='female')
WHERE [Age]	IS NULL AND  [Sex] ='female';
 
UPDATE Titanic.#Temp
SET [Age] = (SELECT DISTINCT	PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY Age) OVER ()
			FROM Titanic.#Temp
			WHERE [Sex] ='male')
WHERE [Age]	IS NULL AND  [Sex] ='male';
GO

WITH CTE ([Name],[Sex],[Age],[Pclass], DuplicateCount)
AS
(
  SELECT [Name],[Sex],[Age],[Pclass],
  ROW_NUMBER() OVER(PARTITION BY [Name],[Sex],[Age],[Pclass] ORDER BY [Name]) AS DuplicateCount
  FROM Titanic.#Temp
) 
DELETE FROM CTE 
WHERE DuplicateCount>1;
GO

ALTER TABLE [NYCTaxi].[Training] 
ADD OUTLIER VARCHAR(4000);
GO

EXEC DQ.MarkOutliersTable 'NYCTaxi.Training',3;
GO
SELECT *
FROM [NYCTaxi].[Training] 
WHERE [OUTLIER] IS NOT NULL;
GO

BEGIN TRAN	 
DELETE FROM [NYCTaxi].[Training] 
WHERE [OUTLIER] LIKE '%[trip_distance]%outside%(-5, 9)%'
AND [OUTLIER] LIKE '%[total_amount]%outside%(-19, 44)%'
ROLLBACK TRAN
GO

BEGIN TRAN	 
UPDATE [NYCTaxi].[Training] 
SET trip_distance = 9
WHERE trip_distance >9
AND [OUTLIER] LIKE '%[trip_distance]%outside%(-5, 9)%'
ROLLBACK TRAN
GO

-- Data transformations

-- Getting dummies
SELECT  [Embarked],
	   IIF([Embarked] = 'S', 1, 0) AS [Southampton],
	   IIF([Embarked] = 'C', 1, 0) AS [Cherbourg],
	   IIF([Embarked] = 'Q', 1, 0) AS [Queenstown]
FROM [Titanic].[Train]
ORDER BY NEWID();
GO

-- Label encoding
SELECT [Embarked],
	CASE WHEN [Embarked] = 'S' THEN 0
		WHEN [Embarked] = 'C' THEN 1
		ELSE 2 END AS EmbarkedEncoded
FROM [Titanic].[Train]
ORDER BY NEWID();
GO

-- Modelling
TRUNCATE TABLE Titanic.Models;
GO

EXEC sp_helptext '[Titanic].[TrainSurvivedRandomForestClassifier]'
GO

DECLARE @model VARBINARY(MAX);
EXEC [Titanic].[TrainSurvivedRandomForestClassifier] @model OUTPUT;
INSERT INTO Titanic.Models VALUES('Random Forest' ,@model);
GO

SELECT *
FROM Titanic.Models;





