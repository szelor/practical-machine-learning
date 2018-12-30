USE ML
GO

--#############################
-- Feature engineering
--#############################

SELECT [Name],
	SUBSTRING([Name], CHARINDEX(',', [Name])+2
		,CHARINDEX('.',[Name],CHARINDEX(',', [Name])+2) - CHARINDEX(',', [Name])-2)	AS Title
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

SELECT [SibSp] + [Parch] + 1 AS FamilySize,
	CASE WHEN [SibSp] + [Parch]	= 0 THEN 1 ELSE 0 END AS IsAlone
FROM Titanic.Train;
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

--#############################
-- Data cleaning
--#############################

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

DROP TABLE Titanic.#Temp;  
GO

-- Removing duplicates
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

-- Removing bad data
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

--#############################
-- Data transformations
--#############################

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

-- Generalization 
WITH Titles AS
	(SELECT SUBSTRING([Name], CHARINDEX(',', [Name])+2
		,CHARINDEX('.',[Name],CHARINDEX(',', [Name])+2) - CHARINDEX(',', [Name])-2)	AS Title
	FROM [Titanic].[Train])
SELECT Title, count(*) AS cnt
FROM Titles
GROUP BY Title
ORDER BY cnt;
GO

WITH Titles AS
	(SELECT SUBSTRING([Name], CHARINDEX(',', [Name])+2
		,CHARINDEX('.',[Name],CHARINDEX(',', [Name])+2) - CHARINDEX(',', [Name])-2)	AS Title
	FROM [Titanic].[Train]),
Cleaned AS
	(SELECT CASE Title 
		WHEN 'Mme' THEN 'Mr'
		WHEN 'Mlle' THEN 'Miss'
		WHEN 'Ms' THEN 'Miss'
		WHEN 'Miss'	THEN 'Miss'
		WHEN 'Mr' THEN 'Mr'
		WHEN 'Mrs' THEN	'Mrs'
		WHEN  'Master' THEN 'Master'
		ELSE 'Rare' END AS Title
	FROM Titles)
SELECT Title, count(*) AS cnt
FROM Cleaned
GROUP BY Title
ORDER BY cnt;
GO

--Rounding
SELECT ROUND([Fare],0) as Fare
FROM [Titanic].[Train];
GO

SELECT [trip_time_in_secs], [trip_time_in_secs]/60 AS trip_time_in_mins
FROM [NYCTaxi].[Training];
GO

-- Code by Dejan Sarka, used with author permission
-- Equal width binning
DECLARE @binwidth AS NUMERIC(5,2), 
 @min AS INT;
SELECT @min = MIN(Fare),
 @binwidth = 1.0 * (MAX(Fare) - MIN(Fare)) / 6
FROM[Titanic].[Train]; 
WITH EWB AS
(SELECT
 CASE 
  WHEN Fare >= @min + 0 * @binwidth AND Fare < @min + 1 * @binwidth
   THEN CAST((@min + 0 * @binwidth) AS VARCHAR(8)) + ' - ' +
        CAST((@min + 1 * @binwidth) AS VARCHAR(8))
  WHEN Fare >= @min + 1 * @binwidth AND Fare < @min + 2 * @binwidth
   THEN CAST((@min + 1 * @binwidth) AS VARCHAR(8)) + ' - ' +
        CAST((@min + 2 * @binwidth) AS VARCHAR(8))
  WHEN Fare >= @min + 2 * @binwidth AND Fare < @min + 3 * @binwidth
   THEN CAST((@min + 2 * @binwidth) AS VARCHAR(8)) + ' - ' +
        CAST((@min + 3 * @binwidth) AS VARCHAR(8))
  WHEN Fare >= @min + 3 * @binwidth AND Fare < @min + 4 * @binwidth
   THEN CAST((@min + 3 * @binwidth) AS VARCHAR(8)) + ' - ' +
        CAST((@min + 4 * @binwidth) AS VARCHAR(8))
  WHEN Fare >= @min + 4 * @binwidth AND Fare < @min + 5 * @binwidth
   THEN CAST((@min + 4 * @binwidth) AS VARCHAR(8)) + ' - ' +
        CAST((@min + 5 * @binwidth) AS VARCHAR(8))
  ELSE CAST((@min + 5 * @binwidth) AS VARCHAR(8)) + ' + '
 END AS FareEWB
FROM [Titanic].[Train]
Fare)
SELECT FareEWB, COUNT(*) AS cnt
FROM EWB
GROUP BY FareEWB;
GO

-- Equal height binning
WITH EHB AS
(SELECT Fare, CAST(NTILE(6) OVER(ORDER BY Fare) AS CHAR(1)) AS FareEHB
	FROM [Titanic].[Train]) 
SELECT MIN(Fare) AS min,MAX(Fare) AS max, FareEHB, COUNT(*) AS cnt
FROM EHB
GROUP BY FareEHB
ORDER BY FareEHB;
GO

-- Custom binning
WITH CB AS
 (SELECT CASE 	
	WHEN Fare <= 8 THEN 0
	WHEN Fare <= 14.5 THEN 1
	WHEN Fare <= 35 THEN 2
	ELSE 3 END AS FareCB
 FROM [Titanic].[Train])
 SELECT FareCB, COUNT(*) as cnt
 FROM CB
 GROUP BY FareCB
 ORDER BY FareCB;
 GO

--#############################
-- Feature scaling
--#############################

--Min-Max scaling
SELECT Age, (Age - MIN(Age) OVER())/(MAX(Age) OVER() - MIN(Age) OVER()) AS normAge
FROM [Titanic].[Train];
GO

-- IQR scaling
SELECT Age, (Age -  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY 1.0*Age) OVER ())
	/(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY 1.0*Age) OVER () -
	 PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY 1.0*Age) OVER ()) AS robustAge
FROM [Titanic].[Train]
ORDER BY NEWID();
GO

-- Softmax scaling
SELECT Age, 1/(1+POWER(2.718281828, -Age)) AS smAge
FROM [Titanic].[Train];
GO

-- Logarithmic scaling
SELECT Age,LOG(Age) AS logeAge, LOG(Age,10) AS log10Age
FROM [Titanic].[Train];
GO

-- Standarization
SELECT Age, (Age - AVG(AGE) OVER())/STDEV(AGE) OVER() AS stAge
FROM [Titanic].[Train];
GO

--#############################
-- Time series smoothing
--#############################

SELECT [cycle] ,[s12]   
FROM [PredictiveMaintenance].[PM_Train]
WHERE [id] = 1
ORDER BY [cycle];
GO

-- Code by Dejan Sarka, used with author permission
-- Simple moving average - last 5 values
SELECT [cycle] ,[s12] ,
 AVG([s12])
 OVER (ORDER BY [cycle] 
       ROWS BETWEEN 4 PRECEDING
	     AND CURRENT ROW) AS SMA
FROM [PredictiveMaintenance].[PM_Train]
WHERE [id] = 1
ORDER BY [cycle];
GO

-- Weighted moving average - last 2 values
DECLARE @w AS FLOAT;
SET @w = 0.7;
SELECT [cycle] ,[s12] AS Val,
 ISNULL((LAG([s12]) OVER (ORDER BY [cycle])), [s12]) AS PrevVal,
 @w * [s12] + (1 - @w) *
  ISNULL((LAG([s12]) OVER (ORDER BY [cycle])), [s12])  AS WMA
FROM [PredictiveMaintenance].[PM_Train]
WHERE [id] = 1
ORDER BY [cycle];
GO

-- Exponential moving average 
DECLARE @A AS FLOAT = 0.7, @B AS FLOAT;
SET @B = 1 - @A; 
WITH cte_cnt AS
(
SELECT [cycle] ,[s12] AS Val,
  ROW_NUMBER() OVER (ORDER BY [cycle]) - 1 as exponent 
FROM [PredictiveMaintenance].[PM_Train]
WHERE [id] = 1
) 
SELECT [cycle], val, 
 ROUND(
  SUM(CASE WHEN exponent=0 THEN 1 
           ELSE @A 
	  END * val * POWER(@B, -exponent))
  OVER (ORDER BY [cycle]) * POWER(@B, exponent)
 , 2) AS EMA 
FROM cte_cnt;
GO 

--#############################
-- Data Reduction
--#############################

-- Feature selection
SELECT *
FROM Titanic.vPreProcessedData
ORDER BY NEWID();
GO

WITH Anova_CTE AS
(
SELECT [Embarked],[Survived],
 COUNT(*) OVER (PARTITION BY [Survived]) AS gr_CasesCount,
 DENSE_RANK() OVER (ORDER BY [Survived]) AS gr_DenseRank,
 SQUARE(AVG([Embarked]) OVER (PARTITION BY [Survived]) -
        AVG([Embarked]) OVER ()) AS between_gr_SS,
 SQUARE([Embarked] - 
        AVG([Embarked]) OVER (PARTITION BY [Survived])) 
		AS within_gr_SS
FROM Titanic.vPreProcessedData
) 
SELECT N'Between groups' AS [Source of Variation],
 SUM(between_gr_SS) AS SS,
 (MAX(gr_DenseRank) - 1) AS df,
 SUM(between_gr_SS) / (MAX(gr_DenseRank) - 1) AS MS,
 (SUM(between_gr_SS) / (MAX(gr_DenseRank) - 1)) /
 (SUM(within_gr_SS) / (COUNT(*) - MAX(gr_DenseRank))) AS F
FROM Anova_CTE
UNION 
SELECT N'Within groups' AS [Source of Variation],
 SUM(within_gr_SS) AS SS,
 (COUNT(*) - MAX(gr_DenseRank)) AS df,
 SUM(within_gr_SS) / (COUNT(*) - MAX(gr_DenseRank)) AS MS,
 NULL AS F
FROM Anova_CTE;
-- F = 0
GO
WITH Anova_CTE AS
(
SELECT [Fare],[Survived],
 COUNT(*) OVER (PARTITION BY [Survived]) AS gr_CasesCount,
 DENSE_RANK() OVER (ORDER BY [Survived]) AS gr_DenseRank,
 SQUARE(AVG([Fare]) OVER (PARTITION BY [Survived]) -
        AVG([Fare]) OVER ()) AS between_gr_SS,
 SQUARE([Fare] - 
        AVG([Fare]) OVER (PARTITION BY [Survived])) 
		AS within_gr_SS
FROM Titanic.vPreProcessedData
) 
SELECT N'Between groups' AS [Source of Variation],
 SUM(between_gr_SS) AS SS,
 (MAX(gr_DenseRank) - 1) AS df,
 SUM(between_gr_SS) / (MAX(gr_DenseRank) - 1) AS MS,
 (SUM(between_gr_SS) / (MAX(gr_DenseRank) - 1)) /
 (SUM(within_gr_SS) / (COUNT(*) - MAX(gr_DenseRank))) AS F
FROM Anova_CTE
UNION 
SELECT N'Within groups' AS [Source of Variation],
 SUM(within_gr_SS) AS SS,
 (COUNT(*) - MAX(gr_DenseRank)) AS df,
 SUM(within_gr_SS) / (COUNT(*) - MAX(gr_DenseRank)) AS MS,
 NULL AS F
FROM Anova_CTE;
--F = 63.0340093393877
GO

WITH Anova_CTE AS
(
SELECT [Pclass],[Survived],
 COUNT(*) OVER (PARTITION BY [Survived]) AS gr_CasesCount,
 DENSE_RANK() OVER (ORDER BY [Survived]) AS gr_DenseRank,
 SQUARE(AVG([Pclass]) OVER (PARTITION BY [Survived]) -
        AVG([Pclass]) OVER ()) AS between_gr_SS,
 SQUARE([Pclass] - 
        AVG([Pclass]) OVER (PARTITION BY [Survived])) 
		AS within_gr_SS
FROM Titanic.vPreProcessedData
) 
SELECT N'Between groups' AS [Source of Variation],
 SUM(between_gr_SS) AS SS,
 (MAX(gr_DenseRank) - 1) AS df,
 SUM(between_gr_SS) / (MAX(gr_DenseRank) - 1) AS MS,
 (SUM(between_gr_SS) / (MAX(gr_DenseRank) - 1)) /
 (SUM(within_gr_SS) / (COUNT(*) - MAX(gr_DenseRank))) AS F
FROM Anova_CTE
UNION 
SELECT N'Within groups' AS [Source of Variation],
 SUM(within_gr_SS) AS SS,
 (COUNT(*) - MAX(gr_DenseRank)) AS df,
 SUM(within_gr_SS) / (COUNT(*) - MAX(gr_DenseRank)) AS MS,
 NULL AS F
FROM Anova_CTE;
-- F = 299.544827586207
GO

WITH Anova_CTE AS
(
SELECT [Age],[Survived],
 COUNT(*) OVER (PARTITION BY [Survived]) AS gr_CasesCount,
 DENSE_RANK() OVER (ORDER BY [Survived]) AS gr_DenseRank,
 SQUARE(AVG([Age]) OVER (PARTITION BY [Survived]) -
        AVG([Age]) OVER ()) AS between_gr_SS,
 SQUARE([Age] - 
        AVG([Age]) OVER (PARTITION BY [Survived])) 
		AS within_gr_SS
FROM Titanic.vPreProcessedData
) 
SELECT N'Between groups' AS [Source of Variation],
 SUM(between_gr_SS) AS SS,
 (MAX(gr_DenseRank) - 1) AS df,
 SUM(between_gr_SS) / (MAX(gr_DenseRank) - 1) AS MS,
 (SUM(between_gr_SS) / (MAX(gr_DenseRank) - 1)) /
 (SUM(within_gr_SS) / (COUNT(*) - MAX(gr_DenseRank))) AS F
FROM Anova_CTE
UNION 
SELECT N'Within groups' AS [Source of Variation],
 SUM(within_gr_SS) AS SS,
 (COUNT(*) - MAX(gr_DenseRank)) AS df,
 SUM(within_gr_SS) / (COUNT(*) - MAX(gr_DenseRank)) AS MS,
 NULL AS F
FROM Anova_CTE;
-- F = 3.76152804688045
GO

WITH Anova_CTE AS
(
SELECT [FamilySize],[Survived],
 COUNT(*) OVER (PARTITION BY [Survived]) AS gr_CasesCount,
 DENSE_RANK() OVER (ORDER BY [Survived]) AS gr_DenseRank,
 SQUARE(AVG([FamilySize]) OVER (PARTITION BY [Survived]) -
        AVG([FamilySize]) OVER ()) AS between_gr_SS,
 SQUARE([FamilySize] - 
        AVG([FamilySize]) OVER (PARTITION BY [Survived])) 
		AS within_gr_SS
FROM Titanic.vPreProcessedData
) 
SELECT N'Between groups' AS [Source of Variation],
 SUM(between_gr_SS) AS SS,
 (MAX(gr_DenseRank) - 1) AS df,
 SUM(between_gr_SS) / (MAX(gr_DenseRank) - 1) AS MS,
 (SUM(between_gr_SS) / (MAX(gr_DenseRank) - 1)) /
 (SUM(within_gr_SS) / (COUNT(*) - MAX(gr_DenseRank))) AS F
FROM Anova_CTE
UNION 
SELECT N'Within groups' AS [Source of Variation],
 SUM(within_gr_SS) AS SS,
 (COUNT(*) - MAX(gr_DenseRank)) AS df,
 SUM(within_gr_SS) / (COUNT(*) - MAX(gr_DenseRank)) AS MS,
 NULL AS F
FROM Anova_CTE;
-- F = 0
GO

;WITH
ObservedCombination_CTE AS
(
SELECT [Survived] AS OnRows, [Title] AS OnCols, 
 COUNT(*) AS ObservedCombination
FROM [Titanic].[vPreProcessedData]
GROUP BY [Survived], [Title]
),
ExpectedCombination_CTE AS
(
SELECT OnRows, OnCols, ObservedCombination
 ,SUM(ObservedCombination) OVER (PARTITION BY OnRows) AS ObservedOnRows
 ,SUM(ObservedCombination) OVER (PARTITION BY OnCols) AS ObservedOnCols
 ,SUM(ObservedCombination) OVER () AS ObservedTotal
 ,CAST(ROUND(SUM(1.0 * ObservedCombination) OVER (PARTITION BY OnRows)
  * SUM(1.0 * ObservedCombination) OVER (PARTITION BY OnCols) 
  / SUM(1.0 * ObservedCombination) OVER (), 0) AS INT) AS ExpectedCombination
FROM ObservedCombination_CTE
)
SELECT (COUNT(DISTINCT OnRows) - 1) * (COUNT(DISTINCT OnCols) - 1) AS DegreesOfFreedom,
	SUM(SQUARE(ObservedCombination - ExpectedCombination) / ExpectedCombination) AS ChiSquared
FROM ExpectedCombination_CTE;
GO
--ChiSquared=286.240354290757

;WITH
ObservedCombination_CTE AS
(
SELECT [Survived] AS OnRows, [Sex] AS OnCols, 
 COUNT(*) AS ObservedCombination
FROM [Titanic].[vPreProcessedData]
GROUP BY [Survived], [Sex]
),
ExpectedCombination_CTE AS
(
SELECT OnRows, OnCols, ObservedCombination
 ,SUM(ObservedCombination) OVER (PARTITION BY OnRows) AS ObservedOnRows
 ,SUM(ObservedCombination) OVER (PARTITION BY OnCols) AS ObservedOnCols
 ,SUM(ObservedCombination) OVER () AS ObservedTotal
 ,CAST(ROUND(SUM(1.0 * ObservedCombination) OVER (PARTITION BY OnRows)
  * SUM(1.0 * ObservedCombination) OVER (PARTITION BY OnCols) 
  / SUM(1.0 * ObservedCombination) OVER (), 0) AS INT) AS ExpectedCombination
FROM ObservedCombination_CTE
)
SELECT (COUNT(DISTINCT OnRows) - 1) * (COUNT(DISTINCT OnCols) - 1) AS DegreesOfFreedom,
	SUM(SQUARE(ObservedCombination - ExpectedCombination) / ExpectedCombination) AS ChiSquared
FROM ExpectedCombination_CTE;
GO
--ChiSquared=260.660376192108

;WITH
ObservedCombination_CTE AS
(
SELECT [Survived] AS OnRows, [IsAlone] AS OnCols, 
 COUNT(*) AS ObservedCombination
FROM [Titanic].[vPreProcessedData]
GROUP BY [Survived], [IsAlone]
),
ExpectedCombination_CTE AS
(
SELECT OnRows, OnCols, ObservedCombination
 ,SUM(ObservedCombination) OVER (PARTITION BY OnRows) AS ObservedOnRows
 ,SUM(ObservedCombination) OVER (PARTITION BY OnCols) AS ObservedOnCols
 ,SUM(ObservedCombination) OVER () AS ObservedTotal
 ,CAST(ROUND(SUM(1.0 * ObservedCombination) OVER (PARTITION BY OnRows)
  * SUM(1.0 * ObservedCombination) OVER (PARTITION BY OnCols) 
  / SUM(1.0 * ObservedCombination) OVER (), 0) AS INT) AS ExpectedCombination
FROM ObservedCombination_CTE
)
SELECT (COUNT(DISTINCT OnRows) - 1) * (COUNT(DISTINCT OnCols) - 1) AS DegreesOfFreedom,
	SUM(SQUARE(ObservedCombination - ExpectedCombination) / ExpectedCombination) AS ChiSquared
FROM ExpectedCombination_CTE;
GO
--ChiSquared=36.6390704858139

