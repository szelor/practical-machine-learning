USE ML
GO

SELECT * 
FROM [ML].[Titanic].[Train];
GO

SELECT MIN(Age) AS minA,
 MAX(Age) AS maxA,
 MAX(Age) - MIN(Age) AS rngA,
 AVG(Age) AS avgA,
 COUNT(Age) AS cAge,
 STDEV(1.0*Age) as stdevAge,
 STDEV(1.0*Age) / AVG(1.0*Age) AS CVAge,
 COUNT(*) - COUNT(Age) AS missingAge,
 COUNT(DISTINCT(Age)) AS uniqueAge
FROM [Titanic].[Train];

--Code by Dejan Sarka, used with author permission

-- Mode
SELECT TOP (1) WITH TIES Age, COUNT(*) AS Number
FROM [Titanic].[Train]
GROUP BY Age
ORDER BY COUNT(*) DESC;

SELECT TOP (1) WITH TIES Age, COUNT(*) AS Number
FROM [Titanic].[Train]
WHERE Age is NOT NULL
GROUP BY Age
ORDER BY COUNT(*) DESC;

--Frequencies
WITH freqCTE AS
(
SELECT v.[Age],
 COUNT(v.[Age]) AS AbsFreq,
 CAST(ROUND(100. * (COUNT(v.[Age])) /
       (SELECT COUNT(*) FROM [Titanic].[Train]), 0) AS INT) AS AbsPerc
FROM [Titanic].[Train] AS v
GROUP BY v.[Age]
)
SELECT [Age],
 AbsFreq,
 SUM(AbsFreq) 
  OVER(ORDER BY [Age] 
       ROWS BETWEEN UNBOUNDED PRECEDING
	    AND CURRENT ROW) AS CumFreq,
 AbsPerc,
 SUM(AbsPerc)
  OVER(ORDER BY [Age]
       ROWS BETWEEN UNBOUNDED PRECEDING
	    AND CURRENT ROW) AS CumPerc,
 CAST(REPLICATE('*',AbsPerc) AS VARCHAR(50)) AS Histogram
FROM freqCTE
ORDER BY [Age];

-- Median
SELECT DISTINCT
 PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 1.0*Age) OVER () AS Median
FROM [Titanic].[Train];

-- IQR
SELECT DISTINCT
 PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY 1.0*Age) OVER () -
 PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY 1.0*Age) OVER () AS IQR
FROM [Titanic].[Train];

-- Skewness
WITH SkewCTE AS
(
SELECT SUM(1.0*Age) AS rx,
 SUM(POWER(1.0*Age,2)) AS rx2,
 SUM(POWER(1.0*Age,3)) AS rx3,
 COUNT(1.0*Age) AS rn,
 STDEV(1.0*Age) AS stdv,
 AVG(1.0*Age) AS av
FROM [Titanic].[Train]
)
SELECT
   (rx3 - 3*rx2*av + 3*rx*av*av - rn*av*av*av)
   / (stdv*stdv*stdv) * rn / (rn-1) / (rn-2) AS Skewness
FROM SkewCTE;

-- Kurtosis
WITH KurtCTE AS
(
SELECT SUM(1.0*Age) AS rx,
 SUM(POWER(1.0*Age,2)) AS rx2,
 SUM(POWER(1.0*Age,3)) AS rx3,
 SUM(POWER(1.0*Age,4)) AS rx4,
 COUNT(1.0*Age) AS rn,
 STDEV(1.0*Age) AS stdv,
 AVG(1.*Age) AS av
FROM [Titanic].[Train]
)
SELECT
   (rx4 - 4*rx3*av + 6*rx2*av*av - 4*rx*av*av*av + rn*av*av*av*av)
   / (stdv*stdv*stdv*stdv) * rn * (rn+1) / (rn-1) / (rn-2) / (rn-3)
   - 3.0 * (rn-1) * (rn-1) / (rn-2) / (rn-3) AS Kurtosis
FROM KurtCTE;

-- Calculating the entropy
WITH ProbabilityCTE AS
(
SELECT Age,
 COUNT(Age) AS StateFreq
FROM [Titanic].[Train]
WHERE Age IS NOT NULL
GROUP BY Age
),
StateEntropyCTE AS
(
SELECT Age,
 1.0*StateFreq / SUM(StateFreq) OVER () AS StateProbability
FROM ProbabilityCTE
)
SELECT 'Age' AS Variable,
 LOG(COUNT(*),2) AS MaxPossibleEntropy,
 (-1)*SUM(StateProbability * LOG(StateProbability,2)) AS TotalEntropy,
 100 * ((-1)*SUM(StateProbability * LOG(StateProbability,2))) / 
 (LOG(COUNT(*),2)) AS PctOfMaxPossibleEntropy
FROM StateEntropyCTE;
GO

-- Profiling
-- Code by Sławomir Malinowski, used with author permission

DELETE FROM [DQ].[DataProfiles];
DELETE FROM [DQ].[DataProfileColumns];
DELETE FROM [DQ].[DataProfileTables];
DELETE FROM [DQ].[DataProfileDatabases]
GO

--Code by Sławomir Malinowski, used with author permission

EXEC sp_helptext '[DQ].[DataProfiling]'
GO

DECLARE @RC int
DECLARE @DatabaseName nvarchar(50)
DECLARE @SchemaName nvarchar(50)
DECLARE @TableView char(1)
DECLARE @Debug bit

EXECUTE @RC = [DQ].[DataProfiling] 
  @DatabaseName = 'ML'
  ,@SchemaName = 'Titanic'
  ,@TableName ='Train'
  ,@TableView ='T'
  ,@Debug = 1
GO

EXEC sp_helptext '[DQ].[DataProfilingAllObjects]'
GO

DECLARE @RC int
DECLARE @DatabaseName nvarchar(50)
DECLARE @SchemaName nvarchar(50)
DECLARE @TableView char(1)
DECLARE @Debug bit

EXECUTE @RC = [DQ].[DataProfilingAllObjects] 
  @DatabaseName = 'ML'
  ,@SchemaName = 'Titanic'
  ,@TableView ='T'
  ,@Debug = 0
GO

SELECT d.DatabaseName, t.SchemaName, t.TableName, t.Rows, t.ProfileDurationS
FROM [DQ].[DataProfileDatabases] AS d
JOIN [DQ].[DataProfileTables] AS t
	ON d.DatabaseId = t.DatabaseId;

SELECT t.TableName, c.ColumnName, c.SystemDataTypeId, c.DataLength, c.DataPrecision
FROM [DQ].[DataProfileTables] AS t 
JOIN [DQ].[DataProfileColumns] AS c
	ON t.TableId = c.TableId;

SELECT [TableName], [ColumnName], [Description], p.DataResult, p.NumericResult, p.StringResult, p.Counts, p.RowsPercentage
FROM [DQ].[DataProfileTables] t
JOIN [DQ].[DataProfileColumns] c on t.TableId = c.TableId
JOIN [DQ].[DataProfiles] p on c.ColumnId = p.ColumnId
JOIN [DQ].[DataProfileStats] s on s.StatId = p.ProfileStatId
WHERE TableName = 'Train' and ColumnName = 'Age'
ORDER BY Description;
GO
