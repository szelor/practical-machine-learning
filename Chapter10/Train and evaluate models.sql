USE ML
GO

TRUNCATE TABLE [LendingClub].[Models]
GO

-- Train model on raw data
EXEC [LendingClub].[BuildModel] 
	@name = N'Decision Forest: All data',
	@input_query = N'SELECT id,[is_bad],[grade],[int_rate],[out_prncp_inv],[policy_code],
	[installment],[open_acc_6m],[all_util],[revol_util],[total_rec_prncp],[bc_util],
	[percent_bc_gt_75],[home_ownership]
	FROM [LendingClub].[LoanStats] 
	WHERE [is_train] = 1';
GO

-- Train model on balanced data
EXEC sp_helptext '[LendingClub].[vLoanStats]'
GO

EXEC [LendingClub].[BuildModel] 
	@name = N'Decision Forest: Balanced data',
	@input_query = N'SELECT * FROM [LendingClub].[vLoanStats] WHERE [is_train] = 1'
GO

SELECT [Name],nTree,mTry,maxDepth,minSplit,[Train F-score],TrainDate
FROM [LendingClub].[Models]
WHERE [Name] LIKE 'Decision Forest:%';
GO

--Score models using new data
EXEC [LendingClub].[ScoreLoans]
	@input_query = 'SELECT * FROM [LendingClub].[LoanStats] WHERE [is_train] = 0',
	@name = 'Decision Forest: All data';
GO

EXEC [LendingClub].[ScoreLoans]
	@input_query = 'SELECT * FROM [LendingClub].[vLoanStats] WHERE [is_train] = 0',
	@name = 'Decision Forest: Balanced data';
GO

SELECT [Name],[Train F-score],[Test F-score]
FROM [LendingClub].[Models]
WHERE [Name] LIKE 'Decision Forest:%';
GO

--Hiperparameters tuning on balanced data
DECLARE @i INT = 1;

DROP TABLE IF EXISTS #hiperparameters;
WITH Sequences20 (nTree,minSplit) AS
	(SELECT 30 AS nTree, 100 AS minSplit
        UNION ALL
    SELECT nTree + 2, minSplit + 20
        FROM Sequences20
        WHERE nTree < 67),
Sequences10 (maxDepth,mTry) AS
	(SELECT 2 AS maxDepth, 1 AS mTry
		UNION ALL
    SELECT maxDepth + 2,  mTry  + 1  
        FROM Sequences10
        WHERE maxDepth < 20)
SELECT S1.nTree, S2.minSplit, S3.maxDepth, S4.mTry
INTO #hiperparameters
FROM Sequences20 AS S1
CROSS JOIN Sequences20 AS S2
CROSS JOIN Sequences10 AS S3
CROSS JOIN Sequences10 AS S4;

DECLARE hiperparameters CURSOR FOR
SELECT TOP 5 PERCENT [nTree], [mTry], [maxDepth], [minSplit]
FROM #hiperparameters
ORDER BY NEWID();

DECLARE @n_tree INT, @m_try INT, @max_depth	INT, @minSplit INT;
DECLARE @name NVARCHAR (100);

OPEN hiperparameters  

FETCH NEXT FROM hiperparameters   
INTO @n_tree, @m_try, @max_depth, @minSplit;  

WHILE @@FETCH_STATUS = 0  
BEGIN
	SET @name = CONCAT('Decision Forest V',@i);
	PRINT '--------------------------------------'
	PRINT 'Training model ' + @name  
	PRINT '--------------------------------------'
	EXEC [LendingClub].[BuildModels] 
	@name = @name,
	@input_query = N'SELECT id
	  ,[is_bad]
	  ,[grade]
      ,[int_rate]
	  ,[out_prncp_inv]
      ,[policy_code]
	  ,[installment]
      ,[open_acc_6m]
      ,[all_util]
      ,[revol_util]
      ,[total_rec_prncp]
	  ,[bc_util]
	  ,[percent_bc_gt_75]
	  ,[home_ownership]
	  FROM [LendingClub].[vLoanStats] 
	  WHERE [is_train] = 1'	,
	@n_tree = @n_tree,
	@m_try = @m_try, 
	@max_depth = @max_depth,
	@minSplit = @minSplit;
	
	FETCH NEXT FROM hiperparameters   
	INTO @n_tree, @m_try, @max_depth, @minSplit;
	SET @i+=1;

END   
CLOSE hiperparameters;  
DEALLOCATE hiperparameters; 
GO

SELECT COUNT(*)
FROM [LendingClub].[Models];

SELECT [Name],[nTree],[mTry],[maxDepth],[minSplit],[Train F-score]
FROM [LendingClub].[Models]
WHERE [Train F-score] in (SELECT TOP 10 [Train F-score] FROM [LendingClub].[Models] ORDER BY [Train F-score] DESC)
  OR [Train F-score] in (SELECT TOP 10 [Train F-score] FROM [LendingClub].[Models] ORDER BY [Train F-score])
ORDER BY [Train F-score] DESC;
GO

SELECT COUNT(*)
FROM [LendingClub].[Models]
WHERE [Train F-score] >=0.7
-- Score all V... models

DECLARE models CURSOR FOR
SELECT [Name]
FROM [LendingClub].[Models]
WHERE [Name] like '%V%';
DECLARE @name NVARCHAR(100);
OPEN models  

FETCH NEXT FROM models   
INTO @name  

WHILE @@FETCH_STATUS = 0  
BEGIN
	EXEC [LendingClub].[ScoreLoans]
	@input_query = 'SELECT * FROM [LendingClub].[vLoanStats] WHERE [is_train] = 0',
	@name = @name
	
	FETCH NEXT FROM models   
	INTO @name ;

END   
CLOSE models;  
DEALLOCATE models; 
GO

SELECT [Name],[nTree],[mTry],[maxDepth],[minSplit],[Train F-score],[Test F-score]
FROM [LendingClub].[Models]
WHERE [Test F-score] in (SELECT TOP 10 [Test F-score] FROM [LendingClub].[Models] ORDER BY [Test F-score] DESC)
  OR [Test F-score] in (SELECT TOP 10 [Test F-score] FROM [LendingClub].[Models] ORDER BY [Test F-score])
ORDER BY [Test F-score] DESC;
GO

SELECT COUNT(*)
FROM [LendingClub].[Models]
WHERE [Test F-score] >=0.4
