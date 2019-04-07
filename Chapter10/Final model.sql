--Final model
EXEC [LendingClub].[BuildRealtimeScoringModel]
	@name = N'RT Decision Forest',
	@input_query = N'SELECT * FROM [LendingClub].[vLoanStats] WHERE [is_train] = 1',
	@n_tree = 20,
	@m_try = 5, 
	@max_depth = 5,
	@minSplit = 200;
GO

SELECT TOP 5 [Name], LEN(Model)
FROM [ML].[LendingClub].[Models]
ORDER BY [Train F-score] DESC;
GO

-- Native prediction

DECLARE @model varbinary(max)
SELECT @model = [model] FROM [LendingClub].[Models] 
	WHERE [name] = 'RT Decision Forest';

TRUNCATE TABLE [LendingClub].[LoanStatsPredictions]
-- Log beginning of processing time
INSERT INTO [LendingClub].[RunTimeStats] VALUES (@@SPID, GETDATE(),'Start scoring model RT Decision Forest')

DECLARE @input_qry varchar(max);
WITH d AS 
	(SELECT id
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
	FROM [LendingClub].[vLoanStats] WHERE [is_train] = 0)

INSERT INTO [LendingClub].[LoanStatsPredictions] (id, actual, predicted) 
SELECT id, is_bad, is_bad_Pred
FROM PREDICT(MODEL = @model, DATA = d)
WITH ([0_prob] float, [1_prob] float, is_bad_Pred nvarchar(max)) p ;
GO

-- Log end of processing time
INSERT INTO [LendingClub].[RunTimeStats] VALUES (@@SPID, GETDATE(),'End scoring model RT Decision Forest')
GO

SELECT * 
FROM [LendingClub].[RunTimeStats]
GO

-- Score model
SELECT *
FROM [LendingClub].[LoanStatsPredictions]
GO
 
DECLARE @tp INT, @fn INT, @fp INT, @tn INT
SELECT @tp = COUNT(*) 
	FROM [LendingClub].[LoanStatsPredictions] 
	WHERE [actual] =1 and [predicted]=1 

SELECT @fn = COUNT(*) 
	FROM [LendingClub].[LoanStatsPredictions] 
	WHERE [actual] =1 and [predicted]=0

SELECT @fp = COUNT(*) 
	FROM [LendingClub].[LoanStatsPredictions] 
	WHERE [actual] =0 and [predicted]=1

SELECT @tn = COUNT(*) 
	FROM [LendingClub].[LoanStatsPredictions] 
	WHERE [actual] =0 and [predicted]=0

select @tp, @fn , @fp , @tn

DECLARE @accuracy DECIMAL(3,2) = (@tp + @tn)*1. / (@tp + @fn + @fp + @tn)
DECLARE @precision DECIMAL(3,2) = @tp*1. / (@tp + @fp)
DECLARE @recall DECIMAL(3,2) = @tp*1. / (@tp + @fn)
DECLARE @fscore DECIMAL(3,2)  = 2. * (@precision * @recall) / (@precision + @recall)

SELECT @accuracy, @precision, @recall, @fscore

UPDATE [LendingClub].[Models]
SET [Test F-score] = @fscore
WHERE [Name] = 'RT Decision Forest'
GO

SELECT TOP 10 [Name],[nTree],[mTry],[maxDepth],[minSplit],[Train F-score],[Test F-score] 
FROM [LendingClub].[Models]
ORDER BY [Test F-score] DESC;
GO