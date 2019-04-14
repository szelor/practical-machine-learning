-- What If scenario

DECLARE @model varbinary(max)
SELECT @model = [model] from [LendingClub].[Models] 
	WHERE [name] = 'RT Decision Forest';

TRUNCATE TABLE [LendingClub].[LoanStatsPredictions];

WITH d AS 
	(SELECT id, is_bad, grade, int_rate, out_prncp_inv, policy_code, installment,
	open_acc_6m, all_util, revol_util, total_rec_prncp, bc_util,home_ownership
	FROM [LendingClub].[LoanStats] WHERE [is_train] = 0)
INSERT INTO [LendingClub].[LoanStatsPredictions] (id, actual, predicted) 
SELECT id, is_bad, is_bad_Pred
FROM PREDICT(MODEL = @model, DATA = d)
WITH ([0_prob] float, [1_prob] float, is_bad_Pred nvarchar(max)) p;
GO

DECLARE @model varbinary(max)
SELECT @model = [model] from [LendingClub].[Models] 
	WHERE [name] = 'RT Decision Forest';

TRUNCATE TABLE [LendingClub].[LoanPredictionsWhatIf];

WITH d AS 
	(SELECT id, is_bad, grade, int_rate+9 as int_rate, out_prncp_inv, policy_code, installment,
	open_acc_6m, all_util, revol_util, total_rec_prncp, bc_util,home_ownership 
	  FROM [LendingClub].[LoanStats] 
	  WHERE [is_train] = 0)
INSERT INTO [LendingClub].[LoanPredictionsWhatIf] (id, actual, predicted) 
SELECT id, is_bad, is_bad_Pred
FROM PREDICT(MODEL = @model, DATA = d)
WITH ([0_prob] float, [1_prob] float, is_bad_Pred nvarchar(max)) p;
GO

SELECT COUNT(*)
FROM [LendingClub].[LoanStatsPredictions] as P
JOIN [LendingClub].[LoanPredictionsWhatIf] as I
ON P.id=I.id
WHERE P.predicted<>I.predicted;
GO

SELECT AVG(1.*predicted) AS Average, STDEV(predicted) AS StDeviation
FROM [LendingClub].[LoanPredictionsWhatIf]
UNION ALL
SELECT AVG(1.*predicted), STDEV(predicted)
FROM [LendingClub].[LoanStatsPredictions];
GO

SELECT loan_status,st.addr_state,CASE WHEN p.predicted = 1 THEN 'High'
	ELSE 'Low' END AS [ChargeOffProbability], 
	COUNT(*) as [Number of Loans], SUM(funded_amnt) AS [Loan Amount]
FROM [LendingClub].[LoanStats] st
INNER JOIN [LendingClub].[LoanStatsPredictions] p
on st.id = p.id
GROUP BY loan_status,st.addr_state, CASE WHEN p.predicted = 1 THEN 'High'
	ELSE 'Low' END;
GO

SELECT loan_status,st.addr_state,CASE WHEN wi.predicted = 1 THEN 'High'
	ELSE 'Low'
	END AS [ChargeOffProbability], 
	COUNT(*) as [Number of Loans], SUM(funded_amnt) as [Loan Amount]
FROM [LendingClub].[LoanStats] st
INNER JOIN [LendingClub].[LoanPredictionsWhatIf] wi
on st.id = wi.id
GROUP BY loan_status,st.addr_state, CASE WHEN wi.predicted = 1 THEN 'High'
	ELSE 'Low'
	END;