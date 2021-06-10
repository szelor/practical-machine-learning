USE ML
GO
TRUNCATE TABLE [LendingClub].[LoanStats]
GO

SELECT TOP 5 term, issue_d, earliest_cr_line, loan_amnt, loan_status, int_rate
FROM [LendingClub].[LoanStatsStaging];
GO

SELECT loan_status, count(*) as cnt
FROM [LendingClub].[LoanStats]
GROUP BY loan_status; 
GO

--Load train data from stage
EXEC [LendingClub].[PerformETL] @is_train=1;
GO

SELECT is_bad, count(*) as cnt
FROM [LendingClub].[LoanStats]
GROUP BY is_bad;
GO

--Load test data from stage
EXEC [LendingClub].[PerformETL]  @is_train=0;
GO	

SELECT TOP 10 *
FROM [LendingClub].[LoanStats];

SELECT is_train, COUNT(*) AS cnt, 100.*count(*)/938821 as pct
FROM [LendingClub].[LoanStats]
GROUP BY is_train;

SELECT is_bad, count(*) as cnt, 100.*count(*)/938821 as pct
FROM [LendingClub].[LoanStats]
GROUP BY is_bad;

SELECT is_bad, count(*) as cnt, 100.*count(*)/
	(SELECT count(*) FROM [LendingClub].[LoanStats]WHERE is_train = 1) as pct
FROM [LendingClub].[LoanStats]
WHERE is_train = 1
GROUP BY is_bad
UNION ALL
SELECT is_bad, count(*) as cnt, 100.*count(*)/
	(SELECT count(*) FROM [LendingClub].[LoanStats]WHERE is_train = 0) as pct
FROM [LendingClub].[LoanStats]
WHERE is_train = 0
GROUP BY is_bad;
GO

WITH cte AS (
	SELECT is_train, ABS(CHECKSUM(NEWID())) % 100 AS rnd
        FROM [LendingClub].[LoanStats] )
UPDATE cte
SET is_train = CASE WHEN rnd<5 THEN 0 ELSE 1 END;
GO

DECLARE @RC int
DECLARE @DatabaseName nvarchar(50)
DECLARE @SchemaName nvarchar(50)
DECLARE @TableView char(1)
DECLARE @Debug bit

EXECUTE @RC = [DQ].[DataProfiling] 
  @DatabaseName = 'ML'
  ,@SchemaName = 'LendingClub'
  ,@TableName ='LoanStats'
  ,@TableView ='T'
  ,@Debug = 0;
GO

	SELECT [ColumnName], 100.*p.Counts/(SELECT COUNT(*) FROM [LendingClub].[LoanStats]) AS pct
	FROM [DQ].[DataProfileTables] t
	JOIN [DQ].[DataProfileColumns] c on t.TableId = c.TableId
	JOIN [DQ].[DataProfiles] p on c.ColumnId = p.ColumnId
	JOIN [DQ].[DataProfileStats] s on s.StatId = p.ProfileStatId
	WHERE TableName = 'LoanStats' and Description = 'Emptyness'
	ORDER BY p.Counts DESC;
GO

ALTER VIEW [LendingClub].[vLoanStats] AS
SELECT [loan_amnt]
      ,[funded_amnt]
      ,[funded_amnt_inv]
      ,[term]
      ,[int_rate]
      ,[installment]
      ,[grade]
      ,[sub_grade]
      ,[emp_title]
      ,[emp_length]
      ,[home_ownership]
      ,[annual_inc]
      ,[verification_status]
      ,[issue_d]
      ,[loan_status]
      ,[pymnt_plan]
      ,[purpose]
      ,[title]
      ,[zip_code]
      ,[addr_state]
      ,[dti]
      ,[delinq_2yrs]
      ,[earliest_cr_line]
      ,[inq_last_6mths]
      ,[mths_since_last_delinq]
      ,[mths_since_last_record]
      ,[open_acc]
      ,[pub_rec]
      ,[revol_bal]
      ,[revol_util]
      ,[total_acc]
      ,[initial_list_status]
      ,[out_prncp]
      ,[out_prncp_inv]
      ,[total_pymnt]
      ,[total_pymnt_inv]
      ,[total_rec_prncp]
      ,[total_rec_int]
      ,[total_rec_late_fee]
      ,[recoveries]
      ,[collection_recovery_fee]
      ,[last_pymnt_d]
      ,[last_pymnt_amnt]
      ,[next_pymnt_d]
      ,[last_credit_pull_d]
      ,[collections_12_mths_ex_med]
      ,[mths_since_last_major_derog]
      ,[policy_code]
      ,[application_type]
      ,[acc_now_delinq]
      ,[tot_coll_amt]
      ,[tot_cur_bal]
      ,[open_acc_6m]
      ,[open_il_12m]
      ,[open_il_24m]
      ,[mths_since_rcnt_il]
      ,[total_bal_il]
      ,[il_util]
      ,[open_rv_12m]
      ,[open_rv_24m]
      ,[max_bal_bc]
      ,[all_util]
      ,[total_rev_hi_lim]
      ,[inq_fi]
      ,[total_cu_tl]
      ,[inq_last_12m]
      ,[acc_open_past_24mths]
      ,[avg_cur_bal]
      ,[bc_open_to_buy]
      ,[bc_util]
      ,[chargeoff_within_12_mths]
      ,[delinq_amnt]
      ,[mo_sin_old_il_acct]
      ,[mo_sin_old_rev_tl_op]
      ,[mo_sin_rcnt_rev_tl_op]
      ,[mo_sin_rcnt_tl]
      ,[mort_acc]
      ,[mths_since_recent_bc]
      ,[mths_since_recent_inq]
      ,[num_accts_ever_120_pd]
      ,[num_actv_bc_tl]
      ,[num_actv_rev_tl]
      ,[num_bc_sats]
      ,[num_bc_tl]
      ,[num_il_tl]
      ,[num_op_rev_tl]
      ,[num_rev_accts]
      ,[num_rev_tl_bal_gt_0]
      ,[num_sats]
      ,[num_tl_120dpd_2m]
      ,[num_tl_30dpd]
      ,[num_tl_90g_dpd_24m]
      ,[num_tl_op_past_12m]
      ,[pct_tl_nvr_dlq]
      ,[percent_bc_gt_75]
      ,[pub_rec_bankruptcies]
      ,[tax_liens]
      ,[tot_hi_cred_lim]
      ,[total_bal_ex_mort]
      ,[total_bc_limit]
      ,[total_il_high_credit_limit]
      ,[is_bad]
      ,[is_train]
      ,[id] 
FROM [LendingClub].[LoanStats]
WHERE is_bad = 1
UNION ALL
SELECT [loan_amnt]
      ,[funded_amnt]
      ,[funded_amnt_inv]
      ,[term]
      ,[int_rate]
      ,[installment]
      ,[grade]
      ,[sub_grade]
      ,[emp_title]
      ,[emp_length]
      ,[home_ownership]
      ,[annual_inc]
      ,[verification_status]
      ,[issue_d]
      ,[loan_status]
      ,[pymnt_plan]
      ,[purpose]
      ,[title]
      ,[zip_code]
      ,[addr_state]
      ,[dti]
      ,[delinq_2yrs]
      ,[earliest_cr_line]
      ,[inq_last_6mths]
      ,[mths_since_last_delinq]
      ,[mths_since_last_record]
      ,[open_acc]
      ,[pub_rec]
      ,[revol_bal]
      ,[revol_util]
      ,[total_acc]
      ,[initial_list_status]
      ,[out_prncp]
      ,[out_prncp_inv]
      ,[total_pymnt]
      ,[total_pymnt_inv]
      ,[total_rec_prncp]
      ,[total_rec_int]
      ,[total_rec_late_fee]
      ,[recoveries]
      ,[collection_recovery_fee]
      ,[last_pymnt_d]
      ,[last_pymnt_amnt]
      ,[next_pymnt_d]
      ,[last_credit_pull_d]
      ,[collections_12_mths_ex_med]
      ,[mths_since_last_major_derog]
      ,[policy_code]
      ,[application_type]
      ,[acc_now_delinq]
      ,[tot_coll_amt]
      ,[tot_cur_bal]
      ,[open_acc_6m]
      ,[open_il_12m]
      ,[open_il_24m]
      ,[mths_since_rcnt_il]
      ,[total_bal_il]
      ,[il_util]
      ,[open_rv_12m]
      ,[open_rv_24m]
      ,[max_bal_bc]
      ,[all_util]
      ,[total_rev_hi_lim]
      ,[inq_fi]
      ,[total_cu_tl]
      ,[inq_last_12m]
      ,[acc_open_past_24mths]
      ,[avg_cur_bal]
      ,[bc_open_to_buy]
      ,[bc_util]
      ,[chargeoff_within_12_mths]
      ,[delinq_amnt]
      ,[mo_sin_old_il_acct]
      ,[mo_sin_old_rev_tl_op]
      ,[mo_sin_rcnt_rev_tl_op]
      ,[mo_sin_rcnt_tl]
      ,[mort_acc]
      ,[mths_since_recent_bc]
      ,[mths_since_recent_inq]
      ,[num_accts_ever_120_pd]
      ,[num_actv_bc_tl]
      ,[num_actv_rev_tl]
      ,[num_bc_sats]
      ,[num_bc_tl]
      ,[num_il_tl]
      ,[num_op_rev_tl]
      ,[num_rev_accts]
      ,[num_rev_tl_bal_gt_0]
      ,[num_sats]
      ,[num_tl_120dpd_2m]
      ,[num_tl_30dpd]
      ,[num_tl_90g_dpd_24m]
      ,[num_tl_op_past_12m]
      ,[pct_tl_nvr_dlq]
      ,[percent_bc_gt_75]
      ,[pub_rec_bankruptcies]
      ,[tax_liens]
      ,[tot_hi_cred_lim]
      ,[total_bal_ex_mort]
      ,[total_bc_limit]
      ,[total_il_high_credit_limit]
      ,[is_bad]
      ,[is_train]
      ,[id] 
FROM [LendingClub].[LoanStats]
WHERE is_bad = 0 AND (ABS(CAST((BINARY_CHECKSUM(NEWID())) as int)) % 100) < 10
GO

EXEC [LendingClub].[PlotDistribution] 
	@input_query = N'SELECT * FROM [LendingClub].[LoanStats] TABLESAMPLE (20 PERCENT)'
GO


SELECT is_bad, COUNT(*) as cnt, 100.*COUNT(*)/
	(SELECT COUNT(*) FROM [LendingClub].[vLoanStats]) AS pct
FROM [LendingClub].[vLoanStats]
GROUP BY is_bad;
GO

EXEC [LendingClub].[PlotDistribution] 
	@input_query = N'SELECT * FROM [LendingClub].[vLoanStats]'
GO

WITH
ObservedCombination_CTE AS
(
SELECT is_bad AS OnRows,
   grade OnCols, 
 COUNT(*) AS ObservedCombination
FROM [LendingClub].[vLoanStats]
GROUP BY is_bad, grade
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
	SUM(SQUARE(ObservedCombination - ExpectedCombination) 
/ ExpectedCombination) AS ChiSquared
FROM ExpectedCombination_CTE;

WITH
ObservedCombination_CTE AS
(
SELECT is_bad AS OnRows,
   application_type OnCols, 
 COUNT(*) AS ObservedCombination
FROM [LendingClub].[vLoanStats]
GROUP BY is_bad, application_type
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
	SUM(SQUARE(ObservedCombination - ExpectedCombination) 
/ ExpectedCombination) AS ChiSquared
FROM ExpectedCombination_CTE;

