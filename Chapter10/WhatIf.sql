-- What If scenario

DECLARE @model varbinary(max)
SELECT @model = [model] from [LendingClub].[Models] 
	WHERE [name] = 'RT Decision Forest';

TRUNCATE TABLE [LendingClub].[LoanPredictionsWhatIf];

DECLARE @input_qry varchar(max);
WITH d AS 
	(SELECT id
	  ,[is_bad]
	  ,[grade]
      ,[int_rate] + 5 AS [int_rate]
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
	  WHERE [is_train] = 0)

INSERT INTO [LendingClub].[LoanPredictionsWhatIf] (id, actual, predicted) 
SELECT id, is_bad, is_bad_Pred
FROM PREDICT(MODEL = @model, DATA = d)
WITH ([0_prob] float, [1_prob] float, is_bad_Pred nvarchar(max)) p ;
GO