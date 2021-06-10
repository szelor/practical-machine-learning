USE [ML]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [PredictiveMaintenance].[TrainModel] 
	@modelName varchar(50), @tableName varchar(100), @topVariables int, @trained_model varbinary(max) OUTPUT
AS
BEGIN
  DECLARE @inquery NVARCHAR(max) = N'SELECT * FROM ' + @tableName;
  EXEC sp_execute_external_script @language = N'R',
                                  @script = N'
train_table <- InputDataSet
train_vars <- rxGetVarNames(train_table)
train_vars <- train_vars[!train_vars  %in% c("label1", "label2", "id", "cycle", "cycle_orig")]

formula <- as.formula(paste("~", paste(train_vars, collapse = "+")))
correlation <- rxCor(formula = formula, 
                     data = train_table)
correlation <- correlation[, "RUL"]
correlation <- abs(correlation)															
correlation <- correlation[order(correlation, decreasing = TRUE)]
correlation <- correlation[-1]
correlation <- correlation[1:top_variables]
formula <- as.formula(paste(paste("RUL~"), paste(names(correlation), collapse = "+")))
print(formula)

model <- MicrosoftML::rxFastLinear(formula = formula,
                     data = train_table,
                     type = "regression",
                     lossFunction = squaredLoss(),
                     convergenceTolerance = 0.01,
                     shuffle = TRUE,
                     trainThreads = NULL,
                     normalize = "warn",
                     maxIterations = NULL,
                     l1Weight = NULL,
                     l2Weight = NULL)

trained_model <- rxSerializeModel(model, realtimeScoringOnly = TRUE)
',
@input_data_1 = @inquery,
@params = N'@top_variables int, @trained_model varbinary(max) OUTPUT',
@top_variables = @topVariables,
@trained_model = @trained_model OUTPUT

END
		 