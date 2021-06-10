DECLARE @model VARBINARY(MAX);
DECLARE @name VARCHAR(255) = 'rxFastTrees';
EXEC [PredictiveMaintenance].[TrainModel] 'rxFastLinear','[PredictiveMaintenance].[train_Features]', 20, @model OUTPUT;
IF EXISTS(SELECT * FROM [PredictiveMaintenance].[Models]  where model_name=@name)
  UPDATE [PredictiveMaintenance].[Models] 
  SET [model] = @model
  WHERE [model_name] = @name
ELSE
	INSERT INTO [PredictiveMaintenance].[Models] (model_name, model) 
	VALUES(@name, @model);



DECLARE @model varbinary(max) = (SELECT model 
	FROM [PredictiveMaintenance].[Models] 
	WHERE model_name = 'rxFastTrees');
EXEC sp_rxPredict
	@model = @model,
	@inputData = N'SELECT RUL , a4 , a11 , a12 , a15 , a7 , a17 , a21 , a20 , a2 , a3 , 
    s11 , s4 , s12 , s7 , s15 , s20 , s21 , a13 , a8 , s2
	FROM [PredictiveMaintenance].[test_Features]';

