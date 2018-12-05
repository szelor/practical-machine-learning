USE ML
GO
DELETE FROM [NYCTaxi].[Models];
GO

SELECT TOP 1 *
FROM [NYCTaxi].[Trips];
GO

EXEC sp_helptext '[NYCTaxi].[SerializePlots]';
GO

EXEC [NYCTaxi].[SerializePlots];
GO

--Feature engineering
EXEC sp_helptext '[NYCTaxi].[fnCalculateDistance]'
GO
EXEC sp_helptext '[NYCTaxi].[fnEngineerFeatures]'
GO

EXEC sp_helptext '[NYCTaxi].[TrainTestSplit]' 
GO

EXEC [NYCTaxi].[TrainTestSplit] @pct=70
GO

SELECT TOP 10 *
FROM [NYCTaxi].[Training]

SELECT COUNT(*) 
FROM [NYCTaxi].[Training]
UNION ALL
SELECT COUNT(*) 
FROM [NYCTaxi].[Testing]
GO

--SciKit model
EXEC sp_helptext '[NYCTaxi].[TrainTipPredictionModelSciKitPy]'
GO
DECLARE @model varbinary(max);
EXEC [NYCTaxi].[TrainTipPredictionModelSciKitPy] @model OUTPUT;
INSERT INTO [NYCTaxi].[Models] (name, model) 
VALUES ('Logistic Regression SciKi', @model);
GO

--Revoscalepy
EXEC sp_helptext '[NYCTaxi].[TrainTipPredictionModelRxPy]'
GO
DECLARE @model varbinary(max);
EXEC [NYCTaxi].[TrainTipPredictionModelRxPy] @model OUTPUT;
INSERT INTO [NYCTaxi].[Models] (name, model) 
VALUES ('Logistic Regression Revoscalepy', @model);
GO

--Revoscalepy native serialization
EXEC sp_helptext '[NYCTaxi].[TrainTipPredictionModelRxPyNative]'
GO 
DECLARE @model varbinary(max);
EXEC [NYCTaxi].[TrainTipPredictionModelRxPyNative] @model OUTPUT;
INSERT INTO [NYCTaxi].[Models] (name, model) 
VALUES ('Native Logistic Regression Revoscalepy', @model);
GO

--Partition-based models
EXEC sp_helptext '[NYCTaxi].[TrainTipPredictionModelsRxPyNativePerPartition]'
GO 

EXEC [NYCTaxi].[TrainTipPredictionModelsRxPyNativePerPartition]; 
GO

SELECT [name], left(model,20)
FROM [NYCTaxi].[Models];
GO
