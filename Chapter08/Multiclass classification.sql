USE ML
--TRUNCATE TABLE [PredictiveMaintenance].[Multiclass_metrics]
GO

SELECT [label2], COUNT(*) AS rn
FROM [PredictiveMaintenance].[Train_Features]
GROUP BY [label2]; 

SELECT [label2], COUNT(*) AS rn
FROM [PredictiveMaintenance].[Test_Features]
GROUP BY [label2];
GO

SELECT [Name], [Variables], [Macro-averaged F-1]
FROM [PredictiveMaintenance].[Multiclass_metrics]
ORDER BY [Macro-averaged F-1] DESC;
GO

WITH ranking AS 
	(SELECT *, ROW_NUMBER() OVER (ORDER BY [Macro-averaged F-1] DESC) as Rn
	FROM [PredictiveMaintenance].[Multiclass_metrics])
SELECT [Name], [Variables], [Macro-averaged F-1], [Overall accuracy], Rn
FROM ranking
WHERE Rn <=5 OR Rn >=20
ORDER BY [Macro-averaged F-1] DESC;
GO

USE [master]
GO
EXEC master.dbo.sp_addlinkedserver @server = N'MLServer', @srvproduct=N'', @provider=N'SQLOLEDB',@datasrc='(local)';
EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname = N'MLServer', @locallogin = NULL , @useself = N'True';
GO

USE ML;
GO

DELETE FROM [PredictiveMaintenance].[Models]	
WHERE model_name IN('rxDForest multiclass classification','rxBTrees multiclass classification') 
GO

EXEC sp_helptext '[PredictiveMaintenance].[TrainMulticlassClassificationModel]'
GO

DECLARE @model VARBINARY(MAX);
EXEC [PredictiveMaintenance].[TrainMulticlassClassificationModel] 
	'rxDForest','[PredictiveMaintenance].[Train_Features]', 20,
	 @model OUTPUT;
INSERT INTO MLServer.ML.[PredictiveMaintenance].[Models] (model_name, model) 
VALUES('rxDForest multiclass classification', @model);
GO

DECLARE @model VARBINARY(MAX);
EXEC [PredictiveMaintenance].[TrainMulticlassClassificationModel] 
	'rxBTrees','[PredictiveMaintenance].[train_Features_Normalized]', 20,
	 @model OUTPUT;
INSERT INTO MLServer.ML.[PredictiveMaintenance].[Models] (model_name, model) 
VALUES('rxBTrees multiclass classification', @model);
GO

DECLARE @model_raw VARBINARY(MAX) = 
	(SELECT model FROM [PredictiveMaintenance].[Models] 
	WHERE model_name = 'rxDForest multiclass classification')
SELECT 
  a.id, a.cycle, a.label2, p.*
 FROM PREDICT(MODEL = @model_raw, DATA = [PredictiveMaintenance].[Test_Features] as a) 
 WITH("0_prob" float, "1_prob" float, "2_prob" float, "label2_Pred" nvarchar(max)) as p
 WHERE a.id >95;
GO

												 
DECLARE @model_raw VARBINARY(MAX) = 
	(SELECT model FROM [PredictiveMaintenance].[Models] 
	WHERE model_name = 'rxBTrees multiclass classification')
SELECT 
  a.id, a.cycle_orig, a.label2, p.*
 FROM PREDICT
	(MODEL = @model_raw, 
	DATA = [PredictiveMaintenance].[test_Features_Normalized] as a)
 WITH("0_prob" float, "1_prob" float, "2_prob" float, "label2_Pred" nvarchar(max)) as p
  WHERE a.id >95;
GO

