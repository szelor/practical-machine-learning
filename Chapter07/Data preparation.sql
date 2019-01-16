USE ML;
GO

SELECT id, cycle, RUL 
FROM [PredictiveMaintenance].[Train_Labels];
GO

SELECT id, cycle, te.RUL, tr.RUL AS [Ground truth]
FROM [PredictiveMaintenance].[Test_Labels] AS Te
JOIN (SELECT RUL, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) as rn 
	  FROM [PredictiveMaintenance].[PM_Truth]) as Tr
ON Te.id = 	Tr.rn;
GO

SELECT *
FROM [PredictiveMaintenance].[Train_Features];
GO

SELECT *
FROM PredictiveMaintenance.Test_Features;
GO

SELECT *
FROM [PredictiveMaintenance].[Train_Features_Normalized];

SELECT *
FROM [PredictiveMaintenance].[Test_Features_Normalized];
GO
