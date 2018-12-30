USE ML
GO

TRUNCATE TABLE Titanic.Models;
GO

EXEC sp_helptext '[Titanic].[TrainSurvivedDTClassifier]'
GO

DECLARE @model VARBINARY(MAX), @score float, @deviation float;
EXEC [Titanic].[TrainSurvivedDTClassifier] @model OUTPUT, @score OUTPUT, @deviation OUTPUT;
INSERT INTO Titanic.Models VALUES('DecisionTree max_depth=8, min_samples_split=4' ,@model, @score, @deviation);
GO

SELECT name, score, deviation, left(model,15) as model
FROM Titanic.Models;
GO

EXEC sp_helptext '[Titanic].[PredictSurvived]'
GO

SELECT * FROM [Titanic].[Test]
GO  

EXEC [Titanic].[PredictSurvived] 'DecisionTree max_depth=8, min_samples_split=4';
GO
