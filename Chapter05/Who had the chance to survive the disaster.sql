USE ML
GO

TRUNCATE TABLE Titanic.Models;
GO

EXEC sp_helptext '[Titanic].[TrainSurvivedRandomForestClassifier]'
GO

DECLARE @model VARBINARY(MAX);
EXEC [Titanic].[TrainSurvivedRandomForestClassifier] @model OUTPUT;
INSERT INTO Titanic.Models VALUES('Random Forest' ,@model);
GO

SELECT *
FROM Titanic.Models;