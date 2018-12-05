USE ML
GO

SELECT * 
FROM [NYCTaxi].[Models]
GO

--Singleton prediction
EXEC sp_helptext'[NYCTaxi].[PredictTipSingleModeSciKitPy]'
GO

SELECT * FROM [NYCTaxi].[fnEngineerFeatures] 
(1, 2.5, 630, 40.763958, -73.973373, 40.782139,-73.977303)

EXEC [NYCTaxi].[PredictTipSingleModeSciKitPy] 
	@model = 'Logistic Regression SciKi',
	@passenger_count = 1,
	@trip_distance = 2.5,
	@trip_time_in_secs = 630,
	@pickup_latitude = 40.763958,
	@pickup_longitude = -73.973373,
	@dropoff_latitude = 40.782139,
	@dropoff_longitude = -73.977303;
GO

--Batch predictions
EXEC sp_helptext'[NYCTaxi].[PredictTipSciKitPy]'
GO

DECLARE @query_string nvarchar(max) 
SET @query_string='
select tipped, fare_amount, passenger_count, trip_time_in_secs, trip_distance,
	NYCTaxi.fnCalculateDistance(pickup_latitude, pickup_longitude,  dropoff_latitude, dropoff_longitude) as direct_distance
from [NYCTaxi].[Testing]'
EXEC [NYCTaxi].[PredictTipSciKitPy] 'Logistic Regression SciKi', @query_string;
GO

--Real-time scoring
EXEC sp_configure 'clr enabled', 1  
GO  
RECONFIGURE  
GO  

--C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\R_SERVICES\library\RevoScaleR\rxLibs\x64>RegisterRExt.exe /installRts /database:ML

DECLARE @model varbinary(max) = (SELECT model FROM NYCTaxi.Models WHERE name = 'Logistic Regression Revoscalepy');
EXEC sp_rxPredict
	@model = @model,
	@inputData = N'select tipped, fare_amount, passenger_count, trip_time_in_secs, trip_distance,
		NYCTaxi.fnCalculateDistance(pickup_latitude, pickup_longitude,  dropoff_latitude, dropoff_longitude) as direct_distance
	from [NYCTaxi].[Testing] where medallion = ''02536669BC1D149D7F2F867CC403E60C''';
GO

--Native predictions
DECLARE @model varbinary(max)
SELECT @model = [model] 
FROM [NYCTaxi].[Models] 
WHERE [name] = 'Native Logistic Regression Revoscalepy';

SELECT  d.*, p.*
FROM PREDICT(MODEL = @model, DATA = [NYCTaxi].[vTesting]  as d)
WITH (tipped_pred float) as p;
GO

--Native predictions with partitioned models
DECLARE @model varbinary(max)
SELECT @model = [model] 
FROM [NYCTaxi].[Models] 
WHERE [name] = 'Native Logistic Regression Revoscalepy.CRD';

WITH ds AS (SELECT tipped, fare_amount, passenger_count, trip_time_in_secs, trip_distance,
				NYCTaxi.fnCalculateDistance(pickup_latitude, pickup_longitude,  dropoff_latitude, dropoff_longitude) AS direct_distance
			FROM [NYCTaxi].[Testing] WHERE [payment_type] = 'CRD')

SELECT  ds.*, p.*
FROM PREDICT(MODEL = @model, DATA = ds)
WITH (tipped_pred float) as p;
GO
