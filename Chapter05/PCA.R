# import data from SELECT statemant
connection_string <- "Driver=SQL Server; Server=MS; Database=ML; Trusted_Connection=yes"
data_sql <- RxSqlServerData( sqlQuery = paste("SELECT CASE [Title]
	WHEN 'Mr' THEN 0
  WHEN 'Mrs' THEN 1
  WHEN 'Miss' THEN 2
  WHEN 'Master' THEN 3
  ELSE 4 END AS Title
 ,[Embarked]
 ,[Fare]
 ,[Pclass]
 ,CASE [Sex]
 WHEN 'male' THEN 0 ELSE 1 END AS Sex
 ,[Age]
 ,[IsAlone]
 FROM [ML].[Titanic].[vPreProcessedData];;"),
  connectionString = connection_string)
data <- rxImport(data_sql)
data.pca <- prcomp(data)
summary(data.pca)
data.pca$rotation[,c("PC1","PC2")]
head(data.pca$x[,c("PC1","PC2")])
data.pca$scale

data.pca <- prcomp(data, scale. = TRUE)
data.pca$scale
summary(data.pca)
data.pca$rotation[,c("PC1","PC2","PC3","PC4","PC5")]
