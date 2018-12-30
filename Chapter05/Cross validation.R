evaluate_classifier <- function (actual, predicted, data, ...) {
  varInfo <- rxGetVarInfo(data)
  if (varInfo[[actual]]$varType != "factor") {
    actual <- paste0("F(",actual,")")
  }
  if (varInfo[[predicted]]$varType != "factor") {
    predicted <- paste0("F(",predicted,")")
  }
  myForm <- as.formula(paste("~",paste(actual,predicted, sep = ":")))
  confusion <- rxCrossTabs(myForm,data = data, returnXtabs = TRUE, ...)
  names(dimnames(confusion)) <- c("actual","predicted")
  #print(confusion)
  #print(prop.table(confusion))
  tn <- confusion[1, 1]
  fp <- confusion[1, 2]
  fn <- confusion[2, 1]
  tp <- confusion[2, 2]
  #print(c(tp,fn,fp,tn))
  accuracy <- (tp + tn) / (tp + fn + fp + tn)
  precision <- tp / (tp + fp)
  recall <- tp / (tp + fn)
  fscore <- 2 * (precision * recall) / (precision + recall)
  metrics <- c("Accuracy" = accuracy,
               "Precision" = precision,
               "Recall" = recall,
               "F-Score" = fscore)
  return(metrics)
}

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
  ,CASE [Sex] WHEN 'male' THEN 0 ELSE 1 END AS Sex
  ,[Age]
  ,[IsAlone]
  ,[Survived]
 FROM [ML].[Titanic].[vPreProcessedData];"),connectionString = connection_string)
data <- rxImport(data_sql)
library(cvTools)
set.seed(12)
accuracy <- as.numeric()
formula <- as.formula("Survived ~ Title+Embarked+Fare+Pclass+Sex+Age+IsAlone")
kf<-cvFolds(nrow(data), K = 4, type = "random")
for (i in 1:4) {
  model <- rxFastTrees(formula, data = data[kf$which!=i,], type = "binary")
  scoreDS <- rxPredict(model, data = data[kf$which==i,], extraVarsToWrite = c("Survived"))
  model_metrics <- evaluate_classifier(actual = "Survived", predicted = "PredictedLabel", data = scoreDS)
  accuracy <- rbind(accuracy, model_metrics[1])
}
sprintf ('Model accuracy: %.2f and stdev %.3f' , 100*mean(accuracy), sd(accuracy))

