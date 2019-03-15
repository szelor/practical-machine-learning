# Connection string and compute context
connection_string <- "Driver=SQL Server; Server=MS; Database=ML; Trusted_Connection=yes"
local <- RxLocalParallel()
rxSetComputeContext(local)

evaluate_classifier <- function (actual, predicted, data, ...) {
  varInfo <- rxGetVarInfo(data)
  if (varInfo[[actual]]$varType != "factor") {
    actual <- paste0("F(",actual,")")
  }
  if (varInfo[[predicted]]$varType != "factor") {
    predicted <- paste0("F(",predicted,")")
  }
  myForm <- as.formula(paste("~",paste(actual,predicted, sep = ":")))
  cm <- rxCrossTabs(myForm,data = data, returnXtabs = TRUE, ...)
  names(dimnames(cm)) <- c("actual","predicted")
  
  n = sum(cm) # number of instances
  nc = nrow(cm) # number of classes
  diag = diag(cm) # number of correctly classified instances per class 
  rowsums = apply(cm, 1, sum) # number of instances per class
  colsums = apply(cm, 2, sum) # number of predictions per class
  p = rowsums / n # distribution of instances over the classes
  q = colsums / n # distribution of instances over the predicted classes
  
  #accuracy
  accuracy = sum(diag) / n
  
  #per class prf
  recall = diag / rowsums
  precision = diag / colsums
  f1 = 2 * precision * recall / (precision + recall)
  
  #random/expected accuracy
  expAccuracy = sum(p*q)
  #kappa
  kappa = (accuracy - expAccuracy) / (1 - expAccuracy)
  
  #random guess
  rgAccuracy = 1 / nc
  rgPrecision = p
  rgRecall = 0*p + 1 / nc
  rgF1 = 2 * p / (nc * p + 1)
  
  
  classNames = names(diag)
  if(is.null(classNames)) classNames = paste("C",(1:nc),sep="")
  metrics = rbind(
    Accuracy = accuracy,
    Precision = precision,
    Recall = recall,
    F1 = f1,
    Kappa = kappa,
    RandomGuessAccuracy = rgAccuracy,
    RandomGuessPrecision = rgPrecision,
    RandomGuessRecall = rgRecall,
    RandomGuessF1 = rgF1)
  
  colnames(metrics) = classNames
  return(list(ConfusionMatrix = cm, Metrics = metrics))
}

# ROC curve
roc_curve <- function(data, observed, predicted) {
  data <- data[, c(observed, predicted)]
  data[[observed]] <- as.numeric(as.character(data[[observed]]))
  rxRocCurve(actualVarName = observed,
             predVarNames = predicted,
             data = data)
}

auc <- function(data, observed, predicted) {
  data <- data[, c(observed, predicted)]
  data[[observed]] <- as.numeric(as.character(data[[observed]]))
  rxAuc(rxRoc(actualVarName = observed,
              predVarNames = predicted,
              data = data))
}

# Data load
train_table_name <- "PredictiveMaintenance.Train_Features"
test_table_name <- "PredictiveMaintenance.Test_Features"

train_table <- RxSqlServerData(table = train_table_name,
                               connectionString = connection_string,
                               colInfo = list(label1 = list(type = "factor", levels = c("0", "1"))))
train_df <- rxImport(train_table)

test_table <- RxSqlServerData(table = test_table_name,
                              connectionString = connection_string,
                              colInfo = list(label1 = list(type = "factor", levels = c("0", "1"))))
test_df <- rxImport(test_table)

# Modelling
formula <- "label1 ~ a11 + a4 + a15 + a21 + a17 + a3 + a20 + a2 + a12 + a7 + s11 + s4 + s12 + s7 + s15"

model <- MicrosoftML::rxLogisticRegression(formula = formula,
                                                 data = train_df,
                                                 type = "binary",
                                                 l1Weight = 0.8,
                                                 l2Weight = 0.8,
                                                 maxIterations = 20,
                                                 normalize = "yes")
model_predictions <- rxPredict(modelObject = model,
                               data = train_df,
                               extraVarsToWrite = "label1")
model_empirical_metrics <- evaluate_classifier(actual = "label1", 
                                               predicted = "PredictedLabel",
                                               data = model_predictions)
model_empirical_metrics

roc_curve(data = model_predictions,
          observed = "label1",
          predicted = "Probability.1")

auc(data = model_predictions,
    observed = "label1",
    predicted = "Probability.1")

model <- MicrosoftML::rxNeuralNet(formula, 
                                  data = train_df, 
                                  type = "binary",
                                  normalize = "yes",
                                  numIterations = 40,
                                  optimizer = adaDeltaSgd(),
                                  numHiddenNodes = 1,
                                  initWtsDiameter = 0.1)

model_predictions <- rxPredict(modelObject = model,
                               data = train_df,
                               extraVarsToWrite = "label1")
model_predictions$PredictedLabel <- ifelse(model_predictions$Probability.1<=0.4, 0,1)
model_empirical_metrics <- evaluate_classifier(actual = "label1", 
                                               predicted = "PredictedLabel",
                                               data = model_predictions)
model_empirical_metrics

roc_curve(data = model_predictions,
          observed = "label1",
          predicted = "Probability.1")

auc(data = model_predictions,
    observed = "label1",
    predicted = "Probability.1")


library(cvTools)
F1 <- as.numeric() 
Kappa <- as.numeric() 
Precision <- as.numeric() 
Recall <- as.numeric() 
kf<-cvFolds(nrow(train_df), K = 10, type = "random")
for (i in 1:10) {
  model <- rxNeuralNet(formula = formula,
                       data = train_df[kf$which!=i,], 
                       type = "binary",
                       normalize = "yes",
                       numIterations = 40,
                       optimizer = adaDeltaSgd(),
                       numHiddenNodes = 1,
                       initWtsDiameter = 0.1)
  scoreDS <- rxPredict(model, data = train_df[kf$which==i,], extraVarsToWrite = c("label1"))
  scoreDS$PredictedLabel <- ifelse(scoreDS$Probability.1<=0.4, 0,1)
  
  model_generalization_metrics <- evaluate_classifier(actual = "label1", 
                                                      predicted = "PredictedLabel",
                                                      data = scoreDS)
  Precision <- rbind(Precision, model_generalization_metrics$Metrics[2,2])
  Recall <- rbind(Recall, model_generalization_metrics$Metrics[3,2])
  F1 <- rbind(F1, model_generalization_metrics$Metrics[4,2])
  Kappa <- rbind(Kappa, model_generalization_metrics$Metrics[5,2])
}
sprintf ('Model mean F1: %.2f, stdev F1 %.3f' , mean(F1), sd(F1))
sprintf ('Model mean Kappa: %.2f, stdev Kappa: %.3f' ,mean(Kappa), sd(Kappa))
sprintf ('Model mean Precision: %.2f, stdev Precision: %.3f' ,mean(Precision), sd(Precision))
sprintf ('Model mean Recall: %.2f, stdev Recall: %.3f' ,mean(Recall), sd(Recall))

for (i in 1:10) {
  model <- rxNeuralNet(formula = formula,
                       data = train_df[kf$which!=i,], 
                       type = "binary",
                       normalize = "yes",
                       numIterations = 100,
                       optimizer = adaDeltaSgd(),
                       numHiddenNodes = 8,
                       initWtsDiameter = 0.05)
  scoreDS <- rxPredict(model, data = train_df[kf$which==i,], extraVarsToWrite = c("label1"))
  scoreDS$PredictedLabel <- ifelse(scoreDS$Probability.1<=0.45, 0,1)
  
  model_generalization_metrics <- evaluate_classifier(actual = "label1", 
                                                      predicted = "PredictedLabel",
                                                      data = scoreDS)
  Precision <- rbind(Precision, model_generalization_metrics$Metrics[2,2])
  Recall <- rbind(Recall, model_generalization_metrics$Metrics[3,2])
  F1 <- rbind(F1, model_generalization_metrics$Metrics[4,2])
  Kappa <- rbind(Kappa, model_generalization_metrics$Metrics[5,2]            )
}
sprintf ('Model mean F1: %.2f, stdev F1 %.3f' , mean(F1), sd(F1))
sprintf ('Model mean Kappa: %.2f, stdev Kappa: %.3f' ,mean(Kappa), sd(Kappa))
sprintf ('Model mean Precision: %.2f, stdev Precision: %.3f' ,mean(Precision), sd(Precision))
sprintf ('Model mean Recall: %.2f, stdev Recall: %.3f' ,mean(Recall), sd(Recall))


model <- rxNeuralNet(formula = formula,
                     data = train_df, 
                     type = "binary",
                     normalize = "yes",
                     numIterations = 100,
                     optimizer = adaDeltaSgd(),
                     numHiddenNodes = 8,
                     initWtsDiameter = 0.05)

model_predictions <- rxPredict(modelObject = model,
                               data = test_df,
                               extraVarsToWrite = "label1")
model_predictions$PredictedLabel <- ifelse(model_predictions$Probability.1<=0.25, 0,1)

model_test_metrics <- evaluate_classifier(actual = "label1", 
                                          predicted = "PredictedLabel",
                                          data = model_predictions)

model_generalization_metrics$Metrics[2:5,2]

roc_curve(data = model_predictions,
          observed = "label1",
          predicted = "Probability.1")

auc(data = model_predictions,
    observed = "label1",
    predicted = "Probability.1")

