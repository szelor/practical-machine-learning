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

roc <- function(data, observed, predicted) {
  data <- data[, c(observed, predicted)]
  data[[observed]] <- as.numeric(as.character(data[[observed]]))
  rxRoc(actualVarName = observed,
             predVarNames = predicted,
             data = data)
}
# Select variables
train_table_name <- "PredictiveMaintenance.train_Features"
test_table_name <- "PredictiveMaintenance.test_Features"
top_variables <- 25

train_table <- RxSqlServerData(table = train_table_name,
                               connectionString = connection_string,
                               colInfo = list(label1 = list(type = "factor", levels = c("0", "1"))))
train_df <- rxImport(train_table)

test_table <- RxSqlServerData(table = test_table_name,
                              connectionString = connection_string,
                              colInfo = list(label1 = list(type = "factor", levels = c("0", "1"))))
test_df <- rxImport(test_table)

# Find top n variables most correlated with label2
train_vars <- rxGetVarNames(train_df)
train_vars <- train_vars[!train_vars  %in% c("RUL", "label1", "id", "cycle_orig","cycle")]
formula <- as.formula(paste("~", paste(train_vars, collapse = "+")))
correlation <- rxCor(formula = formula, 
                     data = train_df,
                     transforms = list(label1 = as.numeric(label1)))
correlation <- correlation[, "label2"]
correlation <- abs(correlation)
correlation <- correlation[order(correlation, decreasing = TRUE)]
correlation <- correlation[-1]
correlation <- correlation[1:top_variables]
formula <- as.formula(paste(paste("label2~"),
                            paste(names(correlation), collapse = "+")))
formula

# Logistic Regression - binary classification using L-BFGS
?MicrosoftML::rxLogisticRegression

logit_model <- MicrosoftML::rxLogisticRegression(formula = formula,
                                                 data = train_df,
                                                 type = "binary",
                                                 l1Weight = 0.9,
                                                 l2Weight = 0.9,
                                                 maxIterations = 100,
                                                 optTol = 1e-07,
                                                 normalize = "warn",
                                                 memorySize = 100)
summary(logit_model)

logit_model_prediction <- rxPredict(modelObject = logit_model,
                                    data = test_df,
                                    extraVarsToWrite = "label1")

tail(logit_model_prediction)
as.data.frame(tail(e1071::sigmoid(logit_model_prediction$Score.1)))

head(logit_model_prediction[order(-logit_model_prediction$Probability.1),c(1,2,4)])

logit_model_metrics <- evaluate_classifier(actual = "label1", 
                                           predicted = "PredictedLabel",
                                          data = logit_model_prediction)
logit_model_metrics$Metrics[c(1,5)]

roc_curve(data = logit_model_prediction,
          observed = "label1",
          predicted = "Probability.1")

# Neural networks 
library(devtools)
source_url('https://gist.githubusercontent.com/fawda123/7471137/raw/466c1474d0a505ff044412703516c34f1a4684a5/nnet_plot_update.r')
library(neuralnet)
t <- train_df
t$label1 <- as.numeric(t$label1)
nn_model<-neuralnet(formula,data=t,hidden=3)
plot.nnet(nn_model)

nn_model <- MicrosoftML::rxNeuralNet(formula, 
                                     data = train_df, 
                                     type = "binary",
                                     normalize = "auto",
                                     numIterations = 100,
                                     optimizer = adaDeltaSgd(),
                                     numHiddenNodes = 5,
                                     initWtsDiameter = 0.05)

summary(nn_model)
nn_model_prediction <- rxPredict(modelObject = nn_model,
                                 data = test_df,
                                 extraVarsToWrite = "label1")
tail(nn_model_prediction[,-3],9)

nn_model_metrics <- evaluate_classifier(actual = "label1", predicted = "PredictedLabel",
                                         data = nn_model_prediction)
nn_model_metrics$Metrics[1]
nn_model_metrics$Metrics[5]
rxAuc(roc(data = nn_model_prediction,
          observed = "label1",
          predicted = "Probability.1"))

nn_model_prediction$newLabel1 <- ifelse(nn_model_prediction$Probability.1<=0.4, 0,1)
nn_model_metrics <- evaluate_classifier(actual = "label1", predicted = "newLabel1",
                                        data = nn_model_prediction)
nn_model_metrics$Metrics[c(1,5)]

#Random forest
?rxFastForest
random_forest_model <- MicrosoftML::rxFastForest(formula, 
                                                 data = train_df,
                                                 type = "binary",
                                                 numTrees = 10,
                                                 numLeaves = 15,
                                                 exampleFraction = 0.6,
                                                 featureFraction = 0.7,
                                                 splitFraction = 0.6,
                                                 minSplit = 15)
summary(random_forest_model)
rf_model_prediction <- rxPredict(modelObject = random_forest_model,
                                 data = test_df,
                                 extraVarsToWrite = "label1",
                                 overwrite = TRUE)

rf_model_metrics <- evaluate_classifier(actual = "label1", predicted = "PredictedLabel",
                                         data = rf_model_prediction)

rf_model_metrics$Metrics[1]
rf_model_metrics$Metrics[5]
rxAuc(roc(data = rf_model_prediction,
          observed = "label1",
          predicted = "Probability.1"))

#MART gradient boosting
?rxFastTrees
gradient_boosting_model <- MicrosoftML::rxFastTrees(formula, 
                           data = train_df,
                           type = "binary",
                           numTrees = 15,
                           exampleFraction = 0.6,
                           featureFraction = 0.9,
                           learningRate = 0.01,
                           unbalancedSets = FALSE,
                           numLeaves = 10,
                           minSplit = 5,
                           splitFraction = 0.8)

summary(gradient_boosting_model)

gb_model_prediction <- rxPredict(modelObject = gradient_boosting_model,
                                 data = test_df,
                                 extraVarsToWrite = "label1")
gb_model_metrics <- evaluate_classifier(actual = "label1", predicted = "PredictedLabel",
                                         data = gb_model_prediction)
gb_model_metrics$Metrics[c(1,5)]
rxAuc(roc(data = gb_model_prediction,
          observed = "label1",
          predicted = "Probability.1"))

#Stacking
?MicrosoftML::rxEnsemble

ensemble_model <- rxEnsemble(
  formula = formula,
  data = train_df,
  type = "binary",
  trainers = list(
    logisticRegression(),
    logisticRegression(l1Weight = 0.9,
        l2Weight = 0.9,
        normalize = "warn"),
    fastTrees(numTrees = 20,
              exampleFraction = 0.6,
              featureFraction = 0.9,
              learningRate = 0.01,
              unbalancedSets = FALSE,
              numLeaves = 10,
              minSplit = 5,
              splitFraction = 0.8),
    fastForest(numTrees = 10,
               numLeaves = 15,
               exampleFraction = 0.6,
               featureFraction = 0.7,
               splitFraction = 0.6,
               minSplit = 15)),
  replace = TRUE,
  modelCount = 8,
  combineMethod = "vote")

summary(ensemble_model)

ensemble_model_prediction <- rxPredict(modelObject = ensemble_model,
                                       data = test_df,
                                       extraVarsToWrite = "label1")

ensemble_model_metrics <- evaluate_classifier(actual = "label1", 
                                              predicted = "PredictedLabel",
                                              data = ensemble_model_prediction)
ensemble_model_metrics$Metrics[c(1,5)]

InformationValue::plotROC(ensemble_model_prediction$label1, ensemble_model_prediction$Probability.1)
InformationValue::AUROC(ensemble_model_prediction$label1, ensemble_model_prediction$Probability.1)
rxAuc(roc(data = ensemble_model_prediction,
          observed = "label1",
          predicted = "Probability.1"))


model_factory <- function(train_table_name, test_table_name, top_variables) {

train_table <- RxSqlServerData(table = train_table_name,
                               connectionString = connection_string,
                               colInfo = list(label1 = list(type = "factor", levels = c("0", "1"))))
train_df <- rxImport(train_table)

test_table <- RxSqlServerData(table = test_table_name,
                               connectionString = connection_string,
                               colInfo = list(label1 = list(type = "factor", levels = c("0", "1"))))
test_df <- rxImport(test_table)

# Find top n variables most correlated with label1
train_vars <- rxGetVarNames(train_df)
train_vars <- train_vars[!train_vars  %in% c("RUL", "label2", "id", "cycle_orig","cycle")]
formula <- as.formula(paste("~", paste(train_vars, collapse = "+")))
correlation <- rxCor(formula = formula, 
                     data = train_df,
                     transforms = list(label1 = as.numeric(label1)))
correlation <- correlation[, "label1"]
correlation <- abs(correlation)
correlation <- correlation[order(correlation, decreasing = TRUE)]
correlation <- correlation[-1]
correlation <- correlation[1:top_variables]
formula <- as.formula(paste(paste("label1~"),
                            paste(names(correlation), collapse = "+")))

# Logistic Regression - binary classification using L-BFGS

logit_model <- MicrosoftML::rxLogisticRegression(formula = formula,
                                                 data = train_df,
                                                 type = "binary",
                                                 l1Weight = 0.9,
                                                 l2Weight = 0.9,
                                                 maxIterations = 100,
                                                 optTol = 1e-07,
                                                 normalize = "warn",
                                                 memorySize = 100)

logit_model_prediction <- rxPredict(modelObject = logit_model,
                             data = test_df,
                             extraVarsToWrite = "label1",
                             overwrite = TRUE)

logit_model_metrics <<- evaluate_classifier(actual = "label1", predicted = "PredictedLabel",
                                        data = logit_model_prediction)


# Neural networks 

nn_model <- MicrosoftML::rxNeuralNet(formula, 
                                     data = train_df, 
                                     type = "binary",
                                     normalize = "auto",
                                     numIterations = 100,
                                     optimizer = adaDeltaSgd(),
                                     numHiddenNodes = 5,
                                     initWtsDiameter = 0.05)

nn_model_prediction <- rxPredict(modelObject = nn_model,
                          data = test_df,
                          extraVarsToWrite = "label1",
                          overwrite = TRUE)

nn_model_prediction$newLabel1 <- ifelse(nn_model_prediction$Probability.1<=0.4, 0,1)
nn_model_metrics <<- evaluate_classifier(actual = "label1", predicted = "newLabel1",
                                        data = nn_model_prediction)


#Random forest
random_forest_model <- MicrosoftML::rxFastForest(formula, 
                                                 data = train_df,
                                                 type = "binary",
                                                 numTrees = 10,
                                                 numLeaves = 15,
                                                 exampleFraction = 0.6,
                                                 featureFraction = 0.7,
                                                 splitFraction = 0.6,
                                                 minSplit = 15)

rf_model_prediction <- rxPredict(modelObject = random_forest_model,
                                 data = test_df,
                                 extraVarsToWrite = "label1",
                                 overwrite = TRUE)

rf_model_metrics <<- evaluate_classifier(actual = "label1", predicted = "PredictedLabel",
                                         data = rf_model_prediction)


#MART gradient boosting
gradient_boosting_model <- MicrosoftML::rxFastTrees(formula, 
                                                    data = train_df,
                                                    type = "binary",
                                                    numTrees = 20,
                                                    exampleFraction = 0.6,
                                                    featureFraction = 0.9,
                                                    learningRate = 0.01,
                                                    unbalancedSets = FALSE,
                                                    numLeaves = 10,
                                                    minSplit = 5,
                                                    splitFraction = 0.8)

gb_model_prediction <- rxPredict(modelObject = gradient_boosting_model,
                                 data = test_df,
                                 extraVarsToWrite = "label1",
                                 overwrite = TRUE)

gb_model_metrics <<- evaluate_classifier(actual = "label1", predicted = "PredictedLabel",
                                         data = gb_model_prediction)

ensemble_model <- MicrosoftML::rxEnsemble(
  formula = formula,
  data = train_df,
  type = "binary",
  trainers = list(
    logisticRegression(),
    logisticRegression(l1Weight = 0.9,
                       l2Weight = 0.9,
                       normalize = "warn"),
    fastTrees(numTrees = 20,
              exampleFraction = 0.6,
              featureFraction = 0.9,
              learningRate = 0.01,
              unbalancedSets = FALSE,
              numLeaves = 10,
              minSplit = 5,
              splitFraction = 0.8),
    fastForest(numTrees = 10,
               numLeaves = 15,
               exampleFraction = 0.6,
               featureFraction = 0.7,
               splitFraction = 0.6,
               minSplit = 15)),
  replace = TRUE,
  modelCount = 8,
  combineMethod = "average")

ensemble_model_prediction <- rxPredict(modelObject = ensemble_model,
                                       data = test_df,
                                       extraVarsToWrite = "label1")

ensemble_model_metrics <<- evaluate_classifier(actual = "label1", 
                                              predicted = "PredictedLabel",
                                              data = ensemble_model_prediction)

}

# Classification models evaluation metrics
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


# Train models on raw data
train_table_name <- "PredictiveMaintenance.train_Labels"
test_table_name <- "PredictiveMaintenance.test_Labels"
top_variables <-20
model_factory (train_table_name, test_table_name, top_variables)


# Combine metrics and write to SQL
metrics_df <- rbind(logit_model_metrics, nn_model_metrics, 
                    rf_model_metrics, gb_model_metrics,ensemble_model_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
models <- c("rxLogisticRegression on raw data",
                "rxNeuralNet on raw data",
                "rxFastForest on raw data",
                "rxFastTrees on raw data",
                "rxEnsemble on raw data")
models <- cbind(models, top_variables)
metrics_df <- cbind(models, metrics_df)
metrics_df$top_variables <- as.integer(trimws(metrics_df$top_variables))
colnames(metrics_df)[1] <- "Name"
colnames(metrics_df)[2] <- "Variables"
metrics_table <- RxSqlServerData(table = "PredictiveMaintenance.Binary_metrics",
                                 connectionString = connection_string)
rxDataStep(inData = metrics_df,
           outFile = metrics_table,
           overwrite = TRUE)

rxDataStep(inData = metrics_df,
           outFile = metrics_table,
           overwrite = FALSE,
           append = "rows")

# Train models on enchanced data
train_table_name <- "PredictiveMaintenance.train_Features"
test_table_name <- "PredictiveMaintenance.test_Features"
top_variables <-20
model_factory (train_table_name, test_table_name, top_variables)

# Combine metrics and write to SQL
metrics_df <- rbind(logit_model_metrics, nn_model_metrics, rf_model_metrics, 
                    gb_model_metrics, ensemble_model_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
models <- c("rxLogisticRegression on enchanced data",
                "rxNeuralNet on enchanced data",
                "rxFastForest on enchanced data",
                "rxFastTrees on enchanced data",
                "rxEnsemble on enchanced data")
models <- cbind(models, top_variables)
metrics_df <- cbind(models, metrics_df)
metrics_df$top_variables <- as.integer(trimws(metrics_df$top_variables))
colnames(metrics_df)[1] <- "Name"
colnames(metrics_df)[2] <- "Variables"
metrics_table <- RxSqlServerData(table = "PredictiveMaintenance.Binary_metrics",
                                 connectionString = connection_string)
rxDataStep(inData = metrics_df,
           outFile = metrics_table,
           overwrite = FALSE,
           append = "rows")

# Train models on normalized data
train_table_name <- "PredictiveMaintenance.train_Features_Normalized"
test_table_name <- "PredictiveMaintenance.test_Features_Normalized"
top_variables <-20
model_factory (train_table_name, test_table_name, top_variables)


# Combine metrics and write to SQL
metrics_df <- rbind(logit_model_metrics, nn_model_metrics, rf_model_metrics,
                    gb_model_metrics, ensemble_model_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
models <- c("rxLogisticRegression on normalized data",
                "rxNeuralNet on normalized data",
                "rxFastForest on normalized data",
                "rxFastTrees on normalized data",
                "rxEnsemble on normalized data")
models <- cbind(models, top_variables)
metrics_df <- cbind(models, metrics_df)
metrics_df$top_variables <- as.integer(trimws(metrics_df$top_variables))
colnames(metrics_df)[1] <- "Name"
colnames(metrics_df)[2] <- "Variables"
metrics_table <- RxSqlServerData(table = "PredictiveMaintenance.Binary_metrics",
                                 connectionString = connection_string)
rxDataStep(inData = metrics_df,
           outFile = metrics_table,
           overwrite = FALSE,
           append = "rows")

