# Connection string and compute context
connection_string <- "Driver=SQL Server; Server=MS; Database=ML; Trusted_Connection=yes"
local <- RxLocalParallel()
rxSetComputeContext(local)

# Classification models evaluation metrics
evaluate_classifier <- function(actual=NULL, predicted=NULL, cm=NULL){
  if(is.null(cm)) {
    naVals = union(which(is.na(actual)), which(is.na(predicted)))
    if(length(naVals) > 0) {
      actual = actual[-naVals]
      predicted = predicted[-naVals]
    }
    f = factor(union(unique(actual), unique(predicted)))
    actual = factor(actual, levels = levels(f))
    predicted = factor(predicted, levels = levels(f))
    cm = as.matrix(table(Actual=actual, Predicted=predicted))
  }
  
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
  
  #macro prf
  macroPrecision = mean(precision)
  macroRecall = mean(recall)
  macroF1 = mean(f1)
  
  #1-vs-all matrix
  oneVsAll = lapply(1 : nc,
                    function(i){
                      v = c(cm[i,i],
                            rowsums[i] - cm[i,i],
                            colsums[i] - cm[i,i],
                            n-rowsums[i] - colsums[i] + cm[i,i]);
                      return(matrix(v, nrow = 2, byrow = T))})
  
  s = matrix(0, nrow=2, ncol=2)
  for(i in 1:nc){s=s+oneVsAll[[i]]}
  
  #avg accuracy
  avgAccuracy = sum(diag(s))/sum(s)
  
  #micro prf
  microPrf = (diag(s) / apply(s,1, sum))[1];
  
  #majority class
  mcIndex = which(rowsums==max(rowsums))[1] # majority-class index
  mcAccuracy = as.numeric(p[mcIndex]) 
  mcRecall = 0*p;  mcRecall[mcIndex] = 1
  mcPrecision = 0*p; mcPrecision[mcIndex] = p[mcIndex]
  mcF1 = 0*p; mcF1[mcIndex] = 2 * mcPrecision[mcIndex] / (mcPrecision[mcIndex] + 1)
  
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
    MacroAvgPrecision = macroPrecision,
    MacroAvgRecall = macroRecall,
    MacroAvgF1 = macroF1,
    AvgAccuracy = avgAccuracy,
    MicroAvgPrecision = microPrf,
    MicroAvgRecall = microPrf,
    MicroAvgF1 = microPrf,
    MajorityClassAccuracy = mcAccuracy,
    MajorityClassPrecision = mcPrecision,
    MajorityClassRecall = mcRecall,
    MajorityClassF1 = mcF1,
    Kappa = kappa,
    RandomGuessAccuracy = rgAccuracy,
    RandomGuessPrecision = rgPrecision,
    RandomGuessRecall = rgRecall,
    RandomGuessF1 = rgF1)
  
  colnames(metrics) = classNames
  
  return(list(ConfusionMatrix = cm, Metrics = metrics))
}

# Select variables
train_table_name <- "PredictiveMaintenance.train_Features"
test_table_name <- "PredictiveMaintenance.test_Features"
top_variables <- 20

train_table <- RxSqlServerData(table = train_table_name,
                               connectionString = connection_string,
                               colInfo = list(label2 = list(type = "factor", 
                                                            levels = c("0", "1", "2"))))
train_df <- rxImport(train_table)

test_table <- RxSqlServerData(table = test_table_name,
                              connectionString = connection_string,
                              colInfo = list(label2 = list(type = "factor", 
                                                           levels = c("0", "1", "2"))))
test_df <- rxImport(test_table)

# Find top n variables most correlated with label2
train_vars <- rxGetVarNames(train_df)
train_vars <- train_vars[!train_vars  %in% c("RUL", "label1", "id", "cycle_orig")]
formula <- as.formula(paste("~", paste(train_vars, collapse = "+")))
correlation <- rxCor(formula = formula, 
                     data = train_df,
                     transforms = list(label2 = as.numeric(label2)))
correlation <- correlation[, "label2"]
correlation <- abs(correlation)
correlation <- correlation[order(correlation, decreasing = TRUE)]
correlation <- correlation[-1]
correlation <- correlation[1:top_variables]
formula <- as.formula(paste(paste("label2~"),
                            paste(names(correlation), collapse = "+")))
formula

# Logistic Regression - multiclass classification using L-BFGS
logit_model <- MicrosoftML::rxLogisticRegression(formula = formula,
                                                 data = train_df,
                                                 type = "multiClass",
                                                 l1Weight = 0.95,
                                                 l2Weight = 0.95,
                                                 optTol = 1e-05,
                                                 normalize = "no")
summary(logit_model)
str(logit_model)

logit_model$cachedSummary
logit_model$cachedSummary$summary

logit_model_prediction <- rxPredict(modelObject = logit_model,
                                    data = test_df,
                                    extraVarsToWrite = "label2")

options(scipen = 999)
tail(logit_model_prediction)
round(rowSums(logit_model_prediction[,-c(1,2)]),3)


logit_model_metrics <- evaluate_classifier(actual = logit_model_prediction$label2, 
                                           logit_model_prediction$PredictedLabel)
logit_model_metrics$Metrics[c(4,20),]

# Neural networks 
nn_model <- MicrosoftML::rxNeuralNet(formula, 
                                     data = train_df, 
                                     type = "multiClass",
                                     normalize = "no",
                                     numIterations = 5,
                                     numHiddenNodes = 2,
                                     optimizer = adaDeltaSgd(),
                                     initWtsDiameter = 0.05,
                                     maxNorm=2)

summary(nn_model)
nn_model_prediction <- rxPredict(modelObject = nn_model,
                                 data = test_df,
                                 extraVarsToWrite = "label2")

nn_model_metrics <- evaluate_classifier(actual = nn_model_prediction$label2, 
                                        predicted = nn_model_prediction$PredictedLabel)
nn_model_metrics$Metrics[c(4,20),]

nn_model_prediction <- rxPredict(modelObject = nn_model,
                                 data = train_df,
                                 extraVarsToWrite = "label2")

nn_model_metrics <<- evaluate_classifier(actual = nn_model_prediction$label2, 
                                         predicted = nn_model_prediction$PredictedLabel)
nn_model_metrics$Metrics[c(4,20),]

?RevoScaleR::rxDForest
forest_model <- RevoScaleR::rxDForest(formula = formula,
                                      data = train_df,
                                      method = "class",
                                      nTree = 20,
                                      mTry = 10,
                                      replace = TRUE,
                                      strata = "label2",
                                      seed = 5,
                                      maxDepth = 10,
                                      cp = 0.002,
                                      minSplit = 15,
                                      maxNumBins = 200,
                                      findSplitsInParallel = TRUE)
summary(forest_model)
forest_model$ntree
forest_model$oob.err
forest_model$confusion

forest_model_prediction <- rxPredict(modelObject = forest_model,
                                     data = test_df,
                                     extraVarsToWrite = "label2",
                                     type = "prob")
tail(forest_model_prediction)

forest_model_metrics <- evaluate_classifier(actual = forest_model_prediction$label2, 
                                            predicted = forest_model_prediction$label2_Pred)
forest_model_metrics$Metrics[c(4,20),]

# Boosted trees 
?RevoScaleR::rxBTrees
boosted_model <- RevoScaleR::rxBTrees(formula = formula,
                                      data = train_df,
                                      lossFunction = "multinomial",
                                      learningRate = 0.05,
                                      nTree = 25,
                                      mTry = 10,
                                      replace = FALSE,
                                      sampRate = 0.632,
                                      strata = "label2",
                                      seed = 215,
                                      cp = 0.01,
                                      minSplit = 15,
                                      minBucket = 8,
                                      maxNumBins = 300)

summary(boosted_model)
boosted_model$forest
boosted_model_prediction <- rxPredict(modelObject = boosted_model,
                                      data = test_df,
                                      extraVarsToWrite = "label2",
                                      type = "prob")

subset(boosted_model_prediction, label2!=0, 1:5)
boosted_model_metrics <- evaluate_classifier(actual = boosted_model_prediction$label2, 
                                             boosted_model_prediction$label2_Pred)
boosted_model_metrics$Metrics[c(4,20),]


# Classification models evaluation metrics
evaluate_classifier <- function(actual, predicted) {
  confusion <- table(actual, predicted)
  num_classes <- nlevels(actual)
  tp <- rep(0, num_classes)
  fn <- rep(0, num_classes)
  fp <- rep(0, num_classes)
  tn <- rep(0, num_classes)
  accuracy <- rep(0, num_classes)
  precision <- rep(0, num_classes)
  recall <- rep(0, num_classes)
  f1 <- rep(0, num_classes)
  for(i in 1:num_classes) {
    tn[i] <- sum(confusion[i, i])
    fn[i] <- sum(confusion[-i, i])
    fp[i] <- sum(confusion[i, -i])
    tp[i] <- sum(confusion[-i, -i])
    accuracy[i] <- (tp[i] + tn[i]) / (tp[i] + fn[i] + fp[i] + tn[i])
    precision[i] <- tp[i] / (tp[i] + fp[i])
    recall[i] <- tp[i] / (tp[i] + fn[i])
    f1[i] = 2 * precision[i] * recall[i] / (precision[i] + recall[i])
  }
  macroF1 = mean(f1)
  overall_accuracy <- sum(tp) / sum(confusion)
  average_accuracy <- sum(accuracy) / num_classes
  micro_precision <- sum(tp) / (sum(tp) + sum(fp))
  macro_precision <- sum(precision) / num_classes
  micro_recall <- sum(tp) / (sum(tp) + sum(fn))
  macro_recall <- sum(recall) / num_classes
  metrics <- c("Overall accuracy" = overall_accuracy,
               "Average accuracy" = average_accuracy,
               "Micro-averaged Precision" = micro_precision,
               "Macro-averaged Precision" = macro_precision,
               "Micro-averaged Recall" = micro_recall,
               "Macro-averaged Recall" = macro_recall,
               "Macro-averaged F-1" = macroF1)
  return(metrics)
}


model_factory <- function(train_table_name, test_table_name, top_variables) {

train_table <- RxSqlServerData(table = train_table_name,
                               connectionString = connection_string,
                               colInfo = list(label2 = list(type = "factor", 
                                                            levels = c("0", "1", "2"))))
train_df <- rxImport(train_table)

test_table <- RxSqlServerData(table = test_table_name,
                               connectionString = connection_string,
                               colInfo = list(label2 = list(type = "factor", 
                                                            levels = c("0", "1", "2"))))
test_df <- rxImport(test_table)

# Find top n variables most correlated with label2
train_vars <- rxGetVarNames(train_df)
train_vars <- train_vars[!train_vars  %in% c("RUL", "label1", "id", "cycle_orig")]
formula <- as.formula(paste("~", paste(train_vars, collapse = "+")))
correlation <- rxCor(formula = formula, 
                     data = train_df,
                     transforms = list(label2 = as.numeric(label2)))
correlation <- correlation[, "label2"]
correlation <- abs(correlation)
correlation <- correlation[order(correlation, decreasing = TRUE)]
correlation <- correlation[-1]
correlation <- correlation[1:top_variables]
formula <- as.formula(paste(paste("label2~"),
                            paste(names(correlation), collapse = "+")))

#Multiclass classification

# Logistic Regression - multiclass classification using L-BFGS
logit_model <- MicrosoftML::rxLogisticRegression(formula = formula,
                                                 data = train_df,
                                                 type = "multiClass",
                                                 l1Weight = 0.95,
                                                 l2Weight = 0.95,
                                                 optTol = 1e-05,
                                                 normalize = "no")

logit_model_prediction <- rxPredict(modelObject = logit_model,
                                    data = test_df,
                                    extraVarsToWrite = "label2",
                                    overwrite = TRUE)

logit_model_metrics <<- evaluate_classifier(actual = logit_model_prediction$label2, 
                                            logit_model_prediction$PredictedLabel)

# Neural networks 
nn_model <- MicrosoftML::rxNeuralNet(formula, 
                                     data = train_df, 
                                     type = "multiClass",
                                     normalize = "no",
                                     numIterations = 5,
                                     numHiddenNodes = 2,
                                     optimizer = adaDeltaSgd(),
                                     initWtsDiameter = 0.05,
                                     maxNorm=2)

nn_model_prediction <- rxPredict(modelObject = nn_model,
                                 data = test_df,
                                 extraVarsToWrite = "label2",
                                 overwrite = TRUE)

nn_model_metrics <<- evaluate_classifier(actual = nn_model_prediction$label2, 
                                         predicted = nn_model_prediction$PredictedLabel)

# Random forest
forest_model <- RevoScaleR::rxDForest(formula = formula,
                                      data = train_df,
                                      method = "class",
                                      nTree = 20,
                                      mTry = 10,
                                      replace = TRUE,
                                      strata = "label2",
                                      seed = 5,
                                      maxDepth = 10,
                                      cp = 0.002,
                                      minSplit = 15,
                                      maxNumBins = 200,
                                      findSplitsInParallel = TRUE)

forest_model_prediction <- rxPredict(modelObject = forest_model,
                                    data = test_df,
                                    extraVarsToWrite = "label2",
                                    type = "prob",
                                    overwrite = TRUE)

forest_model_metrics <<- evaluate_classifier(actual = forest_model_prediction$label2, 
                                             predicted = forest_model_prediction$label2_Pred)

# Boosted trees 
boosted_model <- RevoScaleR::rxBTrees(formula = formula,
                                      data = train_df,
                                      lossFunction = "multinomial",
                                      learningRate = 0.05,
                                      nTree = 25,
                                      mTry = 10,
                                      replace = FALSE,
                                      sampRate = 0.632,
                                      strata = "label2",
                                      seed = 215,
                                      cp = 0.01,
                                      minSplit = 15,
                                      minBucket = 8,
                                      maxNumBins = 300)

boosted_model_prediction <- rxPredict(modelObject = boosted_model,
                          data = test_df,
                          extraVarsToWrite = "label2",
                          type = "prob",
                          overwrite = TRUE)

boosted_model_metrics <<- evaluate_classifier(actual = boosted_model_prediction$label2, 
                                              boosted_model_prediction$label2_Pred)
}

# Train models on raw data
train_table_name <- "PredictiveMaintenance.train_Labels"
test_table_name <- "PredictiveMaintenance.test_Labels"
top_variables <- 10
model_factory (train_table_name, test_table_name, top_variables)

# Combine metrics and write to SQL
metrics_df <- rbind(logit_model_metrics, nn_model_metrics, forest_model_metrics, boosted_model_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
models <- c("rxLogisticRegression on raw data",
            "rxNeuralNet on raw data",
            "rxDForest on raw data",
            "rxBTrees on raw data")
models <- cbind(models, top_variables)
metrics_df <- cbind(models, metrics_df)
metrics_df$top_variables <- as.integer(trimws(metrics_df$top_variables))
colnames(metrics_df)[1] <- "Name"
colnames(metrics_df)[2] <- "Variables"
metrics_table <- RxSqlServerData(table = "[PredictiveMaintenance].[Multiclass_metrics]",
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
metrics_df <- rbind(logit_model_metrics, nn_model_metrics, forest_model_metrics, boosted_model_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
models <- c("rxLogisticRegression on enchanced data",
                "rxNeuralNet on enchanced data",
                "rxDForest on enchanced data",
                "rxBTrees on enchanced data")
models <- cbind(models, top_variables)
metrics_df <- cbind(models, metrics_df)
metrics_df$top_variables <- as.integer(trimws(metrics_df$top_variables))
colnames(metrics_df)[1] <- "Name"
colnames(metrics_df)[2] <- "Variables"
metrics_table <- RxSqlServerData(table = "[PredictiveMaintenance].[Multiclass_metrics]",
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
metrics_df <- rbind(logit_model_metrics, nn_model_metrics, forest_model_metrics, boosted_model_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
models <- c("rxLogisticRegression on normalized data",
                "rxNeuralNet on normalized data",
                "rxDForest on normalized data",
                "rxBTrees on normalized data")
models <- cbind(models, top_variables)
metrics_df <- cbind(models, metrics_df)
metrics_df$top_variables <- as.integer(trimws(metrics_df$top_variables))
colnames(metrics_df)[1] <- "Name"
colnames(metrics_df)[2] <- "Variables"
metrics_table <- RxSqlServerData(table = "[PredictiveMaintenance].[Multiclass_metrics]",
                                 connectionString = connection_string)
rxDataStep(inData = metrics_df,
           outFile = metrics_table,
           overwrite = FALSE,
           append = "rows")
