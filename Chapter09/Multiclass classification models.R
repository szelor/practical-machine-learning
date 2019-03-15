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

# Data load
train_table_name <- "PredictiveMaintenance.Train_Features"
test_table_name <- "PredictiveMaintenance.Test_Features"

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

# Modelling
formula <- "label2 ~ a11 + a15 + a4 + a21 + a17 + a3 + a20 + a2 + a12 + a7 + s11 + s4 + s12 + s7 + s15"

model <- MicrosoftML::rxLogisticRegression(formula = formula,
                                           data = train_df,
                                           type = "multiClass")

model_predictions <- rxPredict(modelObject = model,
                               data = train_df,
                               extraVarsToWrite = "label2")
model_empirical_metrics <- evaluate_classifier(actual = model_predictions$label2, 
                                               predicted = model_predictions$PredictedLabel)
model_empirical_metrics

model <- MicrosoftML::rxLogisticRegression(formula = formula,
                                           data = train_df,
                                           type = "multiClass",
                                           optTol = 1e-10,
                                           normalize = "no")

model_predictions <- rxPredict(modelObject = model,
                               data = train_df,
                               extraVarsToWrite = "label2")
model_empirical_metrics <- evaluate_classifier(actual = model_predictions$label2, 
                                               predicted = model_predictions$PredictedLabel)
model_empirical_metrics




library(cvTools)
MicroAvgF1 <- as.numeric() 
Kappa <- as.numeric() 
MacroAvgF1 <- as.numeric() 
kf<-cvFolds(nrow(train_df), K = 10, type = "random")
for (i in 1:10) {
  model <- rxLogisticRegression(formula = formula,
                       data = train_df[kf$which!=i,], 
                       type = "multiClass",
                       optTol = 1e-10,
                       normalize = "no")
  scoreDS <- rxPredict(model, data = train_df[kf$which==i,], extraVarsToWrite = c("label2"))
  
  model_generalization_metrics <- evaluate_classifier(actual = scoreDS$label2, 
                                                      predicted = scoreDS$PredictedLabel)
  
  MicroAvgF1 <- rbind(MicroAvgF1, model_generalization_metrics$Metrics[11,2])
  Kappa <- rbind(Kappa, model_generalization_metrics$Metrics[16,2])
  MacroAvgF1 <- rbind(MacroAvgF1, model_generalization_metrics$Metrics[7,2])
}
sprintf ('Model mean MicroAvgF1: %.2f, stdev MicroAvgF1 %.3f' , mean(MicroAvgF1), sd(MicroAvgF1))
sprintf ('Model mean MacroAvgF1: %.2f, stdev MacroAvgF1: %.3f' ,mean(MacroAvgF1), sd(MacroAvgF1))
sprintf ('Model mean Kappa: %.2f, stdev Kappa: %.3f' ,mean(Kappa), sd(Kappa))

model_predictions <- rxPredict(modelObject = model,
                               data = test_df,
                               extraVarsToWrite = "label2")

model_test_metrics <- evaluate_classifier(actual = model_predictions$label2, 
                                          predicted = model_predictions$PredictedLabel)
model_test_metrics$ConfusionMatrix
model_test_metrics$Metrics[c(7,11,16),2]
