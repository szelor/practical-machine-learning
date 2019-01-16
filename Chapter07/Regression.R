# Connection string and compute context
connection_string <- "Driver=SQL Server; Server=MS; Database=ML; Trusted_Connection=yes"
local <- RxLocalParallel()
rxSetComputeContext(local)

# Regression models evaluation metrics
evaluateRegressor <- function(observed, predicted) {
  mean_observed <- mean(observed)
  se <- (observed - predicted)^2
  ae <- abs(observed - predicted)
  sem <- (observed - mean_observed)^2
  aem <- abs(observed - mean_observed)
  mae <- mean(ae)
  rmse <- sqrt(mean(se))
  rae <- sum(ae) / sum(aem)
  rse <- sum(se) / sum(sem)
  rsq <- 1 - rse
  metrics <- c("Mean Absolute Error" = mae,
               "Root Mean Squared Error" = rmse,
               "Relative Absolute Error" = rae,
               "Relative Squared Error" = rse,
               "Coefficient of Determination" = rsq)
  return(metrics)
}

# Select variables

train_table_name <- "PredictiveMaintenance.train_Features"
test_table_name <- "PredictiveMaintenance.test_Features"
top_variables <-35

model_factory <- function(train_table_name, test_table_name, top_variables) {
  train_table <- RxSqlServerData(table = train_table_name, 
                                 connectionString = connection_string)
  train_df <- rxImport(train_table)
  
  test_table <- RxSqlServerData(table = test_table_name,
                                connectionString = connection_string)
  test_df <- rxImport(inData = test_table)
  
  # Find top n variables most correlated with RUL
  train_vars <- rxGetVarNames(train_df)
  train_vars <- train_vars[!train_vars  %in% c("label1", "label2", "id", "cycle_orig", "cycle")]
  formula <- as.formula(paste("~", paste(train_vars, collapse = "+")))
  correlation <- rxCor(formula = formula, 
                       data = train_df)
  correlation <- correlation[, "RUL"]
  correlation <- abs(correlation)
  correlation <- correlation[order(correlation, decreasing = TRUE)]
  correlation <- correlation[-1]
  as.data.frame(correlation)
  correlation <- correlation[1:top_variables]
  formula <- as.formula(paste(paste("RUL~"), paste(names(correlation), collapse = "+")))
  #formula
  
  # Linear regression
  #?MicrosoftML::rxFastLinear
  
  linear_model <- MicrosoftML::rxFastLinear(formula = formula,
                                            data = train_df,
                                            lossFunction = squaredLoss(),
                                            type = "regression",
                                            convergenceTolerance = 0.005,
                                            #maxIterations = 50,
                                            shuffle = TRUE,
                                            normalize = "warn",
                                            l1Weight = NULL,
                                            l2Weight = NULL)
  
  #summary(linear_model)
  linear_model_prediction <- rxPredict(modelObject = linear_model,
                                       data = test_df,
                                       extraVarsToWrite = "RUL",
                                       overwrite = TRUE)
  head(linear_model_prediction)
  
  linear_model_metrics <<- evaluateRegressor (observed = linear_model_prediction$RUL, 
                                              predicted = linear_model_prediction$Score)
  
  # Poisson regression
  #?RevoScaleR::rxGlm
  poisson_model <- RevoScaleR::rxGlm(formula = formula,
                                     data = train_df,
                                     family = poisson())
  #summary(poisson_model)
  poisson_prediction <- rxPredict(modelObject = poisson_model,
                                  data = test_df,
                                  predVarNames = "Poisson_Prediction",
                                  overwrite = TRUE)
  
  poisson_model_metrics <<- evaluateRegressor(observed = test_df$RUL,
                                              predicted = poisson_prediction$Poisson_Prediction)
  
  # Microsoft NNet
  #?MicrosoftML::rxNeuralNet
  
  nn_model <- MicrosoftML::rxNeuralNet(formula, 
                                       data = train_df, 
                                       type = "regression",
                                       normalize = "warn",
                                       numIterations = 100,
                                       numHiddenNodes = 10,
                                       optimizer = sgd(learningRate = 0.0001, momentum = 0.5, nag = FALSE, weightDecay = 0,
                                                       lRateRedRatio = 0.9, lRateRedFreq = 50, lRateRedErrorRatio = 0),
                                       initWtsDiameter = 0.8,
                                       acceleration = "gpu")
  
  #summary(nn_model)
  nn_model_prediction <- rxPredict(modelObject = nn_model,
                                   data = test_df,
                                   extraVarsToWrite = "RUL",
                                   overwrite = TRUE)
  
  nn_model_metrics <<- evaluateRegressor (observed = nn_model_prediction$RUL, 
                                          predicted = nn_model_prediction$Score)
  
  #MART gradient boosting
  gradient_boosting_forest_model <- MicrosoftML::rxFastTrees(formula, 
                                                             data = train_df,
                                                             type = "regression",
                                                             numTrees = 50,
                                                             numLeaves = 20,
                                                             learningRate = 0.2)
  #summary(gradient_boosting_forest_model)
  gb_model_prediction <- rxPredict(modelObject = gradient_boosting_forest_model,
                                   data = test_df,
                                   extraVarsToWrite = "RUL",
                                   overwrite = TRUE)
  gb_model_metrics <<- evaluateRegressor (observed = gb_model_prediction$RUL, 
                                          predicted = gb_model_prediction$Score)
  
  #Random forest
  random_forest_model <- MicrosoftML::rxFastForest(formula, 
                                                   data = train_df,
                                                   type = "regression",
                                                   numTrees = 50,
                                                   numLeaves = 20,
                                                   minSplit = 30)
  #summary(random_forest_model)
  rf_model_prediction <- rxPredict(modelObject = random_forest_model,
                                   data = test_df,
                                   extraVarsToWrite = "RUL",
                                   overwrite = TRUE)
  rf_model_metrics <<- evaluateRegressor (observed = rf_model_prediction$RUL, 
                                          predicted = rf_model_prediction$Score)
  
  
  
}


# Train models on raw data
train_table_name <- "PredictiveMaintenance.train_Labels"
test_table_name <- "PredictiveMaintenance.test_Labels"
top_variables <- 20
model_factory (train_table_name, test_table_name, top_variables)

# Combine metrics and write to SQL
metrics_df <- rbind(linear_model_metrics, poisson_model_metrics, nn_model_metrics, rf_model_metrics, gb_model_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
Algorithms <- c("rxFastLinear on raw data",
                "rxGlm on raw data",
                "rxNeuralNet on raw data",
                "rxFastForest on raw data",
                "rxFastTrees on raw data")
metrics_df <- cbind(Algorithms, metrics_df)
metrics_table <- RxSqlServerData(table = "PredictiveMaintenance.Regression_metrics",
                                 connectionString = connection_string)
rxDataStep(inData = metrics_df,
           outFile = metrics_table,
           overwrite = FALSE,
           append = "rows")

# Train models on enchanced data
train_table_name <- "PredictiveMaintenance.train_Features"
test_table_name <- "PredictiveMaintenance.test_Features"
top_variables <-35
model_factory (train_table_name, test_table_name, top_variables)

metrics_df <- rbind(linear_model_metrics, poisson_model_metrics, nn_model_metrics, rf_model_metrics, gb_model_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
Algorithms <- c("rxFastLinear on enchanced data",
                "rxGlm on enchanced data",
                "rxNeuralNet on enchanced data",
                "rxFastForest on enchanced data",
                "rxFastTrees on enchanced data")
metrics_df <- cbind(Algorithms, metrics_df)
metrics_table <- RxSqlServerData(table = "PredictiveMaintenance.Regression_metrics",
                                 connectionString = connection_string)
rxDataStep(inData = metrics_df,
           outFile = metrics_table,
           overwrite = FALSE,
           append = "rows")

# Train models on normalized data
train_table_name <- "PredictiveMaintenance.train_Features_Normalized"
test_table_name <- "PredictiveMaintenance.test_Features_Normalized"
top_variables <-35
model_factory (train_table_name, test_table_name, top_variables)

metrics_df <- rbind(linear_model_metrics, poisson_model_metrics, nn_model_metrics, rf_model_metrics, gb_model_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
Algorithms <- c("rxFastLinear on normalized data",
                "rxGlm on normalized data",
                "rxNeuralNet on normalized data",
                "rxFastForest on normalized data",
                "rxFastTrees on normalized data")
metrics_df <- cbind(Algorithms, metrics_df)
metrics_table <- RxSqlServerData(table = "PredictiveMaintenance.Regression_metrics",
                                 connectionString = connection_string)
rxDataStep(inData = metrics_df,
           outFile = metrics_table,
           overwrite = FALSE,
           append = "rows")
