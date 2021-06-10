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

train_table_name <- "PredictiveMaintenance.Train_Features"
test_table_name <- "PredictiveMaintenance.Test_Features"

train_table <- RxSqlServerData(table = train_table_name, 
                               connectionString = connection_string)
train_df <- rxImport(train_table)

test_table <- RxSqlServerData(table = test_table_name,
                              connectionString = connection_string)
test_df <- rxImport(inData = test_table)

# Find top n variables most correlated with RUL
top_variables <- 20
train_vars <- rxGetVarNames(train_df)
train_vars <- train_vars[!train_vars  %in% c("label1", "label2", "id", "cycle_orig", "cycle")]
formula <- NULL
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
formula

# Linear regression
?MicrosoftML::rxFastLinear

linear_model <- MicrosoftML::rxFastLinear(formula = formula,
                                          data = train_df,
                                          type = "regression",
                                          lossFunction = squaredLoss(),
                                          convergenceTolerance = 0.01,
                                          shuffle = TRUE,
                                          trainThreads = NULL,
                                          normalize = "auto",
                                          maxIterations = NULL,
                                          l1Weight = NULL,
                                          l2Weight = NULL)

class(linear_model)
summary(linear_model)

linear_model_prediction <- rxPredict(modelObject = linear_model,
                                     data = test_df,
                                     #writeModelVars = TRUE,
                                     extraVarsToWrite = "RUL")
head(linear_model_prediction)

linear_model_metrics <- evaluateRegressor (observed = linear_model_prediction$RUL, 
                                            predicted = linear_model_prediction$Score)
linear_model_metrics[c(1,2)]

# GLM
?RevoScaleR::rxGlm
GLM_model <- RevoScaleR::rxGlm(formula = formula,
                               data = train_df,
                               family = gaussian)
summary(GLM_model)
as.data.frame(exp(coef(GLM_model)))
GLM_model$deviance/GLM_model$df[2]

GLM_prediction <- rxPredict(modelObject = GLM_model,
                            data = test_df,
                            predVarNames = "GLM_Prediction")

GLM_model_metrics <<- evaluateRegressor(observed = test_df$RUL,
                                        predicted = GLM_prediction$GLM_Prediction)
GLM_model_metrics[c(1,2)]

# Microsoft NNet
?MicrosoftML::rxNeuralNet

nn_model <- MicrosoftML::rxNeuralNet(formula, 
                                     data = train_df, 
                                     type = "regression",
                                     normalize = "auto",
                                     numHiddenNodes = 5,
                                     numIterations = 100,
                                     initWtsDiameter = 0.01,
                                     acceleration = "gpu",
                                     optimizer = sgd(learningRate = 0.01, momentum = 0.7, nag = TRUE,
                                                     lRateRedRatio = 0.8, lRateRedFreq = 10)
                                     )

summary(nn_model)
nn_model_prediction <- rxPredict(modelObject = nn_model,
                                 data = test_df,
                                 extraVarsToWrite = "RUL")

nn_model_metrics <- evaluateRegressor (observed = nn_model_prediction$RUL, 
                                        predicted = nn_model_prediction$Score)
nn_model_metrics[c(1,2)]

nn_model_prediction <- rxPredict(modelObject = nn_model,
                                 data = train_df,
                                 extraVarsToWrite = "RUL")

nn_model_metrics <- evaluateRegressor (observed = nn_model_prediction$RUL, 
                                       predicted = nn_model_prediction$Score)
nn_model_metrics[c(1,2)]

nn_model <- MicrosoftML::rxNeuralNet(formula, 
                                     data = train_df, 
                                     type = "regression",
                                     normalize = "auto",
                                     numHiddenNodes = 15,
                                     numIterations = 10,
                                     initWtsDiameter = 0.005,
                                     optimizer = adaDeltaSgd()
                                     )

summary(nn_model)
nn_model_prediction <- rxPredict(modelObject = nn_model,
                                 data = test_df,
                                 extraVarsToWrite = "RUL")

nn_model_metrics <- evaluateRegressor (observed = nn_model_prediction$RUL, 
                                       predicted = nn_model_prediction$Score)
nn_model_metrics[c(1,2)]


net_definition <- ("input Data auto;
                   hidden Hidden1 [5] tanh from Data all;
                   hidden Hidden2 [2] sigmoid from Hidden1 all;
                   output Result auto from Hidden2 all;") 

nn_model <- MicrosoftML::rxNeuralNet(formula, 
                                     data = train_df, 
                                     type = "regression",
                                     normalize = "warn",
                                     initWtsDiameter = 0.01,
                                     netDefinition = net_definition,
                                     numIterations = 10,
                                     optimizer = adaDeltaSgd() )


library(ggplot2)
ggplot(data = train_df, aes(x = RUL)) +geom_freqpoly()

#Regression tress
library(rpart.plot)
model <- rpart(formula, data=train_df)

#Random forest
?MicrosoftML::rxFastForest
random_forest_model <- MicrosoftML::rxFastForest(formula, 
                                                 data = train_df,
                                                 type = "regression",
                                                 numTrees = 20,
                                                 numLeaves = 20,
                                                 minSplit = 5,
                                                 numBins = 20,
                                                 exampleFraction = 0.7,
                                                 featureFraction = 0.8,
                                                 splitFraction = 0.7)
summary(random_forest_model)
rf_model_prediction <- rxPredict(modelObject = random_forest_model,
                                 data = test_df,
                                 extraVarsToWrite = "RUL",
                                 overwrite = TRUE)
rf_model_metrics <- evaluateRegressor (observed = rf_model_prediction$RUL, 
                                       predicted = rf_model_prediction$Score)

rf_model_metrics[c(1,2)]

#MART gradient boosting
rpart.plot(model, box.palette = "GnBn", 
           branch.lty = 2, shadow.col = "gray", nn = TRUE)

?MicrosoftML::rxFastTrees
gradient_boosting_forest_model <- MicrosoftML::rxFastTrees(formula, 
                                                           data = train_df,
                                                           type = "regression",
                                                           numTrees = 20,
                                                           numLeaves = 20,
                                                           minSplit = 5,
                                                           numBins = 20,
                                                           exampleFraction = 0.7,
                                                           featureFraction = 0.8,
                                                           splitFraction = 0.8,
                                                           learningRate = 0.1)
summary(gradient_boosting_forest_model)
gb_model_prediction <- rxPredict(modelObject = gradient_boosting_forest_model,
                                 data = test_df,
                                 extraVarsToWrite = "RUL",
                                 overwrite = TRUE)
gb_model_metrics <- evaluateRegressor (observed = gb_model_prediction$RUL, 
                                        predicted = gb_model_prediction$Score)
gb_model_metrics[c(1,2)]

#Stacking
?MicrosoftML::rxEnsemble

ensemble_model <- rxEnsemble(
  formula = formula,
  data = train_df,
  type = "regression",
  trainers = list(
    fastForest(numTrees = 20,
             numLeaves = 20,
             minSplit = 5,
             numBins = 20,
             exampleFraction = 0.7,
             featureFraction = 0.8,
             splitFraction = 0.7),
    fastTrees(numTrees = 20,
              numLeaves = 20,
              minSplit = 5,
              numBins = 20,
              exampleFraction = 0.7,
              featureFraction = 0.8,
              splitFraction = 0.8,
              learningRate = 0.1),
    fastLinear(normalize = "auto")),
  replace = TRUE,
  combineMethod = "average")

summary(ensemble_model)

ensemble_model_prediction <- rxPredict(modelObject = ensemble_model,
                                 data = test_df,
                                 extraVarsToWrite = "RUL",
                                 overwrite = TRUE)
ensemble_model_metrics <- evaluateRegressor (observed = ensemble_model_prediction$RUL, 
                                       predicted = ensemble_model_prediction$Score)

ensemble_model_metrics[c(1,2)]

model_factory <- function(train_table_name, test_table_name, top_variables) {
  
  train_table <- RxSqlServerData(table = train_table_name, 
                                 connectionString = connection_string)
  train_df <- rxImport(train_table)
  
  test_table <- RxSqlServerData(table = test_table_name,
                                connectionString = connection_string)
  test_df <- rxImport(inData = test_table)
  train_vars <- rxGetVarNames(train_df)
  train_vars <- train_vars[!train_vars  %in% c("label1", "label2", "id", "cycle_orig", "cycle")]
  formula <- NULL
  formula <- as.formula(paste("~", paste(train_vars, collapse = "+")))
  correlation <- rxCor(formula = formula, 
                       data = train_df)
  correlation <- correlation[, "RUL"]
  correlation <- abs(correlation)
  correlation <- correlation[order(correlation, decreasing = TRUE)]
  correlation <- correlation[-1]
  correlation <- correlation[1:top_variables]
  formula <- as.formula(paste(paste("RUL~"), paste(names(correlation), collapse = "+")))
  
  # Linear regression
  
  linear_model <- MicrosoftML::rxFastLinear(formula = formula,
                                            data = train_df,
                                            type = "regression",
                                            lossFunction = squaredLoss(),
                                            convergenceTolerance = 0.01,
                                            shuffle = TRUE,
                                            trainThreads = NULL,
                                            normalize = "warn",
                                            maxIterations = NULL,
                                            l1Weight = NULL,
                                            l2Weight = NULL)
  
  linear_model_prediction <- rxPredict(modelObject = linear_model,
                                       data = test_df,
                                       extraVarsToWrite = "RUL",
                                       overwrite = TRUE)
  
  linear_model_metrics <<- evaluateRegressor (observed = linear_model_prediction$RUL, 
                                              predicted = linear_model_prediction$Score)
  
  # GLM 
  GLM_model <- RevoScaleR::rxGlm(formula = formula,
                                 data = train_df,
                                 family = gaussian)
  
  GLM_prediction <- rxPredict(modelObject = GLM_model,
                                  data = test_df,
                                  predVarNames = "GLM_Prediction",
                                  overwrite = TRUE)
  
  GLM_model_metrics <<- evaluateRegressor(observed = test_df$RUL,
                                              predicted = GLM_prediction$GLM_Prediction)
  
  # Microsoft NNet
  
  nn_model <- MicrosoftML::rxNeuralNet(formula, 
                                       data = train_df, 
                                       type = "regression",
                                       normalize = "warn",
                                       numHiddenNodes = 20,
                                       numIterations = 100,
                                       initWtsDiameter = 0.01,
                                       optimizer = sgd(learningRate = 0.02, momentum = 0.8, nag = TRUE,
                                                       lRateRedRatio = 0.8, lRateRedFreq = 10) )
  
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
                                                             numTrees = 20,
                                                             numLeaves = 20,
                                                             minSplit = 5,
                                                             numBins = 20,
                                                             exampleFraction = 0.7,
                                                             featureFraction = 0.8,
                                                             splitFraction = 0.8,
                                                             learningRate = 0.1)
  
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
                                                   numTrees = 20,
                                                   numLeaves = 20,
                                                   minSplit = 5,
                                                   numBins = 20,
                                                   exampleFraction = 0.7,
                                                   featureFraction = 0.8,
                                                   splitFraction = 0.7)
  rf_model_prediction <- rxPredict(modelObject = random_forest_model,
                                   data = test_df,
                                   extraVarsToWrite = "RUL",
                                   overwrite = TRUE)
  rf_model_metrics <<- evaluateRegressor (observed = rf_model_prediction$RUL, 
                                          predicted = rf_model_prediction$Score)
  
  #Stacking
  ensemble_model <- MicrosoftML::rxEnsemble(
    formula = formula,
    data = train_df,
    type = "regression",
    trainers = list(
      fastForest(numTrees = 20,
                 numLeaves = 20,
                 minSplit = 5,
                 numBins = 20,
                 exampleFraction = 0.7,
                 featureFraction = 0.8,
                 splitFraction = 0.7),
      fastTrees(numTrees = 20,
                numLeaves = 20,
                minSplit = 5,
                numBins = 20,
                exampleFraction = 0.7,
                featureFraction = 0.8,
                splitFraction = 0.8,
                learningRate = 0.1),
      fastLinear()),
    replace = TRUE,
    combineMethod = "average")
  
  summary(ensemble_model)
  
  ensemble_model_prediction <- rxPredict(modelObject = ensemble_model,
                                         data = test_df,
                                         extraVarsToWrite = "RUL",
                                         overwrite = TRUE)
  ensemble_model_metrics <<- evaluateRegressor (observed = ensemble_model_prediction$RUL, 
                                               predicted = ensemble_model_prediction$Score)
  
}


# Train models on raw data
train_table_name <- "PredictiveMaintenance.train_Labels"
test_table_name <- "PredictiveMaintenance.test_Labels"
top_variables <- 10
model_factory (train_table_name, test_table_name, top_variables)

# Combine metrics and write to SQL
metrics_df <- rbind(linear_model_metrics, GLM_model_metrics, nn_model_metrics, 
                    rf_model_metrics, gb_model_metrics, ensemble_model_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
models <- c("rxFastLinear on raw data",
            "rxGlm on raw data",
            "rxNeuralNet on raw data",
            "rxFastForest on raw data",
            "rxFastTrees on raw data",
            "rxEnsemble on raw data")
models <- cbind(models, top_variables)
metrics_df <- cbind(models, metrics_df)
metrics_df$top_variables <- as.integer(trimws(metrics_df$top_variables))
colnames(metrics_df)[1] <- "Name"
colnames(metrics_df)[2] <- "Variables"

metrics_table <- RxSqlServerData(table = "PredictiveMaintenance.Regression_metrics",
                                 connectionString = connection_string)
#rxDataStep(inData = metrics_df,
#           outFile = metrics_table,
#           overwrite = TRUE)

rxDataStep(inData = metrics_df,
           outFile = metrics_table,
           overwrite = FALSE,
           append = "rows")

# Train models on enchanced data
train_table_name <- "PredictiveMaintenance.train_Features"
test_table_name <- "PredictiveMaintenance.test_Features"
top_variables <-20
model_factory (train_table_name, test_table_name, top_variables)

metrics_df <- rbind(linear_model_metrics, GLM_model_metrics, nn_model_metrics, rf_model_metrics, gb_model_metrics, ensemble_model_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
models <- c("rxFastLinear on enchanced data",
            "rxGlm on enchanced data",
            "rxNeuralNet on enchanced data",
            "rxFastForest on enchanced data",
            "rxFastTrees on enchanced data",
            "rxEnsemble on enchanced data")
models <- cbind(models, top_variables)
metrics_df <- cbind(models, metrics_df)
metrics_df$top_variables <- as.integer(trimws(metrics_df$top_variables))
colnames(metrics_df)[1] <- "Name"
colnames(metrics_df)[2] <- "Variables"

metrics_table <- RxSqlServerData(table = "PredictiveMaintenance.Regression_metrics",
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

metrics_df <- rbind(linear_model_metrics, GLM_model_metrics, nn_model_metrics, rf_model_metrics, gb_model_metrics, ensemble_model_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
models <- c("rxFastLinear on normalized data",
            "rxGlm on normalized data",
            "rxNeuralNet on normalized data",
            "rxFastForest on normalized data",
            "rxFastTrees on normalized data",
            "rxEnsemble on normalized data")
models <- cbind(models, top_variables)
metrics_df <- cbind(models, metrics_df)
metrics_df$top_variables <- as.integer(trimws(metrics_df$top_variables))
colnames(metrics_df)[1] <- "Name"
colnames(metrics_df)[2] <- "Variables"

metrics_table <- RxSqlServerData(table = "PredictiveMaintenance.Regression_metrics",
                                 connectionString = connection_string)
rxDataStep(inData = metrics_df,
           outFile = metrics_table,
           overwrite = FALSE,
           append = "rows")
