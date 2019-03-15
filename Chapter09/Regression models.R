# Install R and Python packages
install.packages("reticulate", repos = "https://cran.r-project.org/")
devtools::install_github("ModelOriented/shapper")
shapper::install_shap()
install.packages("DALEX2", repos = "https://cran.r-project.org/")

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

# Data load
train_table_name <- "PredictiveMaintenance.Train_Features"
test_table_name <- "PredictiveMaintenance.Test_Features"

train_table <- RxSqlServerData(table = train_table_name, 
                               connectionString = connection_string)
train_df <- rxImport(train_table)

test_table <- RxSqlServerData(table = test_table_name,
                              connectionString = connection_string)
test_df <- rxImport(inData = test_table)

# Modelling
formula <- "RUL ~ a4 + a11 + a21 + a15 + a20 + a17 + a12 + a7 + a2 + a3 + s11 + s4 + s12 + s7 + s15"
model <- MicrosoftML::rxFastTrees(formula, 
                                  data = train_df,
                                  type = "regression",
                                  numTrees = 2,
                                  numLeaves = 5,
                                  minSplit = 10,
                                  numBins = 100,
                                  exampleFraction = 0.9,
                                  featureFraction = 1,
                                  splitFraction = 1,
                                  learningRate = 0.05)
model_predictions <- rxPredict(modelObject = model,
                               data = train_df,
                               extraVarsToWrite = "RUL")
model_empirical_metrics <- evaluateRegressor (observed = model_predictions$RUL, 
                                              predicted = model_predictions$Score)
as.data.frame(model_empirical_metrics)

model <- MicrosoftML::rxFastTrees(formula, 
                                  data = train_df,
                                  type = "regression",
                                  numTrees = 10,
                                  numLeaves = 10,
                                  minSplit = 5,
                                  numBins = 100,
                                  exampleFraction = 0.9,
                                  featureFraction = 1,
                                  splitFraction = 1,
                                  learningRate = 0.05)

model_predictions <- rxPredict(modelObject = model,
                               data = train_df,
                               extraVarsToWrite = "RUL")
model_empirical_metrics <- evaluateRegressor (observed = model_predictions$RUL, 
                                              predicted = model_predictions$Score)
as.data.frame(model_empirical_metrics)

model <- MicrosoftML::rxFastTrees(formula, 
                                  data = train_df,
                                  type = "regression",
                                  numTrees = 30,
                                  numLeaves = 20,
                                  minSplit = 5,
                                  numBins = 100,
                                  exampleFraction = 0.9,
                                  featureFraction = 1,
                                  splitFraction = 1,
                                  learningRate = 0.05)

model_predictions <- rxPredict(modelObject = model,
                               data = train_df,
                               extraVarsToWrite = "RUL")
model_empirical_metrics <- evaluateRegressor (observed = model_predictions$RUL, 
                                              predicted = model_predictions$Score)
as.data.frame(model_empirical_metrics)

library(cvTools)
MAE <- as.numeric() 
RMSE <- as.numeric() 
r2 <- as.numeric() 
kf<-cvFolds(nrow(train_df), K = 10, type = "random")
for (i in 1:10) {
  model <- rxFastTrees(formula = formula,
                       data = train_df[kf$which!=i,], 
                       type = "regression",
                       numTrees = 30,
                       numLeaves = 20,
                       minSplit = 5,
                       numBins = 100,
                       exampleFraction = 0.9,
                       featureFraction = 1,
                       splitFraction = 1,
                       learningRate = 0.05)
  scoreDS <- rxPredict(model, data = train_df[kf$which==i,], extraVarsToWrite = c("RUL"))
  model_generalization_metrics <- evaluateRegressor (observed = scoreDS$RUL, 
                                                     predicted = scoreDS$Score)
  MAE <- rbind(MAE, model_generalization_metrics[1])
  RMSE <- rbind(RMSE, model_generalization_metrics[2])
  r2 <- rbind(r2, model_generalization_metrics[5])
}
sprintf ('Model mean MAE: %.2f, stdev MAE %.3f' , mean(MAE), sd(MAE))
sprintf ('Model mean RMSE: %.2f, stdev RMSE: %.3f' ,mean(RMSE), sd(RMSE))
sprintf ('Model mean R2: %.2f, stdev R2: %.3f' ,mean(r2), sd(r2))

model_prediction <- rxPredict(modelObject = model,
                              data = test_df,
                              extraVarsToWrite = "RUL")
model_test_metrics <- evaluateRegressor (observed = model_prediction$RUL, 
                                                   predicted = model_prediction$Score)
as.data.frame(model_test_metrics)

model <- MicrosoftML::rxFastTrees(formula, 
                                  data = train_df,
                                  type = "regression",
                                  numTrees = 300,
                                  numLeaves = 200,
                                  minSplit = 1,
                                  numBins = 100,
                                  exampleFraction = 1,
                                  featureFraction = 1,
                                  splitFraction = 1,
                                  learningRate = 0.05)

model_predictions <- rxPredict(modelObject = model,
                               data = train_df,
                               extraVarsToWrite = "RUL")
model_empirical_metrics <- evaluateRegressor (observed = model_predictions$RUL, 
                                              predicted = model_predictions$Score)
as.data.frame(model_empirical_metrics)

for (i in 1:10) {
  model <- MicrosoftML::rxFastTrees(formula, 
                                    data = train_df,
                                    type = "regression",
                                    numTrees = 300,
                                    numLeaves = 200,
                                    minSplit = 1,
                                    numBins = 100,
                                    exampleFraction = 1,
                                    featureFraction = 1,
                                    splitFraction = 1,
                                    learningRate = 0.05)
  scoreDS <- rxPredict(model, data = train_df[kf$which==i,], extraVarsToWrite = c("RUL"))
  model_generalization_metrics <- evaluateRegressor (observed = scoreDS$RUL, 
                                                     predicted = scoreDS$Score)
  MAE <- rbind(MAE, model_generalization_metrics[1])
  RMSE <- rbind(RMSE, model_generalization_metrics[2])
  r2 <- rbind(r2, model_generalization_metrics[5])
}
sprintf ('Model mean MAE: %.2f, stdev MAE %.3f' , mean(MAE), sd(MAE))
sprintf ('Model mean RMSE: %.2f, stdev RMSE: %.3f' ,mean(RMSE), sd(RMSE))
sprintf ('Model mean R2: %.2f, stdev R2: %.3f' ,mean(r2), sd(r2))


model_prediction <- rxPredict(modelObject = model,
                              data = test_df,
                              extraVarsToWrite = "RUL")
model_test_metrics <- evaluateRegressor (observed = model_prediction$RUL, 
                                         predicted = model_prediction$Score)
as.data.frame(model_test_metrics)


# SHapley Additive exPlanations

summary(model)
library(shapper)
library(DALEX2)

exp_model <- DALEX2::explain(model,
                     data = train_df[,c("RUL","a4","a11","a21","a15","a20","a17","a12","a7",
                                        "a2","a3","s11","s4","s12","s7","s15")])
new_observation <- test_df[100, c("RUL","a4","a11","a21","a15","a20","a17","a12","a7",
                                  "a2","a3","s11","s4","s12","s7","s15")]

ive_model <- shapper::shap(exp_model, 
                  predict_function = rxPredict,
                  nsamples = as.integer(100),
                  new_observation = new_observation)
plot(ive_model)

new_observation <- test_df[c(10,100), c("RUL","a4","a11","a21","a15","a20","a17","a12","a7",
                                  "a2","a3","s11","s4","s12","s7","s15")]

ive_model <- shap(exp_model, 
                  predict_function = rxPredict,
                  nsamples = as.integer(100),
                  new_observation = new_observation)
plot(ive_model)

m <- rxNeuralNet(formula, 
                 data = train_df,
                 type = "regression")
summary(m)
