####################################################################################################
## Load data
####################################################################################################

# Connection string and compute context
connection_string <- "Driver=SQL Server; Server=MS; Database=ML; Trusted_Connection=yes"
local <- RxLocalParallel()
rxSetComputeContext(local)

# Load train data into SQL table
train_file <- "PM_Train.csv"
train_columns <- c(id = "numeric",
                  cycle = "numeric",
                  setting1 = "numeric",
                  setting2 = "numeric",
                  setting3 = "numeric",
                  s1 = "numeric",
                  s2 = "numeric",
                  s3 = "numeric",
                  s4 = "numeric",
                  s5 = "numeric",
                  s6 = "numeric",
                  s7 = "numeric",
                  s8 = "numeric",
                  s9 = "numeric",
                  s10 = "numeric",
                  s11 = "numeric",
                  s12 = "numeric",
                  s13 = "numeric",
                  s14 = "numeric",
                  s15 = "numeric",
                  s16 = "numeric",
                  s17 = "numeric",
                  s18 = "numeric",
                  s19 = "numeric",
                  s20 = "numeric",
                  s21 = "numeric")
train_data_text <- RxTextData(file = train_file, colClasses = train_columns)
head(train_data_text)
train_table_name <- "PredictiveMaintenance.PM_Train"
train_data_table <- RxSqlServerData(table = train_table_name,
                                    connectionString = connection_string,
                                    colClasses = train_columns)
rxDataStep(inData = train_data_text,
           outFile = train_data_table,
           overwrite = TRUE)

# Load test data into SQL table

test_file <- "PM_Test.csv"
test_data_text <- RxTextData(file = test_file, colClasses = train_columns)
test_table_name <- "PredictiveMaintenance.PM_Test"
test_data_table <- RxSqlServerData(table = test_table_name,
                                   connectionString = connection_string,
                                   colClasses = train_columns)
rxDataStep(inData = test_data_text,
           outFile = test_data_table,
           overwrite = TRUE)

# Load truth data into SQL table
truth_file <- "PM_Truth.csv"
truth_columns <- c(RUL = "numeric")

truth_data_text <- RxTextData(file = truth_file, colClasses = truth_columns)
truth_table_name <- "PredictiveMaintenance.PM_Truth"
truth_data_table <- RxSqlServerData(table = truth_table_name,
                                    connectionString = connection_string,
                                    colClasses = truth_columns)
rxDataStep(inData = truth_data_text,
           outFile = truth_data_table,
           overwrite = TRUE)

####################################################################################################
### Data overwiew 
####################################################################################################

# Import train into data frame 
train_table <- rxImport(train_data_table)

rxGetInfo(data = train_table)
rxGetInfo(data = train_table, getVarInfo = TRUE)
rxGetInfo(data = train_table, getVarInfo = TRUE, numRows = 5)
rxSummary(formula =  ~ ., data = train_table, summaryStats = c("Mean", "StdDev", "Min", "Max", "MissingObs"))
rxSummary(formula = ~ cycle, data = train_table)
rxSummary(formula = cycle ~ F(id), data = train_table, summaryStats = c("Min", "Max"))
rxHistogram(formula = ~s11, data = train_table, numBreaks = 50)
rxHistogram(formula = ~ s11 | F(id), data = train_table)
rxLinePlot(formula = s11~cycle|id, data = train_table)

####################################################################################################
## Data labeling
####################################################################################################

data_label <- function(data) { 
  data <- as.data.frame(data)  
  max_cycle <- plyr::ddply(data, "id", plyr::summarise, max = max(cycle))
  if (!is.null(truth)) {
    max_cycle <- plyr::join(max_cycle, truth, by = "id")
    max_cycle$max <- max_cycle$max + max_cycle$RUL
    max_cycle$RUL <- NULL
  }
  data <- plyr::join(data, max_cycle, by = "id")
  # Label for regression
  data$RUL <- data$max - data$cycle
  # Label for binary/multi-class classification
  data$label1 <- ifelse(data$RUL <= 30, 1, 0)
  # Label for multi-class classification
  data$label2 <- ifelse(data$RUL <= 15, 2, data$label1)
  data$max <- NULL
  
  return(data)
}

#Add data labels for train data
tagged_table_name <- "PredictiveMaintenance.Train_Labels"
truth_df <- NULL 
tagged_table_train = RxSqlServerData(table = tagged_table_name, 
                                     colClasses = train_columns,
                                     connectionString = connection_string)
inDataSource <- RxSqlServerData(table = train_table_name, 
                                connectionString = connection_string, 
                                colClasses = train_columns)
rxDataStep(inData = inDataSource, 
           outFile = tagged_table_train,  
           overwrite = TRUE,
           transformObjects = list(truth = truth_df),
           transformFunc = data_label)


# Add data labels for test data
truth_df <- rxImport(truth_data_table)
#add index to the original truth table 
truth_df$id <- 1:nrow(truth_df)
tagged_table_name <- "PredictiveMaintenance.test_Labels"
tagged_table_test = RxSqlServerData(table = tagged_table_name, 
                                    colClasses = train_columns,
                                    connectionString = connection_string)
inDataSource <- RxSqlServerData(table = test_table_name, 
                                connectionString = connection_string, 
                                colClasses = train_columns)
rxDataStep(inData = inDataSource, 
           outFile = tagged_table_test,  
           overwrite = TRUE,
           transformObjects = list(truth = truth_df),
           transformFunc = data_label)

####################################################################################################
### Time series analysis 
####################################################################################################

# Import train into data frame 
train_table <- rxImport(tagged_table_train)

dev.off()
#install.packages("astsa")
astsa::lag2.plot(train_table$RUL,train_table$s12,8)
astsa::lag2.plot(train_table$s12,train_table$s12,5)

par(mfrow=c(2,1))
acf(train_table$s12, lag.max=30)
pacf(train_table$s12, lag.max=30)


####################################################################################################
## Create features from the raw data by computing the rolling means
## Only the last cycle of each test engine is selected for prediction
####################################################################################################
#install.packages("zoo")

create_features <- function(data) {
  create_rolling_stats <- function(data) {
    data <- data[, sensor]
    rolling_mean <- zoo::rollapply(data = data,
                                   width = window,
                                   FUN = mean,
                                   align = "right",
                                   partial = TRUE)
    rolling_mean <- as.data.frame(rolling_mean)
    names(rolling_mean) <- gsub("s", "a", names(rolling_mean))
    rolling_sd <- zoo::rollapply(data = data,
                                 width = window,
                                 FUN = sd,
                                 align = "right",
                                 partial = TRUE)
    rolling_sd <- as.data.frame(rolling_sd)
    rolling_sd[is.na(rolling_sd)] <- 0
    names(rolling_sd) <- gsub("s", "sd", names(rolling_sd))
    rolling_stats <- cbind(rolling_mean, rolling_sd)
    return(rolling_stats)
  }
  
  data <- as.data.frame(data)
  window <- ifelse(window < nrow(data), window, nrow(data))  
  features <- plyr::ddply(data, "id", create_rolling_stats)
  features$id <- NULL
  data <- cbind(data, features)
  
  if (!identical(data_type, "train"))
  {
    max_cycle <- plyr::ddply(data, "id", plyr::summarise, cycle = max(cycle))
    data <- plyr::join(max_cycle, data, by = c("id", "cycle"))
  }
  
  return(data)
}

window_size <- 5
tagged_table_name <- "PredictiveMaintenance.vTrain_Labels"
tagged_table_train <- RxSqlServerData(table = tagged_table_name,
                                   connectionString = connection_string)
train_vars <- names(rxGetVarInfo(tagged_table_train))
sensor_vars <- train_vars[grep("s[[:digit:]]", train_vars)]

# Create features for train dataset and save into SQL table
train_table_features <- RxSqlServerData(table = "PredictiveMaintenance.Train_Features",
                               connectionString = connection_string)

rxDataStep(inData = tagged_table_train, 
           outFile = train_table_features,  
           overwrite = TRUE,
           transformObjects = list(window = window_size,
                                   sensor = sensor_vars,
                                   data_type = "train"),
           transformFunc = create_features)

# Create features for test dataset and save into SQL table
tagged_table_name <- "PredictiveMaintenance.vTest_Labels"
tagged_table_test <- RxSqlServerData(table = tagged_table_name,
                                      connectionString = connection_string)
test_table_features <- RxSqlServerData(table = "PredictiveMaintenance.Test_Features",
                              connectionString = connection_string)

rxDataStep(inData = tagged_table_test, 
           outFile = test_table_features,  
           overwrite = TRUE,
           transformObjects = list(window = window_size,
                                   sensor = sensor_vars,
                                   data_type = "test"),
           transformFunc = create_features)

####################################################################################################
## Feature normalization with min-max
####################################################################################################

train_summary <- rxSummary(formula = ~ ., 
                           data = train_table_features, 
                           summaryStats = c("min", "max"))
train_summary <- train_summary$sDataFrame
train_summary <- subset(train_summary, !Name %in% c("id", "RUL", "label1", "label2"))
train_summary
train_vars <- train_summary$Name
train_vars_min <- train_summary$Min
train_vars_max <- train_summary$Max

normalize_data <- function(data) {
  data <- as.data.frame(data)
  data_to_keep <- data[, c("id", "cycle")]
  names(data_to_keep) <- c("id", "cycle_orig")
  data$id <- NULL
  temp <- data[, vars]
  normalize <- function(x, min, max) {
    z <- (x - min) / (max - min)
    return(z)
  }
  temp <- mapply(normalize, temp, vars_min, vars_max)
  temp[is.nan(temp)] <- NA
  data <- data[, which(!names(data) %in% vars)]
  data <- cbind(data_to_keep, temp, data)
  data <- data[, apply(!is.na(data), 2, all)]
  data$cycle <- NULL 
  return(data)
}

#Train feature normalization
train_table_normalized <- RxSqlServerData(table = "PredictiveMaintenance.Train_Features_Normalized",
                               connectionString = connection_string)

rxDataStep(inData = train_table_features,
           outFile = train_table_normalized,
           transformObjects = list(vars = train_vars,
                                   vars_min = train_vars_min,
                                   vars_max = train_vars_max),
           transformFunc = normalize_data,
           overwrite = TRUE,
           rowsPerRead = 1000)

# Test feature normalization 
test_table_normalized <- RxSqlServerData(table = "PredictiveMaintenance.Test_Features_Normalized",
                               connectionString = connection_string)

rxDataStep(inData = test_table_features,
           outFile = test_table_normalized,
           transformObjects = list(vars = train_vars,
                                   vars_min = train_vars_min,
                                   vars_max = train_vars_max),
           transformFunc = normalize_data,
           overwrite = TRUE,
           rowsPerRead = 1000)

