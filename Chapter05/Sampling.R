connection_string <- "Driver=SQL Server; Server=MS; Database=ML; Trusted_Connection=yes"
table_name <- "[Titanic].[vPreProcessedData]"
table <- RxSqlServerData(table = table_name, connectionString = connection_string)
data <- rxImport(table)

#Stratified sampling
prop.table(table(data$Sex))

library(dplyr)
set.seed(13)
test <- data %>%
  group_by(Sex) %>%
  sample_frac(.10)
prop.table(table(test$Sex))

library(splitstackshape)
set.seed(123)
strata <- stratified(indt = data, group = "Sex",size =.70, bothSets=TRUE)
prop.table(table(strata$SAMP1$Sex))
prop.table(table(strata$SAMP2$Sex))

#Handling Imbalanced Data
library(ROSE)
nrow(data)
prop.table(table(data$Survived)) 
data_balanced_under <- ovun.sample(Survived~.,data, method = "under", seed = 1)$data
nrow(data_balanced_under)
prop.table(table(data_balanced_under$Survived))

data_balanced_over <- ovun.sample(Survived~.,data, method = "over", p=0.5, seed = 1)$data
nrow(data_balanced_over)
prop.table(table(data_balanced_over$Survived))

data_balanced_both <- ovun.sample(Survived~.,data, method = "both", p=0.5, seed = 1)$data
nrow(data_balanced_both)
prop.table(table(data_balanced_both$Survived))
