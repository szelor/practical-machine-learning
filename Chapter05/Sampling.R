connection_string <- "Driver=SQL Server; Server=MS; Database=ML; Trusted_Connection=yes"
table_name <- "[Titanic].[Train]"
table <- RxSqlServerData(table = table_name, connectionString = connection_string)
data <- rxImport(table)
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
