connection_string <- "Driver=SQL Server; Server=MS; Database=ML; Trusted_Connection=yes"
train_table_name <- "[Titanic].[Temp]"
train_data_table <- RxSqlServerData(table = train_table_name,
                                    connectionString = connection_string)
train_table <- rxImport(train_data_table)


#install.packages("mice")
library(mice)

temp_data <- mice(train_table,m=1,maxit=50,meth='pmm',seed=500)
densityplot(temp_data)
completed_data <- complete(temp_data,1)
summary(train_table$Age)
summary(completed_data$Age)

rxDataStep(inData = completed_data,
           outFile = train_data_table,
           overwrite = TRUE)
