connection_string <- "Driver=SQL Server; Server=MS; Database=ML; Trusted_Connection=yes"
local <- RxLocalParallel()
rxSetComputeContext(local)

LoanStats <- RxSqlServerData(sqlQuery = "SELECT id,[is_bad],[grade],[int_rate],[out_prncp_inv],[policy_code],[installment],[open_acc_6m]
,[all_util],[revol_util],[total_rec_prncp],[bc_util],[percent_bc_gt_75],[home_ownership]
                             FROM [LendingClub].[vLoanStats]",
                             connectionString = connection_string)

loans <- rxImport(LoanStats,
                  rowSelection = (is_train == 0))

loans$is_bad <- as.factor(loans$is_bad)
loans$grade <- as.factor(loans$grade)
loans$home_ownership <- as.factor(loans$home_ownership)
loans$int_rate <- as.numeric(loans$int_rate)
loans$revol_util <- as.numeric(loans$revol_util)

formula <- 'is_bad ~ grade  + out_prncp_inv + int_rate + policy_code + installment  + open_acc_6m + all_util + revol_util + total_rec_prncp + bc_util  + home_ownership'

randomForestObj <- RevoScaleR::rxDForest(formula =  formula,
                             data = loans,
                             nTree = 20,
                             mTry = 5,
                             minSplit = 350,
                             maxDepth = 5)

summary(randomForestObj)
randomForestObj$formula
randomForestObj$mtry
confusion <- randomForestObj$confusion
confusion
tn <- confusion[1, 1]
fp <- confusion[1, 2]
fn <- confusion[2, 1]
tp <- confusion[2, 2]
accuracy <- (tp + tn) / (tp + fn + fp + tn)
precision <- tp / (tp + fp)
recall <- tp / (tp + fn)
fscore <- 2 * (precision * recall) / (precision + recall)
test_metrics <- c("Accuracy" = accuracy,
             "Precision" = precision,
             "Recall" = recall,
             "F-Score" = fscore)
test_metrics


