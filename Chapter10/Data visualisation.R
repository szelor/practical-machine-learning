install.packages(c("reshape2","ggplot2","ROCR","plyr","Rcpp","stringr","stringi","magrittr","digest","gtable",
                   "proto","scales","munsell","colorspace","labeling","gplots","gtools","gdata","caTools","bitops"))

connection_string <- "Driver=SQL Server; Server=MS; Database=ML; Trusted_Connection=yes"
local <- RxLocalParallel()
rxSetComputeContext(local)

LoanStats <- RxSqlServerData(table = "[LendingClub].[vLoanStats]",
                               connectionString = connection_string)

loans <- rxImport(LoanStats)


library("reshape2")
library("ggplot2")

# creating output directory
mainDir <- 'C:\\temp\\plots'  
dir.create(mainDir, recursive = TRUE, showWarnings = FALSE)  
print("Creating output plot files:", quote=FALSE)  


#filtering numeric columns
numeric_cols <- sapply(loans, is.numeric)

#turn the data into long format (key->value)
loans.lng <- melt(loans[,numeric_cols], id="is_bad")

#plot the distribution for is_bad={0/1} for each numeric column
ggplot(aes(x=value, group=is_bad, colour=factor(is_bad)), data=loans.lng) + geom_density() + facet_wrap(~variable, scales="free")
ggsave('C:\\temp\\plots\\loans.jpg',height=30, width = 50, dpi=300,limitsize = FALSE)

dev.off()
