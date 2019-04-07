connection_string <- "Driver=SQL Server; Server=MS; Database=ML; Trusted_Connection=yes"
local <- RxLocalParallel()
rxSetComputeContext(local)

stage_table <- RxSqlServerData(table = "[LendingClub].[LoanStatsStaging]",
                               connectionString = connection_string)

install.packages("funModeling")
library(tidyverse)

na_to_zero_vars <-
  c("mths_since_last_delinq", "mths_since_last_record",
    "mths_since_last_major_derog")

dat <- readr::read_csv("LoanStats_2017Q1.csv", na = "NULL")
problems(dat)
meta_loans <- funModeling::df_status(dat, print_results = TRUE)

dat <- 
  dat %>%
  mutate_at(.vars = na_to_zero_vars, .funs = funs(replace(., is.na(.), 0)))

meta_loans <-
  meta_loans %>%
  mutate(uniq_rat = unique / nrow(dat)) %>%
  select(variable, unique, uniq_rat) %>%
  mutate(unique = unique, uniq_rat = scales::percent(uniq_rat)) %>%
  knitr::kable()
meta_loans

rxDataStep(inData = dat,
           outFile = stage_table,
           overwrite = TRUE)  

dat <- readr::read_csv("LoanStats_2017Q2.csv", na = "NULL")
dat <- 
  dat %>%
  mutate_at(.vars = na_to_zero_vars, .funs = funs(replace(., is.na(.), 0)))
rxDataStep(inData = dat,
           outFile = stage_table,
           overwrite = TRUE)  

dat <- readr::read_csv("LoanStats_2017Q3.csv", na = "NULL")
dat <- 
  dat %>%
  mutate_at(.vars = na_to_zero_vars, .funs = funs(replace(., is.na(.), 0)))
rxDataStep(inData = dat,
           outFile = stage_table,
           overwrite = TRUE)  

dat <- readr::read_csv("LoanStats_2017Q4.csv", na = "NULL")
dat <- 
  dat %>%
  mutate_at(.vars = na_to_zero_vars, .funs = funs(replace(., is.na(.), 0)))
rxDataStep(inData = dat,
           outFile = stage_table,
           overwrite = TRUE)  

dat <- readr::read_csv("LoanStats_2018Q1.csv", na = "NULL")
dat <- 
  dat %>%
  mutate_at(.vars = na_to_zero_vars, .funs = funs(replace(., is.na(.), 0)))
rxDataStep(inData = dat,
           outFile = stage_table,
           overwrite = TRUE) 

dat <- readr::read_csv("LoanStats_2018Q2.csv", na = "NULL")
dat <- 
  dat %>%
  mutate_at(.vars = na_to_zero_vars, .funs = funs(replace(., is.na(.), 0)))
rxDataStep(inData = dat,
           outFile = stage_table,
           overwrite = TRUE) 


dat <- readr::read_csv("LoanStats_2018Q3.csv", na = "NULL", skip = 1)
dat <- 
  dat %>%
  mutate_at(.vars = na_to_zero_vars, .funs = funs(replace(., is.na(.), 0)))
rxDataStep(inData = dat,
           outFile = stage_table,
           overwrite = TRUE) 

dat <- readr::read_csv("LoanStats_2018Q4.csv", na = "NULL")
dat <- 
  dat %>%
  mutate_at(.vars = na_to_zero_vars, .funs = funs(replace(., is.na(.), 0)))
rxDataStep(inData = dat,
           outFile = stage_table,
           overwrite = TRUE) 
