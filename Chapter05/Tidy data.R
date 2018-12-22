install.packages("tidyverse")
library(tidyverse)

######################
# Sample data
######################

# Data set one
table1
table1$cases / table1$population * 10000

# Data set two
table2
case_rows <- c(1, 3, 5, 7, 9, 11, 13, 15, 17)
pop_rows <- c(2, 4, 6, 8, 10, 12, 14, 16, 18)
table2$count[case_rows] / table2$count[pop_rows] * 10000
# Spread
spread(table2, type, count)

# Data set three
table3
# Separate
separate(table3, rate, into = c("cases", "population"))

# Data set five
table5
# Unite
unite(table5, "year", century, year, sep = "")

# Data set four
table4a  # cases
cases <- c(table4a$`1999`, table4a$`2000`)
cases
gather(table4a, "year", "cases", 2:3)

table4b  # population
population <- c(table4b$`1999`, table4b$`2000`)
cases / population * 10000

# Gather & join
tidy4a <- gather(table4a, "year", "cases", -1)
tidy4b <- gather(table4b, "year", "population", -1)
left_join(tidy4a, tidy4b)
