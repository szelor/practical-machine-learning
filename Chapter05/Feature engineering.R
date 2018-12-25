library(caret)
data <- iris
attach(data)

ggplot(data, aes(Petal.Length, Petal.Width)) + 
  geom_point(aes(shape = Species), size = 3) +
  geom_vline(xintercept = 2.45, size = 2) +
  geom_hline(yintercept = 1.75, size = 2)

model <- rpart(formula = Species ~., data = data)
caret::varImp(model)
model

data$Petal <- Petal.Length*Petal.Width
data$Sepal <- Sepal.Length*Sepal.Length
data$Difference <- data$Sepal-data$Petal
data$IsLonger <- Petal.Length>Sepal.Width

model <- rpart(formula = Species ~., data = data)
caret::varImp(model)

