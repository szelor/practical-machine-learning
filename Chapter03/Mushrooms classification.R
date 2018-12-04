library(dplyr)
library(ggplot2)
library(Kmisc)
library(gridExtra)

url_file <-  "http://archive.ics.uci.edu/ml/machine-learning-databases/mushroom/agaricus-lepiota.data"
mushrooms <- read.csv(url(url_file), header=FALSE)

dim(mushrooms)
str(mushrooms)

fields <- c("class",
            "cap_shape",
            "cap_surface",
            "cap_color",
            "bruises",
            "odor",
            "gill_attachment",
            "gill_spacing",
            "gill_size",
            "gill_color",
            "stalk_shape",
            "stalk_root",
            "stalk_surface_above_ring",
            "stalk_surface_below_ring",
            "stalk_color_above_ring",
            "stalk_color_below_ring",
            "veil_type",
            "veil_color",
            "ring_number",
            "ring_type",
            "spore_print_color",
            "population",
            "habitat")
colnames(mushrooms) <- fields

levels(mushrooms$class) <- c("edible", "poisonous")
levels(mushrooms$cap_shape) <- c("bell", "conical", "flat", "knobbed", "sunken", "convex")
levels(mushrooms$cap_color) <- c("buff", "cinnamon", "red", "gray", "brown", "pink", 
                                "green", "purple", "white", "yellow")
levels(mushrooms$cap_surface) <- c("fibrous", "grooves", "scaly", "smooth")
levels(mushrooms$bruises) <- c("no", "yes")
levels(mushrooms$odor) <- c("almond", "creosote", "foul", "anise", "musty", "none", "pungent", "spicy", "fishy")
levels(mushrooms$gill_attachment) <- c("attached", "free")
levels(mushrooms$gill_spacing) <- c("close", "crowded")
levels(mushrooms$gill_size) <- c("broad", "narrow")
levels(mushrooms$gill_color) <- c("buff", "red", "gray", "chocolate", "black", "brown", "orange", 
                                 "pink", "green", "purple", "white", "yellow")
levels(mushrooms$stalk_shape) <- c("enlarging", "tapering")
levels(mushrooms$stalk_root) <- c("missing", "bulbous", "club", "equal", "rooted")
levels(mushrooms$stalk_surface_above_ring) <- c("fibrous", "silky", "smooth", "scaly")
levels(mushrooms$stalk_surface_below_ring) <- c("fibrous", "silky", "smooth", "scaly")
levels(mushrooms$stalk_color_above_ring) <- c("buff", "cinnamon", "red", "gray", "brown", "pink", 
                                             "green", "purple", "white", "yellow")
levels(mushrooms$stalk_color_below_ring) <- c("buff", "cinnamon", "red", "gray", "brown", "pink", 
                                             "green", "purple", "white", "yellow")
levels(mushrooms$veil_type) <- "partial"
levels(mushrooms$veil_color) <- c("brown", "orange", "white", "yellow")
levels(mushrooms$ring_number) <- c("none", "one", "two")
levels(mushrooms$ring_type) <- c("evanescent", "flaring", "large", "none", "pendant")
levels(mushrooms$spore_print_color) <- c("buff", "chocolate", "black", "brown", "orange", 
                                        "green", "purple", "white", "yellow")
levels(mushrooms$population) <- c("abundant", "clustered", "numerous", "scattered", "several", "solitary")
levels(mushrooms$habitat) <- c("wood", "grasses", "leaves", "meadows", "paths", "urban", "waste")

str(mushrooms)
head(mushrooms)

sum(complete.cases(mushrooms))

#Contingence tables
class <- plyr::count(mushrooms$class)
print(sprintf("Edible: %d | Poisonous: %d | Percent of poisonous classes: %.1f%%",class$freq[1],class$freq[2], round(class$freq[1]/nrow(mushrooms)*100,1)))

mush_features <- colnames(mushrooms)[-1]
table_res <- lapply(mush_features, function(x) {table(mushrooms$class, mushrooms[,x])})
names(table_res) <- mush_features
table_res
table_res$ring_type
table_res$odor

#check for the significative relationship between single mushrooms features and their classification as edible or poisonous

ggplot(mushrooms, aes(x = class, y = odor)) + 
  geom_jitter(alpha = 0.5)

#convert factors to numeric
features = mushrooms[,2:23]
label = mushrooms[,1]
features <- sapply(features, function (x) as.numeric(as.factor(x)))

scales <- list(x=list(relation="free"),y=list(relation="free"), cex=0.6)
featurePlot(x=features, y=label, plot="density",scales=scales,
            layout = c(4,6), auto.key = list(columns = 2), pch = "|")

#check for the significative relationship between  mushrooms features and their classification as edible or poisonous

ggplot(mushrooms, aes(x = cap_surface, y = ring_type, col = class, shape = class)) + 
  geom_jitter(alpha = 0.5) + 
  scale_color_manual(breaks = c("edible", "poisonous"), 
                     values = c("green", "red"))

#Chi-Square Test of Independence

chisq_test_res = list()
relevant_features = c()

for (i in 2:length(colnames(mushrooms))) {
  if (nlevels(mushrooms[,i]) > 1) {
    fname = colnames(mushrooms)[i]
    res = chisq.test(mushrooms[,i], mushrooms[,"class"], simulate.p.value = TRUE)
    res$data.name = paste(fname, "class", sep= " and ")
    chisq_test_res[[fname]] = res
    relevant_features = c(relevant_features, fname)
  }
}

chisq_test_res
chisq_test_res$odor
chisq_test_res$odor$observed
chisq_test_res$odor$expected
setdiff(mush_features, relevant_features)

#entropy
install.packages("entropy")
library(entropy)

flips <- c("Head", "Head", "Tail", "Tail", "Head", "Tail", "Head", "Head", "Head", "Tail")
freqs <- table(flips)/length(flips)
entropy(freqs, unit="log2")

flips <- rep("Head", 10)
freqs <- table(flips)/length(flips)
entropy(freqs, unit="log2")

entropy(table_res$odor, unit="log2")
entropy(table_res$veil_type,unit="log2")

#Classification model
library(caret)
library(rpart.plot)

set.seed(1023)
train_idx <- caret::createDataPartition(y = mushrooms$class, p=0.7, list=FALSE)

train_mushrooms <- mushrooms[train_idx, ]
test_mushrooms <- mushrooms[-train_idx, ]

round(prop.table(table(mushrooms$class)), 2)
round(prop.table(table(train_mushrooms$class)), 2)
round(prop.table(table(test_mushrooms$class)), 2)

formula <- paste(relevant_features, collapse = "+")
formula <- as.formula(paste("class ~ ", formula))
formula

model_tree <- rpart(formula = formula, 
                    data = train_mushrooms, 
                    method = "class",
                    cp = 0.00005)
model_tree

rpart.plot(model_tree, extra = 104, box.palette = "GnBn", 
           branch.lty = 2, shadow.col = "gray", nn = TRUE)

caret::varImp(model_tree)

predict(model_tree)
cm <- caret::confusionMatrix(data=predict(model_tree, type = "class"), 
                             reference = train_mushrooms$class)
cm$table

cm <- caret::confusionMatrix(data = 
                       predict(model_tree, newdata = test_mushrooms, type = "class"), 
                       reference = test_mushrooms$class)
cm$table
