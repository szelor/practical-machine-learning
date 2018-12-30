import pandas
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC, LinearSVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn import model_selection


train_df = pandas.read_csv('train.csv')
test_df = pandas.read_csv('test.csv')

#dropping features
train_df = train_df.drop(['Ticket', 'Cabin', 'PassengerId'], axis=1)
test_df = test_df.drop(['Ticket', 'Cabin'], axis=1)


combine = [train_df, test_df]

#Creating new feature
for dataset in combine:
    dataset['Title'] = dataset.Name.str.extract(' ([A-Za-z]+)\.', expand=False)

for dataset in combine:
    dataset['Title'] = dataset['Title'].replace(['Lady', 'Countess','Capt', 'Col','Don', 'Dr', 'Major', 'Rev', 'Sir', 'Jonkheer', 'Dona'], 'Other')

    dataset['Title'] = dataset['Title'].replace('Mlle', 'Miss')
    dataset['Title'] = dataset['Title'].replace('Ms', 'Miss')
    dataset['Title'] = dataset['Title'].replace('Mme', 'Mrs')

title_mapping = {"Mr": 1, "Miss": 2, "Mrs": 3, "Master": 4, "Rare": 5}
for dataset in combine:
    dataset['Title'] = dataset['Title'].map(title_mapping)
    dataset['Title'] = dataset['Title'].fillna(0).astype(int)

train_df = train_df.drop(['Name'], axis=1)
test_df = test_df.drop(['Name'], axis=1)
combine = [train_df, test_df]

#Create new feature combining existing features
for dataset in combine:
    dataset['FamilySize'] = dataset['SibSp'] + dataset['Parch'] + 1

for dataset in combine:
    dataset['IsAlone'] = 0
    dataset.loc[dataset['FamilySize'] == 1, 'IsAlone'] = 1

train_df = train_df.drop(['Parch', 'SibSp', 'FamilySize'], axis=1)
test_df = test_df.drop(['Parch', 'SibSp', 'FamilySize'], axis=1)
combine = [train_df, test_df]

#Converting categorical features
for dataset in combine:
    dataset['Sex'] = dataset['Sex'].map( {'female': 1, 'male': 0} ).astype(int)

#Completing and converting features

freq_port = train_df.Embarked.dropna().mode()[0]
for dataset in combine:
    dataset['Embarked'] = dataset['Embarked'].fillna(freq_port)

for dataset in combine:
    dataset['Embarked'] = dataset['Embarked'].map( {'S': 0, 'C': 1, 'Q': 2} ).astype(int)

guess_ages = np.zeros((2,3))
for dataset in combine:
    for i in range(0, 2):
        for j in range(0, 3):
            guess_df = dataset[(dataset['Sex'] == i) & \
                               (dataset['Pclass'] == j + 1)]['Age'].dropna()

            age_guess = guess_df.median()

            # Convert random age float to nearest .5 age
            guess_ages[i, j] = int(age_guess / 0.5 + 0.5) * 0.5

    for i in range(0, 2):
        for j in range(0, 3):
            dataset.loc[(dataset.Age.isnull()) & (dataset.Sex == i) & (dataset.Pclass == j + 1),
                        'Age'] = guess_ages[i, j]

    dataset['Age'] = dataset['Age'].astype(int)

#Completing and binning
test_df['Fare'].fillna(test_df['Fare'].dropna().median(), inplace=True)
for dataset in combine:
    dataset.loc[ dataset['Fare'] <= 7.91, 'Fare'] = 0
    dataset.loc[(dataset['Fare'] > 7.91) & (dataset['Fare'] <= 14.454), 'Fare'] = 1
    dataset.loc[(dataset['Fare'] > 14.454) & (dataset['Fare'] <= 31), 'Fare']   = 2
    dataset.loc[ dataset['Fare'] > 31, 'Fare'] = 3
    dataset['Fare'] = dataset['Fare'].astype(int)

print(train_df.head(10))

#Modelling

X = train_df.drop("Survived", axis=1)
Y = train_df["Survived"]
X_test = test_df.drop("PassengerId", axis=1).copy()

X_train, X_validation, Y_train, Y_validation = train_test_split(X, Y, test_size=0.3, random_state=123)

#print(X_train)

#Logistic Regression
logreg = LogisticRegression()
logreg.fit(X_train, Y_train)

acc_log = round(logreg.score(X_train, Y_train) * 100, 2)
print("LogisticRegression train score: %2.2f" % acc_log)

Y_pred = logreg.predict(X_validation)
acc_log = round(logreg.score(X_validation, Y_validation) * 100, 2)
print("LogisticRegression test score: %2.2f" % acc_log)


# Support Vector Machines
svc = SVC()
svc.fit(X_train, Y_train)

acc_svc = round(svc.score(X_train, Y_train) * 100, 2)
print("Support Vector Machines train score: %2.2f" % acc_svc)

Y_pred = svc.predict(X_validation)
acc_svc = round(svc.score(X_validation, Y_validation) * 100, 2)
print("Support Vector Machines test score: %2.2f" % acc_svc)


#k-Nearest Neighbors
knn = KNeighborsClassifier(n_neighbors = 3)
knn.fit(X_train, Y_train)

acc_knn = round(knn.score(X_train, Y_train) * 100, 2)
print("k-Nearest Neighbors train score: %2.2f" % acc_knn)

Y_pred = knn.predict(X_validation)
acc_knn = round(knn.score(X_validation, Y_validation) * 100, 2)
print("k-Nearest Neighbors test score: %2.2f" % acc_knn)

# Decision Tree
decision_tree = DecisionTreeClassifier(criterion="gini")
decision_tree.fit(X_train, Y_train)

acc_decision_tree = round(decision_tree.score(X_train, Y_train) * 100, 2)
print("Decision Tree train score: %2.2f" % acc_decision_tree)

Y_pred = decision_tree.predict(X_validation)
acc_decision_tree = round(decision_tree.score(X_validation, Y_validation) * 100, 2)
print("Decision Tree test score: %2.2f" % acc_decision_tree)

# Random Forest
random_forest = RandomForestClassifier()
random_forest.fit(X_train, Y_train)

acc_random_forest = round(random_forest.score(X_train, Y_train) * 100, 2)
print("Random Forest train score: %2.2f" % acc_random_forest)

Y_pred = random_forest.predict(X_validation)
random_forest.score(X_train, Y_train)
acc_random_forest = round(random_forest.score(X_validation, Y_validation) * 100, 2)
print("Random Forest test score: %2.2f" % acc_random_forest)


#dropping features
train_df = train_df.drop(['Embarked'], axis=1)
test_df = test_df.drop(['Embarked'], axis=1)

decision_treeE = DecisionTreeClassifier(criterion="gini")
decision_treeE.fit(X_train, Y_train)

acc_decision_treeE = round(decision_treeE.score(X_train, Y_train) * 100, 2)
print("Decision Tree without Embarked train score: %2.2f" % acc_decision_treeE)

Y_pred = decision_treeE.predict(X_validation)
acc_decision_treeE = round(decision_treeE.score(X_validation, Y_validation) * 100, 2)
print("Decision Tree without Embarked test score: %2.2f" % acc_decision_tree)

# Smaller Decision Tree
decision_tree = DecisionTreeClassifier(criterion="gini", max_depth=8, min_samples_split=4)
decision_tree.fit(X_train, Y_train)

acc_decision_tree = round(decision_tree.score(X_train, Y_train) * 100, 2)
print("Smaller Decision Tree train score: %2.2f" % acc_decision_tree)

Y_pred = decision_tree.predict(X_validation)
acc_decision_tree = round(decision_tree.score(X_validation, Y_validation) * 100, 2)
print("Smaller Decision Tree test score: %2.2f" % acc_decision_tree)

#K-Folds Cross Validation
kfold = model_selection.KFold(n_splits=10, random_state=12)
cv_decision_tree = DecisionTreeClassifier(criterion="gini", max_depth=8, min_samples_split=4)

results = model_selection.cross_val_score(cv_decision_tree, X, Y, cv=kfold)
print("Decision Tree accuracy: Final mean:%.3f%%, Final standard deviation:(%.3f%%)" % (results.mean()*100.0, results.std()*100.0))
print('Decision Tree accuracies from each of the 10 folds using kfold:',results)

#Predictions
cv_decision_tree.fit(X,Y)
Y_test = cv_decision_tree.predict(X_test)
print(Y_test)