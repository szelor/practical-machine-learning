# Imports needed
import numpy as np
import pandas as pd
import pyodbc
import matplotlib.pyplot as plt
import seaborn as sns
import scipy as sc
import scipy.stats as st
import pylab
import statsmodels.api as sm

# Connecting and reading the data
con = pyodbc.connect(r'Driver={SQL Server};Server=MS;Database=ML;Trusted_Connection=yes;')
query = """SELECT * FROM [Titanic].[Train];"""
data = pd.read_sql(query, con)

# Define variables as categorical
data['Survived'] = data['Survived'].astype('category')
data['Pclass'] = data['Pclass'].astype('category')
data['Name'] = data['Name'].astype('category')
data['Sex'] = data['Sex'].astype('category')
data['Ticket'] = data['Ticket'].astype('category')
data['Cabin'] = data['Cabin'].astype('category')
data['Embarked'] = data['Embarked'].astype('category')

# Reordering Embarked
data['Embarked'].cat.reorder_categories(
    ["S", "C", "Q"], inplace=True)
print(data['Embarked'].value_counts(sort=False))

# Descriptive statistics

# Centers
print(data.Age.mean())
print(data.Age.mode())
print(data.Age.median())
print(data.Embarked.mode())

# Spread
print(data.Age.min())
print(data.Age.max())
print(data.Age.max() - data.Age.min())

print(data.Age.quantile(0.25))
print(data.Age.quantile(0.75))
print(data.Age.quantile(0.75) - data.Age.quantile(0.25))

print(data.Age.var())
print(data.Age.std())
print(data.Age.std() / data.Age.mean() * 100)

# Skewness and kurtosis
print(data.Age.skew())
print(data.Age.kurt())

# Descriptive statistics - summary
print(data.Age.describe())

# Calculates the z score of each value
print(st.zscore([0.45, 23, 25, 28, 33, 60, 80]))

#  z score of a p-value and vice versa
print(st.norm.cdf(3.46))
print(st.norm.ppf(.95))
print(st.norm.cdf(1.64))

# Normality test
sm.qqplot(data.Age, line='45')
pylab.show()

data_no_missing = data.dropna()
stat, p = st.shapiro(data_no_missing.Age)
print('Statistics=%.3f, p=%.3f' % (stat, p))
alpha = 0.05
if p > alpha:
    print('Sample looks Gaussian (fail to reject H0)')
else:
    print('Sample does not look Gaussian (reject H0)')

# Embarked crosstab
print(pd.crosstab(index=data["Embarked"], columns="Count"))

# Embarked barchart
sns.countplot(x="Embarked", data=data);
plt.show()

# Count of missing values
print(data.Age.isnull().sum())

# Calculating the entropy
# Function that calculates the entropy
def f_entropy(indata):
    indataprob = indata.value_counts() / len(indata)
    entropy = sc.stats.entropy(indataprob, base=2)
    return entropy


# Use the function on variables
print(np.log2(2), f_entropy(data.Survived), f_entropy(data.Survived) / np.log2(2))
print(np.log2(3), f_entropy(data.Embarked), f_entropy(data.Embarked) / np.log2(3))
print(np.log2(891), f_entropy(data.Name), f_entropy(data.Name) / np.log2(891))
