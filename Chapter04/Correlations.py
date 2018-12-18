# Imports needed
import numpy as np
import pandas as pd
import pyodbc
import matplotlib.pyplot as plt
import seaborn as sns
import scipy as sc
import scipy.stats as st
from scipy.stats import chi2_contingency
import statsmodels.api as sm
from statsmodels.formula.api import ols

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
    ["S","C","Q"], inplace=True)

# Calculating Pearson correlation
df = pd.DataFrame(data=data)
print(df.corr())

# Two-way frequency tables
survived_sex = pd.crosstab(index=data["Survived"],columns=data["Sex"])
survived_sex.index= ["died","survived"]
print(survived_sex)

# Calculating Chi Square Independence Test
chi2, p, dof, expected = chi2_contingency(pd.crosstab(data.Sex, data.Survived))
print('Statistics=%.3f, p=%.100f, dof=%.3f' % (chi2, p , dof))
alpha = 0.05
if p > alpha:
    print('Variables seems to be independent (fail to reject H0)')
else:
    print('Variables seems to be dependent (reject H0)')
print(expected)

# Calculating Spearman’s rank correlation
coef, p = st.spearmanr(data.Survived, data.Pclass)
print('Statistics=%.3f, p=%.30f' % (coef, p))
alpha = 0.05
if p > alpha:
    print('Variables seems to be independent (fail to reject H0)')
else:
    print('Variables seems to be dependent (reject H0)')

# Calculating kendall's correlation
coef, p = st.kendalltau(data.Survived, data.Pclass)
print('Statistics=%.3f, p=%.50f' % (coef, p))
alpha = 0.05
if p > alpha:
    print('Variables seems to be independent (fail to reject H0)')
else:
    print('Variables seems to be dependent (reject H0)')

# Calculating one-way ANOVA
mod = ols('Age ~ Sex', data=data).fit()
aov_table = sm.stats.anova_lm(mod, typ=2)
print(aov_table)
