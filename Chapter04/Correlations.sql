USE ML
GO

-- Linear dependencies: Continuous variables
WITH CoVarCTE AS
(
SELECT 1.0*Age as val1,
 AVG(1.0*Age) OVER () AS mean1,
 1.0*SibSp AS val2,
 AVG(1.0*SibSp) OVER() AS mean2
FROM [Titanic].[Train]
)
SELECT 
 SUM((val1-mean1)*(val2-mean2)) / COUNT(*) AS Covar,
 (SUM((val1-mean1)*(val2-mean2)) / COUNT(*)) /
 (STDEVP(val1) * STDEVP(val2)) AS [r-Pearson]
FROM CoVarCTE;
GO

-- Linear dependencies: Discrete variables
WITH
ObservedCombination_CTE AS
(
SELECT [Survived] AS OnRows,
   [Sex] OnCols, 
 COUNT(*) AS ObservedCombination
FROM [Titanic].[Train]
GROUP BY [Survived], [Sex]
),
ExpectedCombination_CTE AS
(
SELECT OnRows, OnCols, ObservedCombination
 ,SUM(ObservedCombination) OVER (PARTITION BY OnRows) AS ObservedOnRows
 ,SUM(ObservedCombination) OVER (PARTITION BY OnCols) AS ObservedOnCols
 ,SUM(ObservedCombination) OVER () AS ObservedTotal
 ,CAST(ROUND(SUM(1.0 * ObservedCombination) OVER (PARTITION BY OnRows)
  * SUM(1.0 * ObservedCombination) OVER (PARTITION BY OnCols) 
  / SUM(1.0 * ObservedCombination) OVER (), 0) AS INT) AS ExpectedCombination
FROM ObservedCombination_CTE
)
SELECT * FROM ExpectedCombination_CTE;


WITH
ObservedCombination_CTE AS
(
SELECT [Survived] AS OnRows,
   [Sex] OnCols, 
 COUNT(*) AS ObservedCombination
FROM [Titanic].[Train]
GROUP BY [Survived], [Sex]
),
ExpectedCombination_CTE AS
(
SELECT OnRows, OnCols, ObservedCombination
 ,SUM(ObservedCombination) OVER (PARTITION BY OnRows) AS ObservedOnRows
 ,SUM(ObservedCombination) OVER (PARTITION BY OnCols) AS ObservedOnCols
 ,SUM(ObservedCombination) OVER () AS ObservedTotal
 ,CAST(ROUND(SUM(1.0 * ObservedCombination) OVER (PARTITION BY OnRows)
  * SUM(1.0 * ObservedCombination) OVER (PARTITION BY OnCols) 
  / SUM(1.0 * ObservedCombination) OVER (), 0) AS INT) AS ExpectedCombination
FROM ObservedCombination_CTE
)
SELECT (COUNT(DISTINCT OnRows) - 1) * (COUNT(DISTINCT OnCols) - 1) AS DegreesOfFreedom,
	SUM(SQUARE(ObservedCombination - ExpectedCombination) / ExpectedCombination) AS ChiSquared
FROM ExpectedCombination_CTE;
GO

--Linear dependencies: Discrete and Continuous variables
WITH Anova_CTE AS
(
SELECT Sex, Age,
 COUNT(*) OVER (PARTITION BY Sex) AS gr_CasesCount,
 DENSE_RANK() OVER (ORDER BY Sex) AS gr_DenseRank,
 SQUARE(AVG(Age) OVER (PARTITION BY Sex) -
        AVG(Age) OVER ()) AS between_gr_SS,
 SQUARE(Age - 
        AVG(Age) OVER (PARTITION BY Sex)) 
		AS within_gr_SS
FROM [Titanic].[Train]
) 
SELECT N'Between groups' AS [Source of Variation],
 SUM(between_gr_SS) AS SS,
 (MAX(gr_DenseRank) - 1) AS df,
 SUM(between_gr_SS) / (MAX(gr_DenseRank) - 1) AS MS,
 (SUM(between_gr_SS) / (MAX(gr_DenseRank) - 1)) /
 (SUM(within_gr_SS) / (COUNT(*) - MAX(gr_DenseRank))) AS F
FROM Anova_CTE
UNION 
SELECT N'Within groups' AS [Source of Variation],
 SUM(within_gr_SS) AS SS,
 (COUNT(*) - MAX(gr_DenseRank)) AS df,
 SUM(within_gr_SS) / (COUNT(*) - MAX(gr_DenseRank)) AS MS,
 NULL AS F
FROM Anova_CTE;