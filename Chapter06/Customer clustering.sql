USE ML
GO

TRUNCATE TABLE OnlineSales.[py_customer_clusters]
GO

SELECT *
FROM [OnlineSales].[RFM];
GO

EXEC sp_helptext 'OnlineSales.RFM';
GO

EXEC sp_helptext '[OnlineSales].[CustomerSegmentation]';
GO

--Execute the clustering and insert results into table
INSERT INTO OnlineSales.[py_customer_clusters]
EXEC [OnlineSales].[CustomerSegmentation];
GO

-- Select contents of the table									
SELECT * FROM OnlineSales.[py_customer_clusters];
GO

--Get email addresses of customers in cluster 3 for a promotion campaign
SELECT c.[c_email_address], c.c_first_name, c.c_salutation
FROM OnlineSales.customer AS c
JOIN OnlineSales.[py_customer_clusters] AS cl
ON cl.Customer = c.c_customer_sk
WHERE cl.cluster = 3
ORDER BY NEWID();
GO

--Get details about anusual customers in cluster 2 for review
SELECT c.c_first_name, c.c_last_name,  c.c_preferred_cust_flag
FROM OnlineSales.customer AS c
JOIN OnlineSales.[py_customer_clusters] AS cl
ON cl.Customer = c.c_customer_sk
WHERE cl.cluster = 2;
GO