USE ML;
GO

TRUNCATE TABLE [BenfordFraud].FraudulentVendors
TRUNCATE TABLE [BenfordFraud].FraudulentVendorsPlots
GO

SELECT *
FROM [BenfordFraud].[Invoices];
GO

SELECT *
FROM [BenfordFraud].[VendorInvoiceDigits] (421470);
GO

EXEC sp_helptext '[BenfordFraud].[VendorInvoiceDigits]'
GO

SELECT *
FROM [BenfordFraud].[VendorInvoiceDigits] (default)
ORDER BY VendorNumber;
GO

EXEC sp_helptext '[BenfordFraud].[getPotentialFraudulentVendors]'
GO
-- Get the fraudulent vendors:
INSERT INTO [BenfordFraud].[FraudulentVendors]
EXEC [BenfordFraud].getPotentialFraudulentVendors 0.10;
GO

SELECT *
FROM [BenfordFraud].[FraudulentVendors]
ORDER BY Pvalue;
GO
-- Generate plot for a specific fraudulent vendor:
EXEC sp_helptext '[BenfordFraud].[getVendorInvoiceDigits]'
GO

EXEC [BenfordFraud].getVendorInvoiceDigits '105436';
GO

-- Generate plots for the fraudulent vendors:
EXEC [BenfordFraud].getVendorInvoiceDigitsPlots 0.10;
GO

-- Get the vendor / plots:
SELECT *
FROM [BenfordFraud].FraudulentVendorsPlots;
GO

EXEC [BenfordFraud].[getPotentialFraudulentVendorsList]	 0.10;
GO