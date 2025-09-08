CREATE DATABASE if not exists RetailAnalysis;

-- Use the 'RetailAnalysis' database for all subsequent SQL commands.
USE RetailAnalysis;

## Table Creation

-- Create the 'customer_profiles' table.
-- This table stores demographic and essential information for each customer.
CREATE TABLE if not exists customer_profiles (
    `CustomerID`    DECIMAL(38, 0) NOT NULL, 	-- Unique numerical identifier for each customer.
    `Age`           DECIMAL(38, 0) NOT NULL, 	-- Age of the customer, initially stored as a decimal.
    `Gender`        VARCHAR(6) NOT NULL,     	-- Gender of the customer (e.g., 'Male', 'Female').
    `Location`      VARCHAR(5),              	-- Abbreviated location code for the customer.
    `JoinDate`      VARCHAR(8) NOT NULL      	-- Date when the customer registered, stored as YYYYMMDD string.
);
CREATE TABLE if not exists product_inventory (
    `ProductID`     DECIMAL(38, 0) NOT NULL, 	-- Unique numerical identifier for each product.
    `ProductName`   VARCHAR(11) NOT NULL,    	-- Name of the product.
    `Category`      VARCHAR(15) NOT NULL,    	-- Product category (e.g., 'Electronics', 'Apparel').
    `StockLevel`    DECIMAL(38, 0) NOT NULL, 	-- Current quantity of the product available in stock.
    `Price`         DECIMAL(38, 2) NOT NULL  	-- Selling price of the product.
);


-- Create the 'sales_transaction' table.
-- This table records every individual sales event, linking customers to products.
CREATE TABLE IF NOT EXISTS sales_transaction (
    `TransactionID`         DECIMAL(38, 0) NOT NULL, 	-- Unique numerical identifier for each sales transaction.
    `CustomerID`            DECIMAL(38, 0) NOT NULL, 	-- Identifier of the customer who made the purchase.
    `ProductID`             DECIMAL(38, 0) NOT NULL, 	-- Identifier of the product involved in the transaction.
    `QuantityPurchased`     DECIMAL(38, 0) NOT NULL, 	-- Number of units of the product bought in this transaction.
    `TransactionDate`       VARCHAR(8) NOT NULL,     	-- Date of the transaction, stored as YYYYMMDD string.
    `Price`                 DECIMAL(38, 2) NOT NULL  	-- Final price at which the product was sold in this transaction.
);
LOAD DATA LOCAL INFILE "customer_profiles_data.csv"
INTO TABLE customer_profiles
FIELDS TERMINATED BY ','      	-- Specifies that fields in the CSV are separated by commas.
ENCLOSED BY '"'              	-- Specifies that fields are optionally enclosed by double quotes.
LINES TERMINATED BY '\n'     	-- Specifies that lines in the CSV are terminated by a newline character.
IGNORE 1 ROWS;               	-- Skips the first row of the CSV file, assuming it's a header.

-- Load data into the 'product_inventory' table from its corresponding CSV file.
LOAD DATA LOCAL INFILE "product_inventory_data.csv"
INTO TABLE product_inventory
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load data into the 'sales_transaction' table from its corresponding CSV file.
LOAD DATA LOCAL INFILE "sales_transaction_data.csv"
INTO TABLE sales_transaction
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from sales_transaction;
ALTER TABLE customer_profiles
MODIFY COLUMN CustomerID INT NOT NULL COMMENT 'Unique identifier for each customer, optimized for integer storage.';

-- Modifying `Age` column:
-- Converting `Age` from DECIMAL to INT, as age is universally represented in whole numbers.
ALTER TABLE customer_profiles
MODIFY COLUMN Age INT NOT NULL COMMENT 'Age of the customer, stored as a whole number of years.';

-- Modifying `JoinDate` column:
-- Converting `JoinDate` from VARCHAR to DATE, enabling proper date-time functions and chronological sorting.
ALTER TABLE customer_profiles
MODIFY COLUMN JoinDate DATE NOT NULL COMMENT 'Date when the customer joined the platform, formatted as DD/MM/YYYY.';

-- Verifying the updated schema for `customer_profiles`.
-- This command displays the refined structure of the table, confirming the changes.
DESC customer_profiles;
SELECT
    COUNT(*) AS Number_Of_Float_StockLevel_Columns 	-- Alias for clarity on the count result.
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = 'retailanalysis' AND 			-- Filters results to the 'retailanalysis' database.
    COLUMN_NAME = 'StockLevel' AND     				-- Targets the 'StockLevel' column specifically.
    DATA_TYPE = 'float'; 
    
ALTER TABLE product_inventory
MODIFY COLUMN ProductID INT NOT NULL COMMENT 'Unique identifier for each product, optimized for integer storage.';

alter table product_inventory
modify column StockLevel int not null comment 'current quantity of product as stock stored in whole number';
desc product_inventory;
select count(*) as noofquantitypurchase
from INFORMATION_SCHEMA.Columns where table_schema='retailanalysis' and column_name='QuantityPurchased' and Data_type='float';
alter table sales_transaction
modify column TransactionID int not null comment 'Product stored as integer';
alter table sales_transaction
modify column CustomerID int not null comment 'C ustomer stored as integer';
alter table sales_transaction
modify column ProductID int not null comment 'ProductID stored as integer';
alter table sales_transaction
-- Modifying `QuantityPurchased` column:
-- Converting `QuantityPurchased` from DECIMAL to INT, assuming products are sold in whole units.
modify column QuantityPurchased int not null comment 'Quantity stored as integer';
set sql_safe_updates=0;
SELECT DISTINCT TransactionDate 
FROM sales_transaction 
LIMIT 40;
SET SQL_SAFE_UPDATES = 0;

UPDATE sales_transaction 
SET TransactionDate = CASE
    WHEN TransactionDate LIKE '%/%/%' AND LENGTH(TransactionDate) = 8
        THEN STR_TO_DATE(TransactionDate, '%d/%m/%y')
    WHEN TransactionDate LIKE '%/%/%' AND LENGTH(TransactionDate) IN (9,10)
        THEN STR_TO_DATE(TransactionDate, '%m/%d/%Y')
    ELSE NULL 
END
WHERE TransactionID IS NOT NULL;
desc sales_transaction;
### Checking for Explicit NULL Values

-- The `COUNT(*) - COUNT(column_name)` method explicitly identifies NULL values.
-- `COUNT(column_name)` counts only non-NULL values in that column, while `COUNT(*)` counts all rows.
-- The difference between these two counts reveals the exact number of NULLs in the specified column.

-- Target table: `customer_profiles` (Checking for NULLs across all columns)
SELECT
    COUNT(*) - COUNT(`CustomerID`) AS MissingCustomerIDCount, 	-- Counts NULLs in the CustomerID column.
    COUNT(*) - COUNT(`Age`) AS MissingAgeCount,               	-- Counts NULLs in the Age column.
    COUNT(*) - COUNT(`Gender`) AS MissingGenderCount,         	-- Counts NULLs in the Gender column.
    COUNT(*) - COUNT(`Location`) AS MissingLocationCount,     	-- Counts NULLs in the Location column.
    COUNT(*) - COUNT(`JoinDate`) AS MissingJoinDateCount,     	-- Counts NULLs in the JoinDate column.
    COUNT(*) AS TotalRowsInCustomerProfiles                   	-- Provides the total number of rows for context.
FROM
    customer_profiles;
#Your table was created with NOT NULL constraints, meaning:
#Database won't allow empty values
#Every field must have data
#So naturally, you get 0 missing values
#In Simple Terms: You're double-checking that your "no empty data allowed" rule is actually working! 🔍
   # Target table: `product_inventory` (Checking for NULLs across all columns)
SELECT
    COUNT(*) - COUNT(ProductID) AS NullProductIDCount,       	-- Counts NULLs in the ProductID column.
    COUNT(*) - COUNT(ProductName) AS NullProductNameCount,   	-- Counts NULLs in the ProductName column.
    COUNT(*) - COUNT(Category) AS NullCategoryCount,         	-- Counts NULLs in the Category column.
    COUNT(*) - COUNT(StockLevel) AS NullStockLevelCount,     	-- Counts NULLs in the StockLevel column.
    COUNT(*) - COUNT(Price) AS NullPriceCount,               	-- Counts NULLs in the Price column.
    COUNT(*) AS TotalRowsInProductInventory                  	-- Provides the total number of rows for context.
FROM
    product_inventory;
#Target table: `sales_transaction` (Checking for NULLs across all columns)
SELECT
    COUNT(*) - COUNT(TransactionID) AS NullTransactionIDCount,       	-- Counts NULLs in the TransactionID column.
    COUNT(*) - COUNT(CustomerID) AS NullCustomerIDCount,             	-- Counts NULLs in the CustomerID column.
    COUNT(*) - COUNT(ProductID) AS NullProductIDCount,               	-- Counts NULLs in the ProductID column.
    COUNT(*) - COUNT(QuantityPurchased) AS NullQuantityPurchasedCount, 	-- Counts NULLs in the QuantityPurchased column.
    COUNT(*) - COUNT(TransactionDate) AS NullTransactionDateCount,   	-- Counts NULLs in the TransactionDate column.
    COUNT(*) - COUNT(Price) AS NullPriceCount,                       	-- Counts NULLs in the Price column.
    COUNT(*) AS TotalRowsInSalesTransaction                          	-- Provides the total number of rows for context.
FROM
    sales_transaction;
    
    SELECT
    'Gender' AS ColumnName,        -- Labels the column being checked.
    COUNT(*) AS EmptyOrWhitespaceCount -- Counts occurrences of empty or whitespace-only strings.
FROM
    customer_profiles
WHERE
    TRIM(Gender) = '';   -- Filters for rows where Gender is an empty string after removing leading/trailing spaces.
    
    SELECT
    'Location' AS ColumnName,      -- Labels the column being checked.
    COUNT(*) AS EmptyOrWhitespaceCount -- Counts occurrences of empty or whitespace-only strings.
FROM
    customer_profiles
WHERE
    TRIM(Location) = '';   
    -- Filters for rows where Location is an empty string after trimming.
-- Observation: This query previously identified rows where 'Location' was an empty string (e.g., 13 rows).
-- Action (Previously taken): Rows with empty 'Location' values were deleted in a prior run, as 13 rows out of ~1000 was a small proportion.
-- DELETE FROM customer_profiles WHERE Location = ''; -- This statement was executed in a previous step.

    SELECT
    'ProductName' AS ColumnName,
    COUNT(*) AS EmptyOrWhitespaceCount
FROM product_inventory
WHERE TRIM(ProductName) = '';

    SELECT
    'Category' AS ColumnName,
    COUNT(*) AS EmptyOrWhitespaceCount
FROM product_inventory
WHERE TRIM(Category) = '';

SELECT CustomerID, COUNT(*) AS DuplicateCount -- Counts how many times each CustomerID appears.
FROM customer_profiles
GROUP BY CustomerID           -- Groups rows by identical CustomerID values.
HAVING COUNT(*) > 1;  

SELECT
    CustomerID,          -- Selects the CustomerID value.
    COUNT(*) AS DuplicateCount -- Counts how many times each CustomerID appears.
FROM  customer_profiles
GROUP BY CustomerID           -- Groups rows by identical CustomerID values.
HAVING COUNT(*) > 1; 

SELECT Age, COUNT(*) AS DuplicateCount -- Counts occurrences of each unique Age value.
FROM customer_profiles
GROUP BY Age                  -- Groups rows by identical Age values.
HAVING COUNT(*) > 1; 
-- Insight: This is expected to show many results as age values are commonly repeated across different customers.
# It's a normal occurrence and doesn't indicate missing data.

-- Checking `Gender` column for duplicate Gender values.
-- `LOWER()` standardizes case (e.g., 'Male' vs 'male') for accurate grouping of categorical data.

SELECT LOWER(Gender) AS StandardizedGender, -- Standardizes gender to lowercase for consistent grouping.
    COUNT(*) AS DuplicateCount         -- Counts occurrences of each unique standardized gender.
FROM customer_profiles
GROUP BY LOWER(Gender)                      -- Groups rows by the standardized (lowercase) Gender value.
HAVING COUNT(*) > 1;    


SELECT LOWER(Location) AS StandardizedLocation,  COUNT(*) AS DuplicateCount            
FROM customer_profiles
GROUP BY LOWER(Location)                      
HAVING COUNT(*) > 1; 

SELECT JoinDate,            -- Selects the JoinDate value.
    COUNT(*) AS DuplicateCount -- Counts occurrences of each unique JoinDate.
FROM customer_profiles
GROUP BY JoinDate             
HAVING COUNT(*) > 1; 

SELECT ProductID, COUNT(*) AS DuplicateCount -- Counts how many times each ProductID appears.
FROM product_inventory
GROUP BY ProductID            -- Groups rows by identical ProductID values.
HAVING COUNT(*) > 1; 

SELECT
    LOWER(ProductName) AS StandardizedProductName, -- Standardizes product name to lowercase for consistent grouping.
    COUNT(*) AS DuplicateCount                   -- Counts occurrences of each unique standardized product name.
FROM product_inventory
GROUP BY LOWER(ProductName)                           -- Groups rows by the standardized (lowercase) ProductName value.
HAVING COUNT(*) > 1;

SELECT LOWER(Category) AS StandardizedCategory, COUNT(*) AS DuplicateCount             -- Counts occurrences of each unique standardized category.
FROM product_inventory
GROUP BY LOWER(Category)                        -- Groups rows by the standardized (lowercase) Category value.
HAVING COUNT(*) > 1;  

SELECT StockLevel,          -- Selects the StockLevel value.
    COUNT(*) AS DuplicateCount -- Counts occurrences of each unique StockLevel value.
FROM product_inventory
GROUP BY StockLevel           -- Groups rows by identical StockLevel values.
HAVING COUNT(*) > 1;
-- Filters to show stock levels that appear more than once.
-- Insight: Expected to show many results, as various products can have the same stock quantity. 
-- This is a normal observation.
   
   SELECT TransactionID,       -- Selects the TransactionID value.
COUNT(*) AS DuplicateCount -- Counts how many times each TransactionID appears.
FROM sales_transaction
GROUP BY TransactionID        -- Groups rows by identical TransactionID values.
HAVING COUNT(*) > 1; 


    SELECT
    TransactionID,       -- Selects the TransactionID value.
    COUNT(*) AS DuplicateCount -- Counts how many times each TransactionID appears.
FROM sales_transaction
GROUP BY TransactionID  
    HAVING COUNT(*) > 1;
    
    
    SELECT
    TransactionDate,     -- Selects the TransactionDate value.
    COUNT(*) AS DuplicateCount -- Counts occurrences of each unique TransactionDate.
FROM sales_transaction
GROUP BY TransactionDate      -- Groups rows by identical TransactionDate values.
HAVING COUNT(*) > 1;
desc customer_profiles;

#check for duplicates in customer profile

WITH DuplicateCustomers AS ( SELECT CustomerID,Age,Gender,Location, JoinDate,             
        ROW_NUMBER() OVER ( PARTITION BY Age, Gender, Location, JoinDate -- Groups rows with identical Age, Gender, Location, and JoinDate combinations.
            ORDER BY CustomerID                          -- Orders rows within each group by CustomerID (for consistent 'rn' assignment).
        ) AS rn                  -- Assigns the calculated row number an alias 'rn'.
    FROM customer_profiles        -- Specifies the table from which to retrieve data.
)
#-- Assign a row number to each row within partitions defined by the columns
        -- we consider for duplication. The ORDER BY clause ensures consistent numbering
        -- if there are multiple identical rows
        
#Select only those rows where 'rn' (row number) is greater than 1,
-- indicating they are duplicates based on our defined criteria.
SELECT *
FROM DuplicateCustomers
WHERE rn > 1;


WITH DuplicateProducts AS (
    SELECT
        ProductID,               -- Selects the ProductID.
        ProductName,             -- Selects the ProductName.
        Category,                -- Selects the Category.
        StockLevel,              -- Selects the StockLevel.
        -- Assign a row number based on the specified columns for duplicate detection.
        ROW_NUMBER() OVER (PARTITION BY ProductName, Category, StockLevel -- Groups products with identical names, categories, and stock levels.
            ORDER BY ProductID                           -- Orders products within each group by ProductID for consistent numbering.
        ) AS rn                  -- Assigns the calculated row number an alias 'rn'.
    FROM
        product_inventory        -- Specifies the table from which to retrieve data.
)
SELECT *
FROM DuplicateProducts
WHERE rn > 1;

 SET SQL_SAFE_UPDATES=0;
 DELETE s1 FROM sales_transaction s1, sales_transaction s2
WHERE s1.TransactionID = s2.TransactionID 
  AND s1.CustomerID = s2.CustomerID 
  AND s1.ProductID = s2.ProductID 
  AND s1.QuantityPurchased = s2.QuantityPurchased
  AND s1.TransactionID > s2.TransactionID;
 
 ALTER TABLE sales_transaction ADD COLUMN temp_id INT AUTO_INCREMENT PRIMARY KEY;
 DELETE st1
FROM sales_transaction st1
JOIN sales_transaction st2
  ON st1.TransactionID = st2.TransactionID
 AND st1.CustomerID    = st2.CustomerID
 AND st1.ProductID     = st2.ProductID
 AND st1.QuantityPurchased = st2.QuantityPurchased
 AND st1.temp_id > st2.temp_id;
 
 SELECT
    st.TransactionID,                -- Selects the unique identifier for each sales transaction.
    st.ProductID,                    -- Selects the product identifier from the sales transaction.
    pi.ProductName,                  -- Selects the product name from the product inventory.
    pi.Category,                     -- Selects the product category from the product inventory.
    st.QuantityPurchased,            -- Selects the quantity of the product purchased in the transaction.
    st.Price AS TransactionPricePerUnit, -- Selects the price recorded in the sales transaction, aliased for clarity.
    pi.Price AS InventoryPrice,      -- Selects the current price from the product inventory, aliased for clarity.
    (st.Price - pi.Price) AS PriceDifference -- Calculates the monetary difference between the transaction price and inventory price.
FROM
    sales_transaction AS st          -- Specifies the `sales_transaction` table, aliased as 'st' for brevity.
JOIN product_inventory AS pi          -- Joins with the `product_inventory` table, aliased as 'pi'.
    ON st.ProductID = pi.ProductID   -- Links transactions to products using their common ProductID.
WHERE
    st.Price <> pi.Price;
    
    
    
    UPDATE sales_transaction AS st       -- Specifies the `sales_transaction` table to be updated, aliased as 'st'.
SET st.Price = (                     -- Sets the 'Price' column in `sales_transaction`.
    SELECT pi.Price                  -- Subquery to select the current price from `product_inventory`.
    FROM product_inventory AS pi     -- Specifies the `product_inventory` table for the subquery, aliased as 'pi'.
    WHERE pi.ProductID = st.ProductID -- Links the subquery to the outer query using ProductID to find the correct product price.
)
WHERE EXISTS (                       -- Ensures that the update only happens if a discrepancy exists for the product.
    SELECT 1                         -- Checks for the existence of a matching record in `product_inventory` with a price mismatch.
    FROM product_inventory AS pi     -- Specifies `product_inventory` for the EXISTS subquery.
    WHERE pi.ProductID = st.ProductID -- Links `product_inventory` to `sales_transaction`.
      AND st.Price <> pi.Price       -- Only update where there's a discrepancy between the sales transaction price and inventory price.
);
#-- SQL Queries for Exploratory Data Analysis (EDA)

-- This script contains SQL queries to help you understand the structure, patterns,
-- and relationships within your customer, product, and sales data.

---

### 1. Summary Statistics and Univariate Analysis

-- This section focuses on understanding the distribution and basic statistics
-- of columns within each table individually. It's like taking a peek at the individual ingredients
-- before mixing them into a dish to ensure they're all in good shape.

#### A. `customer_profiles` Table

-- Query to get the total number of customers in the table.
-- This gives a fundamental understanding of the size of our customer base.


SELECT
    COUNT(CustomerID) AS TotalCustomers 
FROM
    customer_profiles; 
    SELECT
    MIN(Age) AS MinAge,               		-- Identifies the youngest customer's age recorded.
    MAX(Age) AS MaxAge,               		-- Identifies the oldest customer's age recorded.
    ROUND(AVG(Age), 2) AS AverageAge, 		-- Calculates the average age of all customers, rounded to two decimal places.
    ROUND(STDDEV(Age), 2) AS StdDevAge 		-- Measures the typical deviation of ages from the average, rounded to two decimal places.
FROM
    customer_profiles;
    SELECT
    Gender,                                                 -- Groups the results by the 'Gender' column.
    COUNT(CustomerID) AS NumberOfCustomers,                 -- Counts the number of customers for each gender.
    ROUND((COUNT(CustomerID) * 100.0 / (SELECT COUNT(*) FROM customer_profiles)), 2) AS Percentage 	-- Calculates the percentage of total customers for each gender.
FROM customer_profiles 
GROUP BY Gender
ORDER BY NumberOfCustomers DESC;
    
    SELECT
    Location,                                               -- Groups the results by the 'Location' column.
    COUNT(CustomerID) AS NumberOfCustomers,                 -- Counts the number of customers in each location.
    ROUND((COUNT(CustomerID) * 100.0 / (SELECT COUNT(*) FROM customer_profiles)), 2) AS Percentage -- Calculates the percentage of total customers residing in each location.
FROM
    customer_profiles
GROUP BY
    Location
ORDER BY
    NumberOfCustomers DESC;
    
    SELECT YEAR(JoinDate) AS JoinYear,                     -- Extracts the year from the 'JoinDate' column.
    COUNT(CustomerID) AS NumberOfCustomersJoined    -- Counts customers who joined in that specific year.
FROM customer_profiles 
GROUP BY JoinYear
ORDER BY JoinYear;

#Insight: 
#Analyzing **customer acquisition trends by year** helps track growth patterns 
 #       and can be correlated with historical marketing efforts, economic conditions, or major product launches. It identifies periods of high or low customer influx


SELECT COUNT(ProductID) AS TotalProducts -- Counts every ProductID to get the total count of unique products.
FROM product_inventory;

SELECT
    MIN(StockLevel) AS MinStockLevel,             		-- Identifies the lowest stock quantity for any product.
    MAX(StockLevel) AS MaxStockLevel,             		-- Identifies the highest stock quantity for any product.
    ROUND(AVG(StockLevel), 2) AS AverageStockLevel, 	-- Calculates the average stock level across all products, rounded to two decimal places.
    ROUND(STDDEV(StockLevel), 2) AS StdDevStockLevel 	-- Measures the typical deviation of stock levels from the average, rounded to two decimal places.
FROM product_inventory;

/*Insight:
		**Stock level statistics** are crucial for inventory management. 
        A very low minimum stock level might indicate potential stock-out risks, while the average 
        and standard deviation help assess overall inventory health and variability in stock quantities across products.*/


    SELECT
    MIN(Price) AS MinPrice,             	-- Identifies the lowest price for any product in inventory.
    MAX(Price) AS MaxPrice,             	-- Identifies the highest price for any product in inventory.
    ROUND(AVG(Price), 2) AS AveragePrice, 	-- Calculates the average price of all products, rounded to two decimal places.
    ROUND(STDDEV(Price), 2) AS StdDevPrice 	-- Measures the typical deviation of prices from the average, rounded to two decimal places.
FROM product_inventory;



    SELECT Category,                                               -- Groups the results by the 'Category' column.
    COUNT(ProductID) AS NumberOfProducts,                 -- Counts the number of products within each category.
    ROUND((COUNT(ProductID) * 100.0 / (SELECT COUNT(*) FROM product_inventory)), 2) AS Percentage -- Calculates the percentage of total products belonging to each category.
FROM product_inventory AS pi
GROUP BY pi.Category
ORDER BY NumberOfProducts DESC;
/*Insight: 
    **Product category distribution** helps us understand the breadth and depth of our product offerings. 
    It reveals which categories are most saturated in our inventory and can guide future procurement 
    and merchandising decisions.*/

SELECT Category,                             -- Groups the results by product 'Category'.
    ROUND(AVG(Price), 2) AS AveragePrice -- Calculates the average price for products in each category, rounded to two decimal places.
FROM product_inventory
GROUP BY Category
ORDER BY AveragePrice DESC;
/*
	Insight: 
		This analysis highlights **which product categories command higher average prices**, 
        providing insights into potential profitability per category and where our premium offerings lie.
*/




    SELECT
    COUNT(TransactionID) AS TotalSalesLineItems -- Counts every row, representing each item in a transaction.
FROM sales_transaction;

SELECT
    COUNT(DISTINCT TransactionID) AS TotalNumberOfUniqueTransactions -- Counts only distinct TransactionIDs to get unique sales events.
FROM  sales_transaction;


SELECT     
    QuantityPurchased,                                                 
    COUNT(TransactionID) AS NumberOfTransactions,                      
    ROUND((COUNT(TransactionID) * 100.0 / (SELECT COUNT(*) FROM sales_transaction)), 2) AS Percentage 
FROM sales_transaction 
GROUP BY QuantityPurchased 
ORDER BY QuantityPurchased 
LIMIT 0, 1000;



SELECT
    MIN(Price) AS MinTransactionPrice,             -- Identifies the lowest price an item was sold for in a transaction.
    MAX(Price) AS MaxTransactionPrice,             -- Identifies the highest price an item was sold for in a transaction.
    ROUND(AVG(Price), 2) AS AverageTransactionPrice, -- Calculates the average price of an item across all transactions, rounded to two decimal places.
    ROUND(STDDEV(Price) , 2)AS StdDevTransactionPrice -- Measures the typical deviation of actual transaction prices from the ave
from product_inventory;

/*
	Insight: 
		Identifying **top-revenue-generating products** is crucial for inventory prioritization, 
        strategic marketing focus, and identifying categories that are most profitable. 
        These products are likely our most valuable offerings.
*/



    SELECT
    DATE_FORMAT(TransactionDate, '%Y-%m') AS SalesMonth, -- Formats the transaction date to 'YYYY-MM' for grouping by month.
    SUM(QuantityPurchased * Price) AS TotalRevenue,   -- Calculates the total revenue for each month.
    COUNT(DISTINCT TransactionID) AS NumberOfTransactions -- Counts the unique transactions that occurred in each month.
FROM sales_transaction 
GROUP BY SalesMonth
ORDER BY SalesMonth;

SELECT
    TransactionID,          -- The unique identifier for the transaction.
    CustomerID,             -- The customer who made the purchase in this transaction.
    ProductID,              -- The product that was purchased in this specific line item.
    QuantityPurchased,      -- The number of units of the product purchased in this line item.
    Price,                  -- The price per unit at the time of this transaction.
    (QuantityPurchased * Price) AS TotalItemRevenue -- Calculates the total revenue generated by this single line item.
FROM sales_transaction;



    SELECT
    ProductID,                                  -- The identifier for the product.
    SUM(QuantityPurchased * Price) AS TotalRevenue -- Sums the revenue for all sales of this particular product.
FROM sales_transaction 
GROUP BY ProductID
ORDER BY TotalRevenue DESC
LIMIT 10; 



SELECT
    ProductID,                                 -- The identifier for the product.
    SUM(QuantityPurchased) AS TotalQuantitySold -- Sums the total quantity sold for all sales of this product.
FROM sales_transaction
GROUP BY ProductID
ORDER BY TotalQuantitySold DESC
LIMIT 10; 



SELECT
    CustomerID,                                -- The identifier for the customer.
    SUM(QuantityPurchased * Price) AS TotalSpending -- Sums the total money spent by each customer across all their transactions.
FROM sales_transaction 
GROUP BY CustomerID
ORDER BY TotalSpending DESC
LIMIT 10;


SELECT
    cp.Gender,                                          -- Groups the results by customer 'Gender'.
    SUM(st.QuantityPurchased * st.Price) AS TotalRevenue -- Calculates the total revenue for each gender.
FROM sales_transaction AS st                          -- Starts with the sales transaction table.
JOIN customer_profiles AS cp ON st.CustomerID = cp.CustomerID -- Joins with customer_profiles on matching CustomerIDs.
GROUP BY cp.Gender
ORDER BY TotalRevenue DESC; -- Orders the results to show the gender with the highest revenue contribution first.



SELECT
    cp.Location,                                        -- Groups the results by customer 'Location'.
    SUM(st.QuantityPurchased * st.Price) AS TotalRevenue -- Calculates the total revenue for each location.
FROM sales_transaction AS st
JOIN customer_profiles AS cp ON st.CustomerID = cp.CustomerID
GROUP BY cp.Location
ORDER BY
    TotalRevenue DESC; 
    SELECT
    pi.Category,                                        -- Groups the results by product 'Category'.
    SUM(st.QuantityPurchased * st.Price) AS TotalRevenue -- Calculates the total revenue for each product category.
FROM sales_transaction AS st
JOIN product_inventory AS pi ON st.ProductID = pi.ProductID -- Joins with product_inventory on matching ProductIDs.
GROUP BY pi.Category
ORDER BY TotalRevenue DESC;


    SELECT
    pi.Category,                                          -- Groups the results by product 'Category'.
    ROUND(AVG(st.QuantityPurchased), 2) AS AverageQuantityPurchased -- Calculates the average quantity purchased for items within each category, rounded to two decimal places.
FROM sales_transaction AS st
JOIN product_inventory AS pi ON st.ProductID = pi.ProductID
GROUP BY pi.Category
ORDER BY AverageQuantityPurchased DESC;


    SELECT
    CASE                                                   -- Defines custom age groups for more structured analysis.
        WHEN cp.Age < 18 THEN '<18'
        WHEN cp.Age BETWEEN 18 AND 24 THEN '18-24'
        WHEN cp.Age BETWEEN 25 AND 34 THEN '25-34'
        WHEN cp.Age BETWEEN 35 AND 44 THEN '35-44'
        WHEN cp.Age BETWEEN 45 AND 54 THEN '45-54'
        WHEN cp.Age BETWEEN 55 AND 64 THEN '55-64'
        ELSE '65+'                                         -- Catches all ages 65 and above.
    END AS AgeGroup,                                       -- Assigns the calculated age group label.
    SUM(st.QuantityPurchased * st.Price) AS TotalRevenue   -- Calculates the total revenue generated by each age group.
FROM
    sales_transaction AS st
JOIN
    customer_profiles AS cp ON st.CustomerID = cp.CustomerID
GROUP BY
    AgeGroup                                               -- Groups the results by the defined AgeGroup.
ORDER BY TotalRevenue DESC;
SELECT SUM(QuantityPurchased * Price) AS TotalRevenue -- Sums the total value of all sales transactions.
FROM sales_transaction;
   
SELECT     
    ROUND(SUM(QuantityPurchased * Price) / COUNT(DISTINCT TransactionID), 2) AS AverageTransactionValue
FROM sales_transaction
LIMIT 0, 1000;


SELECT COUNT(DISTINCT CustomerID) AS NumberOfUniqueCustomers -- Counts how many individual customers have made at least one purchase.
FROM sales_transaction;
    
    SELECT COUNT(DISTINCT ProductID) AS NumberOfUniqueProductsSold -- Counts how many different products have been sold across all transactions.
FROM sales_transaction;

SELECT st.CustomerID,                                         -- The ID of the top-spending customer.
    SUM(st.QuantityPurchased * st.Price) AS TotalSpending  -- Calculates their total expenditure across all their purchases.
FROM sales_transaction AS st
JOIN customer_profiles AS cp ON st.CustomerID = cp.CustomerID -- Joins to link sales data with customer demographic information (though not directly used in the aggregate).
GROUP BY st.CustomerID
ORDER BY TotalSpending DESC
LIMIT 10; 

SELECT
    cp.CustomerID,
    cp.Age,
    cp.Gender,
    cp.Location,
    cp.JoinDate,
    COUNT(st.TransactionID) AS NumberOfTransactions
FROM customer_profiles cp
JOIN sales_transaction st ON cp.CustomerID = st.CustomerID
GROUP BY cp.CustomerID, cp.Age, cp.Gender, cp.Location, cp.JoinDate
HAVING COUNT(st.TransactionID) > 1 -- Filters for customers who have more than one transaction.
ORDER BY NumberOfTransactions DESC, cp.CustomerID;
    
    SELECT
    pi.ProductID,          -- The unique identifier for the product.
    pi.ProductName,        -- The descriptive name of the product.
    pi.Category,           -- The category the product belongs to.
    pi.StockLevel          -- The current quantity of the product in stock.
FROM product_inventory AS pi
WHERE pi.StockLevel < 50     -- **Define your 'low stock' threshold here.** This value should be dynamic and determined by business needs (e.g., based on average daily sales and lead time for replenishment).
ORDER BY pi.StockLevel ASC;
    
    SELECT
    pi.ProductName,
    pi.Category,
    SUM(st.QuantityPurchased) AS TotalQuantitySold
FROM sales_transaction AS st
JOIN product_inventory AS pi ON st.ProductID = pi.ProductID
GROUP BY pi.ProductName, pi.Category
ORDER BY TotalQuantitySold DESC
LIMIT 5;

SELECT
    pi.ProductID,
    pi.ProductName,
    pi.Category,
    pi.StockLevel
FROM product_inventory AS pi
LEFT JOIN sales_transaction AS st ON pi.ProductID = st.ProductID
WHERE st.ProductID IS NULL -- Products that exist in inventory but have no matching sales transactions.
GROUP BY pi.ProductID, pi.ProductName, pi.Category, pi.StockLevel;

SELECT
    DATE_FORMAT(st.TransactionDate, '%Y-%m') AS SalesMonth,      -- Extracts and formats the year and month from the transaction date (e.g., '2024-06').
    SUM(st.QuantityPurchased * st.Price) AS TotalMonthlyRevenue, -- Calculates the sum of revenue for each month.
    COUNT(DISTINCT st.TransactionID) AS NumberOfMonthlyTransactions -- Counts the unique transactions occurring in each month.
FROM sales_transaction AS st
GROUP BY SalesMonth
ORDER BY SalesMonth;
    
    SELECT
    DATE_FORMAT(st.TransactionDate, '%Y-%m-%d') AS SalesDay, -- Extracts and formats the full date (e.g., '2024-06-19').
    SUM(st.QuantityPurchased * st.Price) AS TotalDailyRevenue, -- Calculates the sum of revenue for each day.
    COUNT(DISTINCT st.TransactionID) AS NumberOfDailyTransactions -- Counts the unique transactions occurring each day.
FROM sales_transaction AS st
GROUP BY SalesDay
ORDER BY SalesDay;
    
    SELECT
    DAYNAME(TransactionDate) AS DayOfWeek, -- Extracts the name of the day (e.g., 'Monday').
    COUNT(DISTINCT TransactionID) AS NumberOfTransactions,
    SUM(QuantityPurchased * Price) AS TotalRevenue
FROM sales_transaction
GROUP BY DayOfWeek
ORDER BY FIELD(DayOfWeek, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday');

SELECT
    HOUR(TransactionDate) AS SalesHour, -- Extracts the hour of the day (0-23).
    COUNT(DISTINCT TransactionID) AS NumberOfTransactions,
    SUM(QuantityPurchased * Price) AS TotalRevenue
FROM sales_transaction
GROUP BY SalesHour
ORDER BY SalesHour;
    
    SELECT
    cp.CustomerID,
    COUNT(DISTINCT YEAR(st.TransactionDate)) AS NumberOfYearsWithPurchases, -- Counts the number of distinct years a customer made a purchase.
    MIN(YEAR(st.TransactionDate)) AS FirstPurchaseYear,     -- The year of their very first purchase.
    MAX(YEAR(st.TransactionDate)) AS LastPurchaseYear       -- The year of their most recent purchase.
FROM sales_transaction AS st
JOIN customer_profiles AS cp ON st.CustomerID = cp.CustomerID
GROUP BY    cp.CustomerID
HAVING    COUNT(DISTINCT YEAR(st.TransactionDate)) > 1 -- Filters for customers who purchased in more than one unique year.
ORDER BY    NumberOfYearsWithPurchases DESC, cp.CustomerID;
    
    SELECT
    cp.CustomerID,
    SUM(st.QuantityPurchased * st.Price) AS TotalSpending
FROM sales_transaction AS st
JOINcustomer_profiles AS cp ON st.CustomerID = cp.CustomerID
GROUP BY    cp.CustomerID
ORDER BY    TotalSpending DESC
LIMIT 10;

SELECT
    cp.CustomerID,
    COUNT(DISTINCT st.TransactionID) AS NumberOfTransactions
FROM sales_transaction AS st
JOIN    customer_profiles AS cp ON st.CustomerID = cp.CustomerID
GROUP BY cp.CustomerID
ORDER BY NumberOfTransactions DESC
LIMIT 10;

SELECT
    cp.CustomerID,
    AVG(DATEDIFF(next_purchase.TransactionDate, current_purchase.TransactionDate)) AS AvgDaysBetweenPurchases
FROM sales_transaction AS current_purchase
JOIN
    sales_transaction AS next_purchase
    ON current_purchase.CustomerID = next_purchase.CustomerID
    AND current_purchase.TransactionDate < next_purchase.TransactionDate -- Ensure next_purchase is actually later
JOIN customer_profiles AS cp ON current_purchase.CustomerID = cp.CustomerID
GROUP BY cp.CustomerID
HAVING COUNT(current_purchase.TransactionID) > 1 -- Only consider customers with more than one purchase
ORDER BY AvgDaysBetweenPurchases ASC;
    
    SELECT
    st1.ProductID AS Product1ID,
    pi1.ProductName AS Product1Name,
    st2.ProductID AS Product2ID,
    pi2.ProductName AS Product2Name,
    COUNT(DISTINCT st1.TransactionID) AS NumberOfTransactionsTogether
FROM
    sales_transaction AS st1
JOIN
    sales_transaction AS st2
    ON st1.TransactionID = st2.TransactionID
    AND st1.ProductID < st2.ProductID -- Prevents duplicate pairs (A,B and B,A) and self-joins (A,A)
JOIN    product_inventory AS pi1 ON st1.ProductID = pi1.ProductID
JOIN product_inventory AS pi2 ON st2.ProductID = pi2.ProductID
GROUP BY Product1ID, Product2ID, Product1Name, Product2Name
ORDER BY    NumberOfTransactionsTogether DESC
LIMIT 10;



WITH CustomerRFM AS (
    SELECT
        CustomerID,
        DATEDIFF(
            (SELECT MAX(TransactionDate) FROM sales_transaction), -- 'Current date' = latest transaction
            MAX(TransactionDate)
        ) AS RecencyInDays,
        COUNT(DISTINCT TransactionID) AS Frequency,
        SUM(QuantityPurchased * Price) AS MonetaryValue
    FROM sales_transaction
    GROUP BY CustomerID
)
SELECT *
FROM CustomerRFM;

WITH CustomerRFM AS (
    SELECT
        CustomerID,
        DATEDIFF(
            (SELECT MAX(TransactionDate) FROM sales_transaction),
            MAX(TransactionDate)
        ) AS RecencyInDays,
        COUNT(DISTINCT TransactionID) AS Frequency,
        SUM(QuantityPurchased * Price) AS MonetaryValue
    FROM sales_transaction
    GROUP BY CustomerID
),
RFMScores AS (
    SELECT
        CustomerID,
        RecencyInDays,
        Frequency,
        MonetaryValue,
        NTILE(5) OVER (ORDER BY RecencyInDays DESC) AS R_Score, -- lower recency → higher score
        NTILE(5) OVER (ORDER BY Frequency ASC) AS F_Score,     -- higher frequency → higher score
        NTILE(5) OVER (ORDER BY MonetaryValue ASC) AS M_Score  -- higher monetary → higher score
    FROM CustomerRFM
)
SELECT * FROM RFMScores;



















