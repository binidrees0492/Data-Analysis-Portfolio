/*
SQL (Structured Query Language) is used to manage and query relational databases.
These scripts demonstrate advanced analytical queries for business intelligence.
*/

-- 1. Advanced Customer Analytics with Multiple CTEs
/*
CTE (Common Table Expression): Temporary named result set that can be referenced 
in subsequent queries. Improves readability and maintainability of complex queries.
*/
WITH CustomerSales AS (
    -- First CTE: Calculate basic customer sales metrics
    SELECT 
        c.CustomerID,
        c.CustomerName,
        c.Region,
        COUNT(DISTINCT s.OrderID) as TotalOrders,  -- Count unique orders
        SUM(s.TotalAmount) as LifetimeValue,       -- Total spending
        AVG(s.TotalAmount) as AvgOrderValue        -- Average order size
    FROM Customers c
    LEFT JOIN Sales s ON c.CustomerID = s.CustomerID  -- LEFT JOIN keeps all customers
    WHERE s.OrderDate >= DATEADD(YEAR, -1, GETDATE()) -- Last 12 months only
    GROUP BY c.CustomerID, c.CustomerName, c.Region   -- Group by customer attributes
),
CustomerSegments AS (
    -- Second CTE: Segment customers based on calculated metrics
    SELECT 
        CustomerID,
        CustomerName,
        Region,
        TotalOrders,
        LifetimeValue,
        AvgOrderValue,
        -- Customer tiering based on lifetime value
        CASE 
            WHEN LifetimeValue > 10000 THEN 'Platinum'
            WHEN LifetimeValue > 5000 THEN 'Gold'
            WHEN LifetimeValue > 1000 THEN 'Silver'
            ELSE 'Bronze'
        END as CustomerTier,
        -- PERCENT_RANK: Calculates relative rank (0-1) within result set
        PERCENT_RANK() OVER (ORDER BY LifetimeValue) as ValuePercentile
    FROM CustomerSales
)
-- Main query using the CTEs
SELECT 
    CustomerTier,
    COUNT(*) as CustomerCount,           -- Number of customers in each tier
    AVG(LifetimeValue) as AvgLifetimeValue,  -- Average spending per tier
    AVG(TotalOrders) as AvgOrdersPerCustomer -- Average orders per tier
FROM CustomerSegments
GROUP BY CustomerTier                    -- Aggregate by customer tier
ORDER BY AvgLifetimeValue DESC;          -- Show highest value tiers first

-- 2. Sales Trend Analysis with Window Functions
/*
Window Functions: Perform calculations across a set of table rows that are 
somehow related to the current row. Unlike GROUP BY, they don't collapse rows.
*/
SELECT 
    FORMAT(OrderDate, 'yyyy-MM') as YearMonth,  -- Format date as Year-Month
    ProductCategory,
    SUM(TotalAmount) as MonthlySales,           -- Total sales for month/category
    
    -- LAG: Access data from previous row in the ordered result set
    LAG(SUM(TotalAmount)) OVER (
        PARTITION BY ProductCategory             -- Reset for each category
        ORDER BY FORMAT(OrderDate, 'yyyy-MM')    -- Order by time
    ) as PreviousMonthSales,
    
    -- Calculate month-over-month growth percentage
    (SUM(TotalAmount) - LAG(SUM(TotalAmount)) OVER (
        PARTITION BY ProductCategory 
        ORDER BY FORMAT(OrderDate, 'yyyy-MM')
    )) / LAG(SUM(TotalAmount)) OVER (
        PARTITION BY ProductCategory 
        ORDER BY FORMAT(OrderDate, 'yyyy-MM')
    ) * 100 as GrowthPercentage,
    
    -- Moving average over 3 months
    AVG(SUM(TotalAmount)) OVER (
        PARTITION BY ProductCategory 
        ORDER BY FORMAT(OrderDate, 'yyyy-MM')
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW  -- Define window: current + 2 previous
    ) as ThreeMonthMovingAvg
FROM Sales s
JOIN Products p ON s.ProductID = p.ProductID      -- Link sales to products
WHERE OrderDate >= DATEADD(MONTH, -12, GETDATE()) -- Last 12 months
GROUP BY FORMAT(OrderDate, 'yyyy-MM'), ProductCategory  -- Monthly aggregates
ORDER BY ProductCategory, YearMonth;              -- Sort by category then time

-- 3. Inventory Optimization Query
-- Identifies stock issues and optimization opportunities
WITH ProductSales AS (
    -- Calculate sales patterns for each product
    SELECT 
        p.ProductID,
        p.ProductName,
        p.Category,
        SUM(s.Quantity) as TotalSold,             -- Total units sold
        AVG(s.Quantity) as AvgMonthlySales,       -- Monthly sales average
        COUNT(DISTINCT FORMAT(s.OrderDate, 'yyyy-MM')) as MonthsWithSales
    FROM Products p
    JOIN Sales s ON p.ProductID = s.ProductID
    WHERE s.OrderDate >= DATEADD(MONTH, -6, GETDATE())  -- Last 6 months
    GROUP BY p.ProductID, p.ProductName, p.Category
),
InventoryAnalysis AS (
    -- Analyze current inventory against sales patterns
    SELECT 
        ps.ProductID,
        ps.ProductName,
        ps.Category,
        ps.TotalSold,
        ps.AvgMonthlySales,
        i.CurrentStock,
        i.ReorderLevel,
        i.LeadTimeDays,
        -- Categorize stock status based on business rules
        CASE 
            WHEN i.CurrentStock <= 0 THEN 'Out of Stock'
            WHEN i.CurrentStock < ps.AvgMonthlySales THEN 'Low Stock'
            WHEN i.CurrentStock > (ps.AvgMonthlySales * 3) THEN 'Overstocked'
            ELSE 'Optimal'
        END as StockStatus,
        -- Calculate how many months of supply we have
        (i.CurrentStock / NULLIF(ps.AvgMonthlySales, 0)) as MonthsOfSupply
    FROM ProductSales ps
    JOIN Inventory i ON ps.ProductID = i.ProductID
)
-- Summary by category and status
SELECT 
    Category,
    StockStatus,
    COUNT(*) as ProductCount,              -- Number of products in each status
    AVG(MonthsOfSupply) as AvgMonthsSupply -- Average inventory coverage
FROM InventoryAnalysis
GROUP BY Category, StockStatus
ORDER BY Category, StockStatus;

-- 4. Customer Retention Analysis
-- Measures how well we retain customers over time
WITH CustomerFirstPurchase AS (
    -- Find when each customer first purchased
    SELECT 
        CustomerID,
        MIN(OrderDate) as FirstPurchaseDate
    FROM Sales
    GROUP BY CustomerID
),
CustomerMonthlyActivity AS (
    -- Track customer activity by month
    SELECT 
        cf.CustomerID,
        cf.FirstPurchaseDate,
        FORMAT(s.OrderDate, 'yyyy-MM') as ActivityMonth,
        COUNT(DISTINCT s.OrderID) as MonthlyOrders,
        SUM(s.TotalAmount) as MonthlySpend
    FROM CustomerFirstPurchase cf
    JOIN Sales s ON cf.CustomerID = s.CustomerID
    GROUP BY cf.CustomerID, cf.FirstPurchaseDate, FORMAT(s.OrderDate, 'yyyy-MM')
),
RetentionAnalysis AS (
    -- Calculate retention metrics
    SELECT 
        ActivityMonth,
        COUNT(DISTINCT CustomerID) as ActiveCustomers,
        -- New customers are those making first purchase in this month
        COUNT(DISTINCT CASE 
            WHEN ActivityMonth = FORMAT(FirstPurchaseDate, 'yyyy-MM') 
            THEN CustomerID 
        END) as NewCustomers,
        -- Compare to previous month
        LAG(COUNT(DISTINCT CustomerID)) OVER (ORDER BY ActivityMonth) as PreviousMonthCustomers,
        -- Calculate retention rate: (Current - New) / Previous
        (COUNT(DISTINCT CustomerID) - LAG(COUNT(DISTINCT CustomerID)) OVER (ORDER BY ActivityMonth)) 
        / CAST(LAG(COUNT(DISTINCT CustomerID)) OVER (ORDER BY ActivityMonth) AS FLOAT) * 100 as RetentionRate
    FROM CustomerMonthlyActivity
    GROUP BY ActivityMonth
)
-- Final retention report
SELECT 
    ActivityMonth,
    ActiveCustomers,
    NewCustomers,
    PreviousMonthCustomers,
    RetentionRate
FROM RetentionAnalysis
ORDER BY ActivityMonth;

-- 5. Supplier Performance Analysis
-- Evaluates supplier reliability and performance
SELECT 
    s.SupplierID,
    s.SupplierName,
    COUNT(DISTINCT p.ProductID) as ProductsSupplied,  -- Variety of products
    COUNT(po.PurchaseOrderID) as TotalOrders,         -- Order volume
    AVG(po.DeliveryDate - po.OrderDate) as AvgDeliveryDays, -- Speed
    -- Count late deliveries
    SUM(CASE 
        WHEN po.DeliveryDate > po.ExpectedDeliveryDate THEN 1 
        ELSE 0 
    END) as LateDeliveries,
    -- Calculate late delivery percentage
    (SUM(CASE 
        WHEN po.DeliveryDate > po.ExpectedDeliveryDate THEN 1 
        ELSE 0 
    END) * 100.0 / COUNT(po.PurchaseOrderID)) as LateDeliveryPercentage,
    SUM(po.TotalAmount) as TotalSpend,                -- Business volume
    -- Performance rating based on delivery reliability
    CASE 
        WHEN (SUM(CASE WHEN po.DeliveryDate > po.ExpectedDeliveryDate THEN 1 ELSE 0 END) * 100.0 / COUNT(po.PurchaseOrderID)) < 5 THEN 'A - Excellent'
        WHEN (SUM(CASE WHEN po.DeliveryDate > po.ExpectedDeliveryDate THEN 1 ELSE 0 END) * 100.0 / COUNT(po.PurchaseOrderID)) < 10 THEN 'B - Good'
        WHEN (SUM(CASE WHEN po.DeliveryDate > po.ExpectedDeliveryDate THEN 1 ELSE 0 END) * 100.0 / COUNT(po.PurchaseOrderID)) < 20 THEN 'C - Average'
        ELSE 'D - Poor'
    END as PerformanceRating
FROM Suppliers s
LEFT JOIN Products p ON s.SupplierID = p.SupplierID        -- Products they supply
LEFT JOIN PurchaseOrders po ON s.SupplierID = po.SupplierID -- Purchase orders
WHERE po.OrderDate >= DATEADD(YEAR, -1, GETDATE())         -- Last year only
GROUP BY s.SupplierID, s.SupplierName
HAVING COUNT(po.PurchaseOrderID) > 0  -- Only suppliers with orders
ORDER BY LateDeliveryPercentage ASC;  -- Best performers first

-- 6. Employee Sales Performance with Ranking
-- Analyzes and ranks sales team performance
SELECT 
    e.EmployeeID,
    e.FirstName + ' ' + e.LastName as EmployeeName,
    e.Department,
    e.HireDate,
    COUNT(DISTINCT s.OrderID) as TotalOrders,        -- Order count
    SUM(s.TotalAmount) as TotalSales,                -- Sales volume
    AVG(s.TotalAmount) as AvgOrderValue,             -- Order size
    -- RANK: Assigns rank with gaps for ties (1, 2, 2, 4, 5...)
    RANK() OVER (ORDER BY SUM(s.TotalAmount) DESC) as SalesRank,
    -- PERCENT_RANK: Relative position (0-1) within the group
    PERCENT_RANK() OVER (ORDER BY SUM(s.TotalAmount)) as SalesPercentile,
    -- Experience level based on tenure
    CASE 
        WHEN DATEDIFF(MONTH, e.HireDate, GETDATE()) < 6 THEN 'Trainee'
        WHEN DATEDIFF(MONTH, e.HireDate, GETDATE()) < 24 THEN 'Junior'
        ELSE 'Senior'
    END as ExperienceLevel,
    -- Sales productivity: total sales divided by months employed
    SUM(s.TotalAmount) / NULLIF(DATEDIFF(MONTH, e.HireDate, GETDATE()), 0) as SalesPerMonth
FROM Employees e
LEFT JOIN Sales s ON e.EmployeeID = s.EmployeeID     -- Link employees to sales
WHERE s.OrderDate >= DATEADD(YEAR, -1, GETDATE())    -- Last year performance
    AND e.Active = 1                                 -- Only active employees
GROUP BY e.EmployeeID, e.FirstName, e.LastName, e.Department, e.HireDate
HAVING COUNT(DISTINCT s.OrderID) > 0  -- Only employees with sales
ORDER BY TotalSales DESC;             -- Top performers first