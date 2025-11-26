
-- -- Create a table to store sales data
-- CREATE TABLE Ordersf (
--     SaleID INT,
--     SalesRep VARCHAR(100),
--     ProductName VARCHAR(50),
--     SaleAmount DECIMAL(10, 2),
--     SaleDate DATE
-- );

-- -- Insert sample data
-- INSERT INTO Ordersf(SaleID, SalesRep, ProductName, SaleAmount, SaleDate)
-- VALUES
--     (1, 'Sales1@contoso.com', 'Smartphone', 500.00, '2023-08-01'),
--     (2, 'Sales2@contoso.com', 'Laptop', 1000.00, '2023-08-02'),
--     (3, 'Sales1@contoso.com', 'Headphones', 120.00, '2023-08-03'),
--     (4, 'Sales2@contoso.com', 'Tablet', 800.00, '2023-08-04'),
--     (5, 'Sales1@contoso.com', 'Smartwatch', 300.00, '2023-08-05'),
--     (6, 'Sales2@contoso.com', 'Gaming Console', 400.00, '2023-08-06'),
--     (7, 'Sales1@contoso.com', 'TV', 700.00, '2023-08-07'),
--     (8, 'Sales2@contoso.com', 'Wireless Earbuds', 150.00, '2023-08-08'),
--     (9, 'Samiksha@dreamitcs.com', 'Fitness Tracker', 80.00, '2023-08-09'),
--     (10, 'Shivam@dreamitcs.com', 'Camera', 600.00, '2023-08-10');
-- CREATE SCHEMA sales;
-- GO

-- Create a table to store sales data
CREATE TABLE dbo.Orders(
    SaleID INT,
    SalesRep VARCHAR(100),
    ProductName VARCHAR(50),
    SaleAmount DECIMAL(10, 2),
    SaleDate DATE
);

-- Insert sample data
INSERT INTO dbo.Orders(SaleID, SalesRep, ProductName, SaleAmount, SaleDate)
VALUES
    (1, 'Sales1@contoso.com', 'Smartphone', 500.00, '2023-08-01'),
    (2, 'Sales2@contoso.com', 'Laptop', 1000.00, '2023-08-02'),
    (3, 'Sales1@contoso.com', 'Headphones', 120.00, '2023-08-03'),
    (4, 'Sales2@contoso.com', 'Tablet', 800.00, '2023-08-04'),
    (5, 'Sales1@contoso.com', 'Smartwatch', 300.00, '2023-08-05'),
    (6, 'Sales2@contoso.com', 'Gaming Console', 400.00, '2023-08-06'),
    (7, 'Sales1@contoso.com', 'TV', 700.00, '2023-08-07'),
    (8, 'Aryan@dreamitcs.com', 'Wireless Earbuds', 150.00, '2023-08-08'),
    (9, 'Shivam@dreamitcs.com', 'Fitness Tracker', 80.00, '2023-08-09'),
    (10, 'Samiksha@dreamitcs.com', 'Camera', 600.00, '2023-08-10'),
    (11, 'Samrin@dreamitcs.com', 'Camera', 600.00, '2023-08-10');

select * from dbo.Orders
-- drop TABLE Orders1
-- select user_name()