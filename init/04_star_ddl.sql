\c star;

-- Time Dimension Table
CREATE TABLE Time (
    TimeId INT PRIMARY KEY,
    Date DATE,
    Year INT,
    Quarter INT,
    Month INT,
    Day INT
);

-- Customer Dimension Table
CREATE TABLE Customer (
    CustomerId INT PRIMARY KEY,
    CompanyName VARCHAR(255),
    City VARCHAR(255),
    Country VARCHAR(255)
);

-- Product Dimension Table
CREATE TABLE Product (
    ProductId INT PRIMARY KEY,
    ProductName VARCHAR(255),
    ProductCategory VARCHAR(255)
);

-- Supplier Dimension Table
CREATE TABLE Supplier (
    SupplierId INT PRIMARY KEY,
    CompanyName VARCHAR(255),
    Country VARCHAR(255)
);

-- Employee Dimension Table
CREATE TABLE Employee (
    EmployeeId INT PRIMARY KEY,
    EmpLastName VARCHAR(255),
    MngLastName VARCHAR(255)
);

-- Orders Facts Table
CREATE TABLE Orders_facts (
    CustomerId INT,
    ProductId INT,
    EmployeeId INT,
    TimeId INT,
    SupplierId INT,
    Price DECIMAL(10, 2),
    Quantity INT,
    PRIMARY KEY (CustomerId, ProductId, EmployeeId, TimeId, SupplierId),
    FOREIGN KEY (CustomerId) REFERENCES Customer(CustomerId),
    FOREIGN KEY (ProductId) REFERENCES Product(ProductId),
    FOREIGN KEY (EmployeeId) REFERENCES Employee(EmployeeId),
    FOREIGN KEY (TimeId) REFERENCES Time(TimeId),
    FOREIGN KEY (SupplierId) REFERENCES Supplier(SupplierId)
);
