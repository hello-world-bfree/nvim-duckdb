-- Example DuckDB queries for the sample data files
-- Open the data files in Neovim and run these queries using :DuckDB

-- ============================================================================
-- BASIC QUERIES (employees.csv)
-- ============================================================================

-- Get all employees
SELECT * FROM buffer;

-- Count employees by department
SELECT department, COUNT(*) as count
FROM buffer
GROUP BY department
ORDER BY count DESC;

-- Average salary by department
SELECT department,
       COUNT(*) as employees,
       AVG(salary) as avg_salary,
       MIN(salary) as min_salary,
       MAX(salary) as max_salary
FROM buffer
GROUP BY department;

-- Top 5 highest paid employees
SELECT name, department, salary
FROM buffer
ORDER BY salary DESC
LIMIT 5;

-- Employees hired in 2020
SELECT name, department, hire_date
FROM buffer
WHERE hire_date >= '2020-01-01' AND hire_date < '2021-01-01'
ORDER BY hire_date;

-- ============================================================================
-- AGGREGATIONS (products.json)
-- ============================================================================

-- Products by category
SELECT category, COUNT(*) as count, AVG(price) as avg_price
FROM buffer
GROUP BY category;

-- High-rated products in stock
SELECT name, price, stock, rating
FROM buffer
WHERE rating >= 4.5 AND stock > 0
ORDER BY rating DESC, price ASC;

-- Total inventory value
SELECT category,
       SUM(price * stock) as total_value,
       SUM(stock) as total_units
FROM buffer
GROUP BY category
ORDER BY total_value DESC;

-- ============================================================================
-- WINDOW FUNCTIONS (employees.csv)
-- ============================================================================

-- Rank employees by salary within each department
SELECT name,
       department,
       salary,
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
FROM buffer;

-- Running total of salaries
SELECT name,
       salary,
       SUM(salary) OVER (ORDER BY hire_date) as running_total
FROM buffer
ORDER BY hire_date;

-- Department salary percentiles
SELECT department,
       name,
       salary,
       PERCENT_RANK() OVER (PARTITION BY department ORDER BY salary) as percentile
FROM buffer;

-- ============================================================================
-- ADVANCED QUERIES
-- ============================================================================

-- CTEs: High earners analysis
WITH high_earners AS (
  SELECT * FROM buffer WHERE salary > 80000
),
dept_stats AS (
  SELECT department, AVG(salary) as avg_salary
  FROM buffer
  GROUP BY department
)
SELECT he.name, he.salary, ds.avg_salary,
       he.salary - ds.avg_salary as difference
FROM high_earners he
JOIN dept_stats ds ON he.department = ds.department
ORDER BY difference DESC;

-- Subqueries: Above average salaries
SELECT name, department, salary
FROM buffer
WHERE salary > (SELECT AVG(salary) FROM buffer)
ORDER BY salary DESC;

-- CASE expressions: Salary categories
SELECT name,
       salary,
       CASE
         WHEN salary < 70000 THEN 'Entry Level'
         WHEN salary < 90000 THEN 'Mid Level'
         WHEN salary < 100000 THEN 'Senior'
         ELSE 'Principal'
       END as level
FROM buffer
ORDER BY salary;

-- ============================================================================
-- DATE FUNCTIONS (employees.csv)
-- ============================================================================

-- Years of service
SELECT name,
       hire_date,
       DATEDIFF('year', CAST(hire_date AS DATE), CURRENT_DATE) as years_of_service
FROM buffer
ORDER BY years_of_service DESC;

-- Employees by hire quarter
SELECT EXTRACT(YEAR FROM CAST(hire_date AS DATE)) as year,
       EXTRACT(QUARTER FROM CAST(hire_date AS DATE)) as quarter,
       COUNT(*) as hires
FROM buffer
GROUP BY year, quarter
ORDER BY year, quarter;

-- ============================================================================
-- STRING FUNCTIONS
-- ============================================================================

-- Extract first names
SELECT SPLIT_PART(name, ' ', 1) as first_name,
       department,
       salary
FROM buffer;

-- Search for names containing 'son'
SELECT name, department
FROM buffer
WHERE name LIKE '%son%';

-- ============================================================================
-- STATISTICAL FUNCTIONS (employees.csv)
-- ============================================================================

-- Salary statistics
SELECT department,
       COUNT(*) as count,
       AVG(salary) as mean,
       MEDIAN(salary) as median,
       STDDEV(salary) as std_dev,
       MIN(salary) as min,
       MAX(salary) as max
FROM buffer
GROUP BY department;

-- ============================================================================
-- JSON QUERIES (products.json)
-- ============================================================================

-- Filter by nested properties (if JSON has nested structure)
-- Example: SELECT * FROM buffer WHERE price > 100;

-- ============================================================================
-- MULTI-BUFFER QUERIES
-- (Open both employees.csv and products.json, then run these)
-- ============================================================================

-- Cross join (Cartesian product) - be careful with large datasets!
-- SELECT e.name, p.name as product
-- FROM buffer('employees.csv') e, buffer('products.json') p
-- LIMIT 10;

-- ============================================================================
-- EXPORT EXAMPLES (via Lua)
-- ============================================================================

-- Run these from Neovim using Lua:
-- :lua require('duckdb').query('SELECT * FROM buffer WHERE salary > 90000', {export = '/tmp/high_earners.csv', format = 'csv'})
-- :lua require('duckdb').query('SELECT * FROM buffer', {export = '/tmp/all_employees.json', format = 'json'})
