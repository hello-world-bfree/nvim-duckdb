-- Example DuckDB queries for the sample data files
-- Select a query and run it using visual selection + <leader>dq

-- ============================================================================
-- BASIC QUERIES (employees.csv)
-- ============================================================================

-- Get all employees
SELECT * FROM read_csv('./examples/employees.csv');

-- Count employees by department
SELECT department, COUNT(*) as count
FROM read_csv('./examples/employees.csv')
GROUP BY department
ORDER BY count DESC;

-- Average salary by department
SELECT department,
       COUNT(*) as employees,
       AVG(salary) as avg_salary,
       MIN(salary) as min_salary,
       MAX(salary) as max_salary
FROM read_csv('./examples/employees.csv')
GROUP BY department;

-- Top 5 highest paid employees
SELECT name, department, salary
FROM read_csv('./examples/employees.csv')
ORDER BY salary DESC
LIMIT 5;

-- Employees hired in 2020
SELECT name, department, hire_date
FROM read_csv('./examples/employees.csv')
WHERE hire_date >= '2020-01-01' AND hire_date < '2021-01-01'
ORDER BY hire_date;

-- ============================================================================
-- AGGREGATIONS (products.json)
-- ============================================================================

-- Products by category
SELECT category, COUNT(*) as count, AVG(price) as avg_price
FROM read_json('./examples/products.json')
GROUP BY category;

-- High-rated products in stock
SELECT name, price, stock, rating
FROM read_json('./examples/products.json')
WHERE rating >= 4.5 AND stock > 0
ORDER BY rating DESC, price ASC;

-- Total inventory value
SELECT category,
       SUM(price * stock) as total_value,
       SUM(stock) as total_units
FROM read_json('./examples/products.json')
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
FROM read_csv('./examples/employees.csv');

-- Running total of salaries
SELECT name,
       salary,
       SUM(salary) OVER (ORDER BY hire_date) as running_total
FROM read_csv('./examples/employees.csv')
ORDER BY hire_date;

-- Department salary percentiles
SELECT department,
       name,
       salary,
       PERCENT_RANK() OVER (PARTITION BY department ORDER BY salary) as percentile
FROM read_csv('./examples/employees.csv');

-- ============================================================================
-- ADVANCED QUERIES
-- ============================================================================

-- CTEs: High earners analysis
WITH employees AS (
  SELECT * FROM read_csv('./examples/employees.csv')
),
high_earners AS (
  SELECT * FROM employees WHERE salary > 80000
),
dept_stats AS (
  SELECT department, AVG(salary) as avg_salary
  FROM employees
  GROUP BY department
)
SELECT he.name, he.salary, ds.avg_salary,
       he.salary - ds.avg_salary as difference
FROM high_earners he
JOIN dept_stats ds ON he.department = ds.department
ORDER BY difference DESC;

-- Subqueries: Above average salaries
WITH employees AS (SELECT * FROM read_csv('./examples/employees.csv'))
SELECT name, department, salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees)
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
FROM read_csv('./examples/employees.csv')
ORDER BY salary;

-- ============================================================================
-- DATE FUNCTIONS (employees.csv)
-- ============================================================================

-- Years of service
SELECT name,
       hire_date,
       DATEDIFF('year', CAST(hire_date AS DATE), CURRENT_DATE) as years_of_service
FROM read_csv('./examples/employees.csv')
ORDER BY years_of_service DESC;

-- Employees by hire quarter
SELECT EXTRACT(YEAR FROM CAST(hire_date AS DATE)) as year,
       EXTRACT(QUARTER FROM CAST(hire_date AS DATE)) as quarter,
       COUNT(*) as hires
FROM read_csv('./examples/employees.csv')
GROUP BY year, quarter
ORDER BY year, quarter;

-- ============================================================================
-- STRING FUNCTIONS
-- ============================================================================

-- Extract first names
SELECT SPLIT_PART(name, ' ', 1) as first_name,
       department,
       salary
FROM read_csv('./examples/employees.csv');

-- Search for names containing 'son'
SELECT name, department
FROM read_csv('./examples/employees.csv')
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
FROM read_csv('./examples/employees.csv')
GROUP BY department;

-- ============================================================================
-- JSONL QUERIES (orders.jsonl)
-- ============================================================================

-- Order totals by customer
SELECT customer_id, COUNT(*) as orders, SUM(total) as total_spent
FROM read_json('./examples/orders.jsonl', format='newline_delimited')
GROUP BY customer_id
ORDER BY total_spent DESC;

-- Orders by status
SELECT status, COUNT(*) as count
FROM read_json('./examples/orders.jsonl', format='newline_delimited')
GROUP BY status;

-- ============================================================================
-- MULTI-TABLE QUERIES
-- ============================================================================

-- Join employees and products (cross join example)
SELECT e.name as employee, p.name as product, p.price
FROM read_csv('./examples/employees.csv') e,
     read_json('./examples/products.json') p
WHERE e.department = 'Engineering'
LIMIT 10;

-- ============================================================================
-- EXPORT EXAMPLES (via Lua)
-- ============================================================================

-- Run these from Neovim command line:
-- :lua require('duckdb').query("SELECT * FROM read_csv('./examples/employees.csv') WHERE salary > 90000", {export = '/tmp/high_earners.csv', format = 'csv'})
-- :lua require('duckdb').query("SELECT * FROM read_csv('./examples/employees.csv')", {export = '/tmp/all_employees.json', format = 'json'})
