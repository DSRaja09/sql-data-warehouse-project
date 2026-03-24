## Naming Convention

This project follows the **snake_case** naming convention for all database objects to ensure consistency, readability, and maintainability.

### snake_case Rules

- Use lowercase letters only
- Separate words using underscores (`_`)
- Avoid spaces and special characters
- Use meaningful and descriptive names
- Maintain consistent naming across all layers (Bronze, Silver, Gold)

### Examples

#### Tables / Views
customer_info  
dim_customer_info  
fact_sales  
crm_sales_details  

#### Columns
customer_id  
product_key  
order_date  
sales_amount  
created_date  

#### Schemas
Bronze  
Silver  
Gold
Note: Only the schemas had naming conventions different; 
all three schemas had capital first letter letter followed by small letters 

### Why snake_case?

snake_case improves:

- Readability of SQL code
- Consistency across the data warehouse
- Compatibility with SQL standards
- Collaboration between developers and analysts
- Maintainability of ETL pipelines
