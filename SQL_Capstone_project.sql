Create database Amazon_data;
use amazon_data;

select * from amazon; -- To check if dataset imported successfully or not

-- Checking null values
Select * from amazon where 1=0; -- It ensures no rows are returned.

-- -----------------------------------------FEATURE ENGINEERING-------------------------------------------------------
-- Adding 3 new columns from existing one as:
-- 1) 'timeofday' -- To get Morning, Afternoon and Evening.
-- 2) 'dayname' -- To get Mon, Tue, Wed, Thur, Fri, sat.
-- 3) 'monthname' -- To get Jan, Feb, Mar etc.

ALTER TABLE amazon
ADD COLUMN timeofday VARCHAR(10),
ADD COLUMN dayname VARCHAR(10),
ADD COLUMN monthname VARCHAR(10);

UPDATE amazon
SET timeofday = CASE 
                    WHEN CAST(time AS TIME) BETWEEN '05:00:00' AND '11:59:59' THEN 'Morning'
                    WHEN CAST(time AS TIME) BETWEEN '12:00:00' AND '16:59:59' THEN 'Afternoon'
                    ELSE 'Evening'
                END,
    dayname = DAYNAME(STR_TO_DATE(date, '%d-%m-%Y')),
    monthname = MONTHNAME(STR_TO_DATE(date, '%d-%m-%Y'));

-- checking if these 3 new columns are added successfully or not.
select * from amazon;
-- -------------------------------------------------------------------------------------------------------------
-- Checking no. of columns and rows:
SELECT
    (SELECT COUNT(*)
     FROM information_schema.columns
     WHERE table_schema = 'amazon_data' AND table_name = 'amazon') AS column_count,
    COUNT(*) AS row_count
FROM amazon;

-- To check datatype of each column
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'amazon_data' AND table_name = 'amazon';


-- Changing name of 3 columns to work more effectively.
ALTER TABLE amazon
CHANGE COLUMN `tax 5%` vat FLOAT(6, 4);

ALTER TABLE amazon
CHANGE COLUMN `customer type` customer_type VARCHAR(30);

ALTER TABLE amazon
CHANGE COLUMN `product line` product_line VARCHAR(100);


-- --------------------------------------Business Questions & Answers ----------------------------------------------------------------
-- 1) What is the count of distinct cities in the dataset?
select count(distinct(city)) from amazon;

-- 2) For each branch, what is the corresponding city?
Select Branch, City from amazon group by 1,2 order by branch;

-- 3) What is the count of distinct product lines in the dataset?
select count(distinct(product_line)) as Number_of_Product_line from amazon;

-- 4) Which payment method occurs most frequently?
select payment,count(*) as frequency from amazon group by payment;

-- 5) Which product line has the highest sales?
Select product_line,sum(total) as total_sales from amazon group by product_line order by total_sales desc;

-- 6) How much revenue is generated each month?
select monthname as Month, sum(total) as Revenue from amazon group by 1 ;

-- 7) In which month did the cost of goods sold reach its peak?
select monthname as Month, sum(cogs) as cogs from amazon group by 1 order by cogs desc limit 1;

-- 8) Which product line generated the highest revenue?
select product_line,round(sum(total),2) as revenue from amazon group by product_line order by revenue desc limit 1;

-- 9) In which city was the highest revenue recorded?
select city, round(sum(total),2) as highest_revenue from amazon group by city order by highest_revenue desc limit 1;

-- 10) Which product line incurred the highest Value Added Tax?
select product_line, round(sum(vat),2) as Tax from amazon group by 1 order by tax desc limit 1;

-- 11) For each product line, add a column indicating "Good" if its sales are above average, otherwise "Bad."
select product_line,total,
case
	when total>avg_sales then 'Good' else 'Bad' end as Sales_Category
from amazon
join
(select avg(total) as avg_sales from amazon) as avg_table on 1=1;

-- 12) Identify the branch that exceeded the average number of products sold.
select branch,sum(quantity) as product_sold from amazon group by branch
having sum(quantity)> (select avg(product_sold)
from (select sum(quantity) as product_sold from amazon group by branch) as Avg_table);

# Calculated the total quantity of the product sold.
# Calculated the average number of products sold across all branches. The inner subquery sums up the quantity sold for each branch.
# And then the outer query calculates the average of these totals using the avg function.
# Having clause filtered branches where the sum of quantities sold> than the avg.

-- 13) Which product line is most frequently associated with each gender?
with gender_product_frequency as (
	select gender,product_line,count(*) as frequency,
    row_number() over(partition by gender order by count(*) desc) as rn
    from amazon
    group by gender,product_line
    )
    select gender,product_line,frequency
    from gender_product_frequency where rn=1;
    
    # CTE is used to get each product line for each gender.
    # Row-number assigns a unique integer to each row with a partition of gender. The rows are order by frequency in descending order.
    # where filters only those results where the row number is 1.
    
-- 14) Calculate the average rating for each product line.
select product_line, round(avg(rating),2) as average_rating from amazon group by 1 order by average_rating desc;

-- 15) Count the sales occurrences for each time of day on every weekday.
select dayname,timeofday,count(*) as sales_occurences
from amazon group by 1,2 order by 1,2;

-- 16) Identify the customer type contributing the highest revenue.
Select customer_type,round(sum(total),2) as revenue from amazon
group by customer_type order by revenue desc limit 1;

-- 17) Determine the city with the highest VAT percentage.
select city,round(sum(vat),2) as total_vat from amazon group by 1 order by total_vat desc limit 1;

-- 18) Identify the customer type with the highest VAT payments.
select customer_type,round(sum(vat),2) as Highest_Vat from amazon group by 1 order by highest_vat desc limit 1;

-- 19) What is the count of distinct customer types in the dataset?
select count(distinct(customer_type)) as distinct_customer_type from amazon;

-- 20) What is the count of distinct payment methods in the dataset?
select count(distinct(payment)) as distinct_payment from amazon;

-- 21) Which customer type occurs most frequently?
select customer_type,count(*) as frequency from amazon group by 1 order by frequency desc limit 1;

-- 22) Identify the customer type with the highest purchase frequency.
select customer_type,count(*) as frequency from amazon group by 1 order by frequency desc limit 1;

-- 23) Determine the predominant gender among customers.
select gender,count(*) as predominant_gender from amazon group by 1 order by 2 desc;

-- 24) Examine the distribution of genders within each branch.
select branch,gender,count(gender) as number from amazon group by 1,2 order by branch,gender;

-- 25) Identify the time of day when customers provide the most ratings.
select timeofday,count(rating) as no_of_ratings from amazon group by 1 order by no_of_ratings desc;

-- 26) Determine the time of day with the highest customer ratings for each branch.
select timeofday,branch,rating_frequency from (
select branch, timeofday, count(*) as rating_frequency,
row_number() over(partition by branch order by count(*) desc) as rn
from amazon 
group by branch, timeofday) as rating_time where rn=1
order by branch;

-- 27) Identify the day of the week with the highest average ratings.
select dayname, round(avg(rating),2) as Average_rating from amazon group by 1 order by 2 desc;

-- 28) Determine the day of the week with the highest average ratings for each branch.
select dayname,branch,avg_rating from (
select branch, dayname, round(avg(rating),2) as avg_rating,
row_number() over(partition by branch order by avg(rating) desc) as rn
from amazon 
group by branch,dayname) as avg_rating_time where rn =1
order by branch;

-- ---------------------------------------PRODUCT ANALYSIS------------------------------------------------------------
-- 1) There are total 6 product lines.
-- 2) Top 3 highest rated product lines are 'Food and beverages' followed by 'Fashion accessories' & 'Health and beauty' with rating above 7.
-- 3) As per Gender wise, Female customers are more for Fashion accessories.
-- 4) As per analysis Yangon branch exceeded the average number of products sold.

-- Scope of Improvement in Product: 1) Cities 'Mandalay' and 'Naypyitaw' needs improvement to increase the average number of products sold.
--                                  2) 'Sports and travel', 'Electronic accessories' & 'Home and lifestyle' requires improvement
--                                      as customers are giving low rating for these.


-- ---------------------------------------SALES ANALYSIS----------------------------------------------------------------
-- 1) Food and Beverages have highest sales among all product lines.
-- 2) In Naypyitaw branch highest revenue was recorded.
-- 3) Maximum revenue generated in the month of January approx 116291 so the Value of cogs in january was 110754.
-- 4) Every day maximum sales were done in afternoon and least were done in morning.
-- 5) There are two types of customer:'Member' and 'Normal' and those customers who has the membership of amazon contributed the most in revenue.

-- Scope of Improvement in Sales: 1) Amazon Member customers needs to be increased as they contribute most in revenue.
--                                2) Though 'Health & Beauty' has good rating but it has lowest sales number which needs to be improved.


-- ---------------------------------------CUSTOMER ANALYSIS--------------------------------------------------------------
-- 1) Customer rated 7+ three product lines which are 'Food and beverages' followed by 'Fashion accessories' and 'Health and beauty'.
-- 2) Female customers are more than Male customers.
-- 3) As per gender wise, Female customers are more for 'Fashion accessories' and Male for 'Health and beauty'.
