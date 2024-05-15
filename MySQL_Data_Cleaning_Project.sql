-- ============== MySQL Data Cleaning Project ============ --

-- We will perform the following in our Data Cleaning :
-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null values or Blank values
-- 4. Remove Unecessary columns
-- ====================================
# Before we start with the data cleaning, 
# we will first create a staging area/landing zone (Data Staging),
# so that our primary/raw data is live, available and unchanged during the ETL.
# We will be using the staging area throughout the project.
-- ====================================

# Creating a staging area (layoffs_staging) out of the primary data (layoffs).

CREATE TABLE layoffs_staging
LIKE layoffs;

# Inserting staged data (copy of the primary data) into the staging area.

INSERT INTO layoffs_staging
SELECT * FROM
layoffs;

-- 1. Removing Duplicates
# Since we don't have a unique row id to remove the duplicates, so we will first
# create a row number column (using the ROW_NUMBER () Function),
# that will have a unique row number (1) for every unique record. 
# This row number (1) will serve as a unique row id which we can use 
# as a filter on the data to remove duplicates where row_num > 1.
-- ================
# 1.a  Creating a new column (row_num) 

SELECT *,
ROW_NUMBER ()
OVER( PARTITION BY company, industry, total_laid_off,
percentage_laid_off, `date`, stage, country,
funds_raised_millions ) AS row_num
FROM layoffs_staging;

# Now in order to use the row_num column as a filter on our data for 
# removing the duplicates, we will have to create a CTE that will help us 
# remove the duplicates on the condition (WHERE row_num > 1).
# For creating this CTE, just copy the above created column (row_num)
# and paste it in the CTE.

# 1.b Creating a CTE (duplicate_cte)

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER ()
OVER( PARTITION BY company, industry, total_laid_off,
percentage_laid_off, `date`, stage, country,
funds_raised_millions ) AS row_num
FROM layoffs_staging
)
SELECT *    
FROM duplicate_cte     # here we are specifying the condition (WHERE row_num > 1)
WHERE row_num > 1;     # using the CTE to view the duplicates. 

# Now we can Delete the duplicates using the CTE, however we can not do so 
# because a DELETE function is basically an update function in MYSQL and 
# the update functions (DELETE, ALTER, SET, etc) do not work with a CTE. 
# Therefore, we will have to create an another staging area where we can perform 
# the DELETE function on the rows that are > 1, without having to use a CTE. 

# In order to create an another staging area (layoffs_staging2),
# just left click on the layoffs_staging in the SCHEMAS panel,
# and after left clicking on the layoffs_staging, 
# click on 'Send to SQL editor' and within it click on the 'Create statement'. 
# This will create a statement for our new staging area
# based on the statement of our first staging area.
# Now we need to manually add the column (row_num) which is serving 
# as the unique row_id for our data. After doing this we can now 
# staright away remove the duplicates using the DELETE function.

  

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

# Inserting data by copying the row_num column query in the layoffs_staging,
# doing so will allow us to copy all the primary data + the row_num column which
# is acting as our unique row id. 

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER ()
OVER( PARTITION BY company, industry, total_laid_off,
percentage_laid_off, `date`, stage, country,
funds_raised_millions ) AS row_num
FROM layoffs_staging;

# Now we will delete all the records that are > 1 because these 
# records are duplicates. 

DELETE
FROM layoffs_staging2
WHERE row_num > 1;


-- 2. Standardizing the data

## Trimming the Company Column

UPDATE layoffs_staging2
SET company = TRIM(company);

# Trimming the Country Column

-- we will remove the period from 'United states.' and 
-- update it to correct country name (United States) 

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';  
-- or we can do it in the following way as well ;
UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';  

## Updating the Industry Column

SELECT DISTINCT industry 
FROM layoffs_staging2;

# standardizing the Crypto industry name

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

# Updating the date column by changing its format from
# text to MySQL's standard date format. 

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y'); 

# Now changing the data type of the date column from 
# text data type to date data type. 
# NOTE : NEVER DO IT on primary/raw data, ALWAYS DO IT
# on the staged/copied data so that the originality of 
# the raw data is maintained. 

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Removing Null and Blank Values. 

# Removing records that are Null in both - total_laid_off and 
# percentage_laid_off column, at the same time. 

SELECT * FROM 
layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

# Deleting the nulls from these columns
DELETE FROM 
layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

# Replacing blank and null values with values in the Industry Column
SELECT DISTINCT industry
FROM layoffs_staging2;

# Looking for blank and null values in the column. 
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR 
industry = '';

# We can't right away remove blank and null values unless they are
# completely useless and not populated in the data elsewhere. 
# Now we will look for blank or null records if they are populated 
# in the data elsewhere. We will take company 'Airbnb' as an example.

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

# now we will populate the blank records in the industry column
# For ex : we will add 'Travel' to the blank record in the 
# industry column where company is 'Airbnb', so we will have all 
# 'Airbnb' records having 'Travel' mentioned. This way our blank 
# values will be populated with the useful records that we will need for 
# our EDA. 

# To do so we will use a JOIN on the same table (layoffs_staging2) and 
# also we will de updating the blank records in the industry column
# to 'Null' so that we can use them to replace with 'Not null' values
# through a JOIN. 

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

SELECT * FROM
layoffs_staging2
WHERE industry IS NULL;

# Now will do a JOIN on the same table and get results

SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
     ON 
     t1.company = t2.company
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON
    t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;    
 
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';






