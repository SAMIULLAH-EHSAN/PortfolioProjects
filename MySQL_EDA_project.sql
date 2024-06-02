-- ============== MySQL EDA Project ============ --
# We will explore the dataset and rank company, industry 
# and country based on the most layoffs (yearly). 

-- 1. Exploring the total_laid_off column

SELECT MIN(total_laid_off), MAX(total_laid_off)
FROM layoffs_staging2;

# Here we will be Creating CTEs (Monthly_layoffs, Layoff_month)
# to show the total layoffs for each month. 

SELECT SUM(total_laid_off) AS Layoffs, 
DATE_FORMAT(`date`,'%Y-%m') AS Layoff_month
FROM layoffs_staging2
WHERE `Date` AND total_laid_off IS NOT NULL
GROUP BY `Date`
ORDER BY 2 ASC;

# Creating CTEs
WITH Monthly_layoffs AS (
SELECT SUM(total_laid_off) AS Layoffs, 
DATE_FORMAT(`date`,'%Y-%m') AS Layoff_month
FROM layoffs_staging2
WHERE `Date` AND total_laid_off IS NOT NULL
GROUP BY `Date`
ORDER BY 2 ASC
),
Layoff_month AS
(
SELECT DATE_FORMAT(`date`,'%Y-%m') AS 
Layoff_month FROM 
layoffs_staging2
)
SELECT Layoff_month, SUM(Layoffs) AS Total_layoffs
FROM Monthly_layoffs
GROUP BY Layoff_month
ORDER BY 1 ASC;

# Now we will do a yearly ranking of the companies with most layoffs
# along with the industries. To do so we will be creating CTEs.


# Creating CTEs (Company_year, Company_yearly_rank)

WITH Company_year (Company, Industry, Country, Years, Total_laid, Percentage_layoff)
AS(
SELECT Company, Industry, Country, YEAR(`date`), SUM(total_laid_off),
CONCAT(FORMAT(percentage_laid_off * 100, 2), '%')
FROM layoffs_staging2
WHERE total_laid_off AND percentage_laid_off IS NOT NULL
GROUP BY company, industry, Country, YEAR(`date`), percentage_laid_off
), Company_yearly_rank AS (
SELECT *, 
DENSE_RANK() OVER (PARTITION BY Years ORDER BY Total_laid DESC) AS Ranking
FROM Company_year
WHERE Years IS NOT NULL
)
SELECT *
FROM Company_yearly_rank
WHERE Ranking <= 5;











-- SELECT company, YEAR(`date`), SUM(total_laid_off)
-- FROM layoffs_staging2
-- WHERE total_laid_off IS NOT NULL
-- GROUP BY Company, YEAR(`date`)


