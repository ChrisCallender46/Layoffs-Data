SELECT * FROM layoffs
LIMIT 3000;

SELECT * FROM layoffs_staging
LIMIT 3000;

SELECT * FROM layoffs_staging2
LIMIT 3000;

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging
) duplicates
WHERE 
	row_num > 1;

CREATE TABLE layoffs_staging2 (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO layoffs_staging2 (
`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging;

SELECT * FROM layoffs_staging2
WHERE row_num > 1
LIMIT 3000;

SELECT company, TRIM(company)
FROM layoffs_staging2
LIMIT 3000;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1
LIMIT 3000;

SELECT * 
FROM layoffs_staging2
WHERE industry LIKE "Crypto%"
LIMIT 3000;

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

UPDATE layoffs_staging2
SET country = "United States"
WHERE country LIKE "United States%";

SELECT `date`
FROM layoffs_staging2
LIMIT 3000;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, "%m/%d/%Y");

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
LIMIT 3000;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = "";

SELECT *
FROM layoffs_staging2
WHERE company LIKE "Bally%";

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = "")
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = "";

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
LIMIT 3000;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2
LIMIT 3000;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC
LIMIT 2000;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC
LIMIT 2000;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT company, AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 2000;

SELECT SUBSTRING(`date`, 1, 7) AS Month, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY Month
ORDER BY 1 ASC 
LIMIT 2000;

WITH Rolling_Total AS 
(
    SELECT SUBSTRING(`date`, 1, 7) AS Month, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY Month
ORDER BY 1 ASC 
LIMIT 2000
)
SELECT Month, total_off
,SUM(total_off) OVER(ORDER BY Month) AS rolling_total
FROM Rolling_Total;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
LIMIT 2000;

WITH company_year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`) 
), Company_Year_Rank AS (
SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_year
WHERE years IS NOT NULL
LIMIT 2000
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;

SELECT SUM(total_laid_off)
FROM layoffs_staging2;

--Use this to find total laid off from each industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC
LIMIT 2000;


--Use this to find what countries were most/least affected
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY SUM(total_laid_off) DESC
LIMIT 2000;

--Use this table to show top 10 companies with most layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

--Use this for total layoffs over time
WITH Rolling_Total AS 
(
    SELECT SUBSTRING(`date`, 1, 7) AS Month, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY Month
ORDER BY 1 ASC 
LIMIT 2000
)
SELECT Month, total_off
,SUM(total_off) OVER(ORDER BY Month) AS rolling_total
FROM Rolling_Total;