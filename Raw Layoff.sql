SELECT * FROM layoff ;

CREATE TABLE layoff_raw
LIKE layoff
;

INSERT INTO layoff_raw
SELECT *
     FROM layoff
LIMIT 10000
;

SELECT *
    FROM layoff_raw
LIMIT 10000
;

SELECT *,
ROW_NUMBER() OVER 
    (PARTITION BY companies, location, stage, industry, total_laid_off, percentage_laid_off,`date`, 
    country, funds_raised_millions) AS row_num
FROM layoff_raw
;

WITH DUPLICATE_CTE as 
(
SELECT *,
ROW_NUMBER() OVER 
    (PARTITION BY companies, location, stage, industry, total_laid_off, percentage_laid_off,`date`, 
    country, funds_raised_millions) AS row_num
FROM layoff_raw
)
SELECT *
    FROM DUPLICATE_CTE
    WHERE row_num > 1
LIMIT 10000
;

---------------------------------------------------------------------------------------------------------------------------

CREATE TABLE `layoffs_construction`
(
  `companies` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
;

INSERT INTO layoffs_construction
SELECT *,
ROW_NUMBER() OVER 
    (PARTITION BY companies, location, stage, industry, total_laid_off, percentage_laid_off,`date`, 
    country, funds_raised_millions)
FROM layoff_raw
;

SELECT *
    FROM layoffs_construction
WHERE row_num > 1
;

DELETE
    FROM layoffs_construction
WHERE row_num > 1
;

SELECT companies, TRIM (companies)
FROM layoffs_construction
;

UPDATE layoffs_construction
SET companies = TRIM(companies)
;

SELECT *
FROM layoffs_construction
;

SELECT * 
    FROM layoffs_construction
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_construction
    SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'

SELECT DISTINCT country, TRIM(TRAILING '.'FROM  country)
    FROM layoffs_construction
WHERE country LIKE 'United States%'
;

UPDATE layoffs_construction
    SET country = TRIM(TRAILING '.'FROM  country)
WHERE country LIKE 'United States%'
;

ALTER TABLE layoffs_construction
MODIFY COLUMN `date` DATE
;

SELECT `date`, 
    STR_TO_DATE(`date`,'%m-%d-%Y') AS formatted_date
FROM layoffs_construction
LIMIT 10000;
;

UPDATE layoffs_construction
SET `date` = STR_TO_DATE(`date`,'%m-%d-%Y')
;

SELECT *
    FROM layoffs_construction
    WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT *
    FROM layoffs_construction
    WHERE industry IS NULL OR industry = ''
ORDER BY 1
;

SELECT *
    FROM layoffs_construction
WHERE companies LIKE 'Bally%'
;

UPDATE layoffs_construction
    SET  industry = NULL
WHERE industry = ''

SELECT T1.industry, T2.industry
    FROM layoffs_construction T1
JOIN layoffs_construction T2
    ON T1.companies = T2.companies
    AND T1.location = T2.location
WHERE (T1.industry  IS NULL OR T1.industry = '' )
    AND T2.industry IS NOT NULL
;

UPDATE layoffs_construction T1
JOIN layoffs_construction T2
    ON T1.companies = T2.companies
    SET T1.industry = T2.industry
WHERE T1.industry  IS NULL
    AND T2.industry IS NOT NULL
;

SELECT *
FROM layoffs_construction
;

SELECT *
    FROM layoffs_construction
    WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

DELETE
    FROM layoffs_construction
    WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

ALTER TABLE layoffs_construction
DROP COLUMN row_num

SELECT *
    FROM layoffs_construction
LIMIT 10000
;