--  Cleaning

SELECT *
FROM layoffs;


CREATE TABLE layofftemp_staging
LIKE layoffs;

SELECT *
FROM layofftemp_staging;

INSERT layofftemp_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off,percentage_laid_off, 'date',
stage, country, funds_raised_millions) AS no_row
FROM layofftemp_staging;









CREATE TABLE `layofftemp1_staging` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `no_row` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layofftemp1_staging;

INSERT INTO layofftemp1_staging
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off, 'date',
stage, country, funds_raised_millions) AS no_row
FROM layofftemp_staging;

SELECT *
FROM layofftemp1_staging
WHERE no_row > 1;

SET SQL_SAFE_UPDATES = 0;

DELETE
FROM layofftemp1_staging
WHERE no_row > 1;

SELECT *
FROM layofftemp1_staging1;


-- Standardizing data

SELECT company, (TRIM(company))
FROM layofftemp1_staging;

UPDATE layofftemp1_staging
SET company = TRIM(company);

SELECT *
FROM layofftemp1_staging
WHERE industry LIKE "Crypt%";

UPDATE layofftemp1_staging
SET industry="Crypto"
WHERE industry like 'Crypt%';

SELECT  DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layofftemp1_staging
ORDER BY 1;

UPDATE layofftemp1_staging
SET country=TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'united States%';

SELECT  `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layofftemp1_staging
;

UPDATE layofftemp1_staging
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');


ALTER TABLE layofftemp1_staging
MODIFY COLUMN `date` DATE;


SELECT *
FROM layofftemp1_staging
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layofftemp1_staging 
WHERE company LIKE 'Bally%';

UPDATE layofftemp1_staging
SET industry = NULL
WHERE industry = '';

SELECT tb1.industry, tb2.industry
FROM layofftemp1_staging tb1
JOIN layofftemp1_staging tb2
ON tb1.company=tb2.company
WHERE (tb1.industry IS NULL OR tb1.industry='' )
AND tb2.industry IS NOT NULL;

UPDATE layofftemp1_staging tb1
JOIN layofftemp1_staging tb2
ON tb1.company=tb2.company
SET tb1.industry=tb2.industry
WHERE (tb1.industry IS NULL )
AND tb2.industry IS NOT NULL;

SELECT *
FROM layofftemp1_staging ;

DELETE 
FROM layofftemp1_staging
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

ALTER TABLE layofftemp1_staging
DROP COLUMN no_row;


SELECT YEAR(`date`) AS year, COUNT(*) AS layoffs
FROM layofftemp1_staging
GROUP BY year;

SELECT company, SUM(total_laid_off) AS total
FROM layofftemp1_staging
GROUP BY company
ORDER BY total DESC
LIMIT 5;



