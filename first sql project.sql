select *
from layoff2; 

create table layoff2 
like layoffs;

insert layoff2 
select * 
from layoffs;


select  *
from ( 	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
	  ROW_NUMBER() OVER (
						PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions)as rawnum 
from layoff3 ) as dupl
where rawnum > 1;

CREATE TABLE `layoff3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `rownum` INT
);
INSERT layoff3
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`rownum`)
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
					PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions)as rawnum 
FROM layoff2;

SET SQL_SAFE_UPDATES = 0;

DELETE FROM LAYOFFSD.layoff3
WHERE rownum >= 2;

select * from layoff3 
where rownum =2;

SELECT DISTINCT industry
FROM layoff2
ORDER BY industry;

select * from layoff2 
where industry is null or industry = '' 
order by industry;

SELECT *
FROM layoff2
WHERE company LIKE 'Bally%' ;

UPDATE layoff2
SET industry = NULL
WHERE industry = '';

UPDATE layoff2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

SELECT DISTINCT country
FROM layoff2
ORDER BY country;

UPDATE layoff2
SET country = TRIM(TRAILING '.' FROM country);


UPDATE layoff2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoff2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoff2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

delete
FROM layoff2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

ALTER TABLE layoff2
DROP COLUMN rownum;

SELECT MAX(total_laid_off)
FROM layoff2;

SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoff2
WHERE  percentage_laid_off IS NOT NULL;

SELECT *
FROM layoff2
WHERE  percentage_laid_off = 1;

SELECT *
FROM layoff2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, total_laid_off
FROM layoff2
ORDER BY 2 DESC
LIMIT 10;


SELECT location, sum(total_laid_off)
FROM layoff2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

SELECT country, sum(total_laid_off)
FROM layoff2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM layoff2
GROUP BY YEAR(date)
ORDER BY 1 desc;

SELECT industry, SUM(total_laid_off)
FROM layoff2
GROUP BY industry
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoff2
GROUP BY stage
ORDER BY 2 DESC;

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoff2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoff2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;

