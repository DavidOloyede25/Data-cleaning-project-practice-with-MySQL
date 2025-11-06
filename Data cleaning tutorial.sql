-- Data Cleaning
select * 
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. remove null or blank values
-- 4. remove any columns

create table layoffs_staging
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- 1. Removing Duplicates
select * 
from layoffs_staging;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1;

select * 
from layoffs_staging
where company = 'Casper';

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

DELETE 
FROM duplicate_cte
WHERE row_num > 1;

select * 
from layoffs_staging
where company = 'Casper';


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging; 

select * 
from layoffs_staging2
WHERE row_num > 1;

delete
from layoffs_staging2
WHERE row_num > 1;

-- 2. Standardizing  data
select distinct(company)
from layoffs_staging2;

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company =  trim(company);

select distinct(industry)
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';

select distinct(industry)
from layoffs_staging2;

select distinct(country)
from layoffs_staging2
order by 1;

select distinct(country), trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` DATE;

select *
from layoffs_staging2;

-- 3. remove null or blank values
select *
from layoffs_staging2
where total_laid_off IS NULL
AND  percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

select *
from layoffs_staging2
where total_laid_off IS NULL
AND  percentage_laid_off IS NULL;

DELETE 
from layoffs_staging2
where total_laid_off IS NULL
AND  percentage_laid_off IS NULL;


-- 4. remove any columns
select *
from layoffs_staging2; 

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;