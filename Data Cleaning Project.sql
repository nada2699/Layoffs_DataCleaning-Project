-- Data Cleaning Project

-- 1- Remove Duplicates
-- 2- Standarize The Data
-- 3- Null Values Or Blank Values
-- 4- Remove Any Unnecessary Columns
---------------------------------------------------------------------------------
-- First We have to make a copy of the table to keep the raw data safe
CREATE TABLE layoffs_cleaneddata
LIKE layoffs;

INSERT layoffs_cleaneddata
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_cleaneddata;
---------------------------------------------------------------------------------
-- 1- Remove Duplicates
-- we have to check duplicates but we don't have a unique row id so we will make one.
-- First Row number column.
WITH dupli_CTE AS
(
SELECT *, ROW_NUMBER() 
OVER(PARTITION BY company,location,industry,
total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) as row_num
FROM layoffs_cleaneddata
)
-- Check for duplicates, we will check if there is row numbers > 1 since we partitioned by columns.
SELECT *
FROM dupli_CTE
WHERE row_num > 1;

-- Now We have to make a copy of this data with the new column "row_num" to drop dupliactes easily

CREATE TABLE `layoffs_cleaneddata2` (
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


INSERT INTO layoffs_cleaneddata2
SELECT *, ROW_NUMBER() 
OVER(PARTITION BY company,location,industry,
total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) as row_num
FROM layoffs_cleaneddata;

SELECT * 
FROM layoffs_cleaneddata2
WHERE row_num>1;

-- We Need To check if it's really duplicated or not so we will take a random company name and see.
SELECT *
FROM layoffs_cleaneddata2
WHERE company like "Casper%";

-- After We are sure that they're duplicated we delete these rows.
DELETE
FROM layoffs_cleaneddata2
WHERE row_num > 1;

-- Now there is no duplicates.
SELECT *
FROM layoffs_cleaneddata2;
-----------------------------------------------------------------------------
-- 2- Standarize The Data

-- First we check each column to understand it's data and check if there any thing need to be cleaned.
-- Company Column
SELECT DISTINCT company 
FROM layoffs_cleaneddata2;
-- Checking if there is whitespaces.
SELECT DISTINCT company ,(TRIM(company)),
CASE
	WHEN company=(TRIM(company)) Then 1
    WHEN company!=(TRIM(company)) Then 0
END AS validation_raw
FROM layoffs_cleaneddata2;

-- CLean Whitespaces
UPDATE layoffs_cleaneddata2
SET company=TRIM(company);
-------------------------------------------------------------------
-- Location Column
SELECT DISTINCT location 
FROM layoffs_cleaneddata2
ORDER BY 1;

-- Checking if there is whitespaces to use trim.
SELECT DISTINCT location ,(TRIM(location)),
CASE
	WHEN location=(TRIM(location)) Then 1
    WHEN location!=(TRIM(location)) Then 0
END AS validation_raw
FROM layoffs_cleaneddata2;
-- No Whitespaces it seems that the column is clean
--------------------------------------------------------------------
-- Industry Column
SELECT DISTINCT industry
FROM layoffs_cleaneddata2
ORDER BY 1;

-- Checking if there is whitespaces to use trim.
SELECT DISTINCT industry ,(TRIM(industry)),
CASE
	WHEN industry=(TRIM(industry)) Then 1
    WHEN industry!=(TRIM(industry)) Then 0
END AS validation_raw
FROM layoffs_cleaneddata2;

-- No Whitespaces but we have a same industry in 3 different words
SELECT DISTINCT industry
FROM layoffs_cleaneddata2
WHERE industry LIKE "Crypto%"; 

UPDATE layoffs_cleaneddata2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";
--------------------------------------------------------------------
-- Date Column
-- ReFormat & change column data type
SELECT `date`,
STR_TO_DATE(`date`,"%m/%d/%Y")
FROM layoffs_cleaneddata2;

UPDATE layoffs_cleaneddata2
SET `date`= STR_TO_DATE(`date`,"%m/%d/%Y");

ALTER TABLE layoffs_cleaneddata2
MODIFY COLUMN `date` DATE;
---------------------------------------------------------------------------------
-- Country Column
SELECT DISTINCT country 
FROM layoffs_cleaneddata2;

-- It seems that there is 2 "United States"
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_cleaneddata2;

UPDATE layoffs_cleaneddata2
SET country=TRIM(TRAILING '.' FROM country)
WHERE country LIKE "United States%";
--------------------------------------------------------------
-- Now Get Rid of Nulls and blanks
SELECT *
FROM layoffs_cleaneddata2
WHERE industry = "";

SELECT *
FROM layoffs_cleaneddata2
WHERE industry IS NULL ;

UPDATE layoffs_cleaneddata2
SET industry = NULL
WHERE industry = "";

SELECT *
FROM layoffs_cleaneddata2
WHERE industry IS NULL;

SELECT *
FROM layoffs_cleaneddata2
WHERE company = "Airbnb";


SELECT *
FROM layoffs_cleaneddata2  tb1
JOIN layoffs_cleaneddata2  tb2
ON tb1.company = tb2.company
AND tb1.location=tb2.location
WHERE tb1.industry IS NOT NULL 
AND tb2.industry IS NULL;


UPDATE layoffs_cleaneddata2  tb1
JOIN layoffs_cleaneddata2  tb2
ON tb1.company = tb2.company
AND tb1.location=tb2.location
SET tb2.industry=tb1.industry
WHERE tb1.industry IS NOT NULL 
AND tb2.industry IS NULL;


-------------------------------------------------------
-- Now we don't need row_num cloumn any more so we will drop it

ALTER TABLE layoffs_cleaneddata2
DROP COLUMN row_num;

SELECT *
FROM layoffs_cleaneddata2;
