-- data cleaning
use sql_myproject;
select *from `world layoffs dataset`;
-- create a dupilcate table to work with
create table world_layoffs
like `world layoffs dataset`; 

insert into world_layoffs
select *from `world layoffs dataset`;

-- check for duplicate
select *,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,'date',stage,
country,funds_raised_millions) as ROW_NUM
from world_layoffs;

with layoff_duplicate as
(select *,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,'date',stage,
country) as ROW_NUM
from world_layoffs
)
select *
from layoff_duplicate 
where row_num>1;
-- check duplicate accuracy per row before removing
select * from world_layoffs
where company='casper';
select * from world_layoffs
where company='hibob';

-- remove the right duplicate,we want to add row_num column to table
CREATE TABLE `world_layoffs2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *from world_layoffs2;

insert into world_layoffs2
select *,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,'date',stage,
country,funds_raised_millions) as ROW_NUM
from world_layoffs;

set sql_safe_updates=0;
delete 
 from world_layoffs2
where row_num>1;

select *from world_layoffs2;

-- standardizing data
select company,trim(company)
from world_layoffs2;
select country,trim(country)
from world_layoffs2;

update world_layoffs2
set company=trim(company);

select distinct industry
from world_layoffs2
order by 1;
select *
from world_layoffs2
where industry like 'crypto%';

update world_layoffs2
set industry= 'crypto'
where industry like 'crypto%';

select distinct country,trim(trailing '.' from country)
from world_layoffs2;
update world_layoffs2
set country=trim(trailing '.' from country)
where country like'united states%';

-- change date format
ALTER TABLE world_layoffs2
modify column date DATE; 

 update world_layoffs2
 set date = str_to_date(date, '%m/%d/%Y');
 select * from world_layoffs2;
 
 -- NULL values
 SELECT 
    *
FROM
    world_layoffs2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;


update world_layoffs2
set industry=null
where industry =' ';
 
select * 
from world_layoffs2
where company='Airbnb';

SELECT t1.industry, t2.industry
FROM
    world_layoffs2 t1
        JOIN
    world_layoffs2 t2 ON t1.company = t2.company
WHERE
    (t1.industry IS NULL OR t1.industry = '')
        AND t2.industry IS NOT NULL;
 
 update world_layoffs2 t1
join world_layoffs2 t2
 on t1.company=t2.company
 set t1.industry = t2.industry
 WHERE
    t1.industry IS NULL 
	AND t2.industry IS NOT NULL;
 

