--1.Query to get a list of patients with event type of EGV and  glucose (mgdl) greater than 155 .
SELECT  DISTINCT d.patientid
FROM dexcom d
JOIN eventtype e ON e.id=d.eventid
WHERE e.event_type = 'EGV' AND d.glucose_value_mgdl > 155
ORDER BY 1 

----
SELECT d.patientid
FROM dexcom d
JOIN eventtype e ON e.id=d.eventid
WHERE e.event_type = 'EGV' AND d.glucose_value_mgdl > 155
GROUP BY 1
ORDER BY 1 


--2.How many patients consumed meals with at least 20 grams of protein in it? 
SELECT COUNT(DISTINCT patientid) 
FROm foodlog
WHERE protein>=20;

--3.Who consumed maximum calories during dinner? (assuming the dinner time is after 6pm-8 pm)
SELECT f.patientid, d.firstname, d.lasttname, SUM(f.calorie) As total_calorie
FROM demographics d 
JOIN foodlog f on d.patientid=f.patientid
WHERE EXTRACT(HOUR FROM f.datetime) BETWEEN 18 AND 20
GROUP BY f.patientid,d.firstname,d.lasttname
ORDER BY 4 DESC LIMIT 1

--

with CTE_TotalCalories AS (
SELECT Sum(F1.calorie) Dinner ,D1.PATIENTID, D1.FIRSTNAME,D1.LASTTNAME, EXTRACT(HOUR FROM F1.DATETIME)
from foodlog F1
INNER JOIN DEMOGRAPHICS D1 ON D1.PATIENTID = F1.PATIENTID
Group By D1.PATIENTID, D1.FIRSTNAME,D1.LASTTNAME,EXTRACT(HOUR FROM F1.DATETIME)
Having EXTRACT(HOUR FROM F1.DATETIME) BETWEEN 18 AND 20
)
Select ct.PATIENTID, ct.FIRSTNAME,ct.LASTTNAME, Max(Dinner) Dinner 
from CTE_TotalCalories ct
Group By ct.PATIENTID, ct.FIRSTNAME,ct.LASTTNAME
ORDER BY Max(Dinner) DESC
Limit 1

--4.Which patient showed a high level of stress on most days recorded for him/her?

WITH StressDays AS(
	SELECT d.patientid,
		EXTRACT (DAY FROM datestamp) AS getday,
		COUNT(*) AS StressDaysCount
	FROM
    demographics d
    JOIN ibi i ON d.patientid = i.patientid
    JOIN hr h ON d.patientid = h.patientid
    JOIN eda e ON d.patientid = e.patientid
	GROUP BY d.patientid
	HAVING MAX(e.max_eda) > 40 OR AVG(RMSSD_ms)*600 < 20 OR MAX(max_hr) >100
)
SELECT patientid,StressDaysCount
FROM StressDays
WHERE StressDaysCount = (SELECT MAX(StressDaysCount) FROM StressDays)
----

select tmp.patientid, firstname, lastname, count(*) no_of_days from
(select ibi.patientid, datestamp::timestamp::date as stress_date,avg(ibi.rmssd_ms*600) as hrv From ibi
group by ibi.patientid , stress_date
 ) tmp 
inner join eda on eda.patientid = tmp.patientid and tmp.stress_date = eda.datestamp
inner join hr on hr.patientid = tmp.patientid and tmp.stress_date = hr.datestamp
inner join public.demographics demo on demo.patientid = tmp.patientid
where max_eda > 40 or max_hr > 100 or tmp.hrv < 2
-----
with top_value as 
(
 select extract(day from datestamp) day_number,patientid, max_eda, 
        row_number() over (partition by extract(day from datestamp)  order by max_eda desc) value_rank
        from eda

)
select patientid, count(patientid) as number_days_highest
from top_value
where value_rank = 1
group by patientid
order by count(patientid) desc
Limit 1


with EDA_cte as
(
select patientid, max(max_eda) EDA1 from public.eda group by 1 
),
HRV as
(
select patientid, avg(rmssd_ms)*600 HRV1 from public.ibi group by 1 having avg(rmssd_ms)*600<55
),
 HR_cte as
(
select patientid,max(max_hr) mxHR from public.hr group by 1 having max(max_hr)>90
)
select EDA_cte.patientid, EDA_cte.EDA1, HRV.HRV1, HR_cte.mxHR from EDA_cte, HR_cte,HRV, demographics where
EDA_cte.patientid=HR_cte.patientid and
HRV.patientid=HR_cte.patientid and
demographics.patientid=HRV.patientid


--5.Based on mean HR and HRV alone, which patient would be considered least healthy?

SELECT d.patientid, round(CAST(avg(h.mean_hr) AS NUMERIC),2) hr_mean, round(avg(RMSSD_ms::numeric)*600,2)
FROM demographics d
JOIN hr h ON d.patientid = h.patientid
JOIN ibi i ON d.patientid = i.patientid
GROUP BY 1
HAVING AVG(RMSSD_ms*600) < 20 AND MAX(mean_hr) >=100
ORDER BY 2,3 LIMIT 1

------
select d.patientid
from public.ibi as i 
join public.demographics as d
on i.patientid=d.patientid
join public.hr as h
on h.patientid=d.patientid
where h.mean_hr <=60 or h.mean_hr >=100
group by 1
having (AVG(i.rmssd_ms)*600) < 20
------------
select demo.PatientID,avg(HR.mean_hr) HR_Mean,round(avg(RMSSD_ms::numeric)*600)::int HRV 
from public.ibi demo
left join public.hr HR
on demo.PatientID=HR.PatientID
group by 1
having avg(HR.mean_hr)>=100 and (avg(RMSSD_ms::numeric)*600)< 50
order by 3,2 limit 1

--6. Create a table that stores any Patient Demographics of your choice as the parent table. 
--6.Create a child table that contains max_EDA and mean_HR per patient and inherits all columns from the parent table
-- Create the parent table for Patient Demographics
CREATE TABLE PatientDemographics (
    patient_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    date_of_birth DATE,
    gender VARCHAR(10)
);
--create child table
CREATE TABLE PatientData(
	patient_id SERIAL PRIMARY KEY,
	max_EDA REAL,
	mean_HR REAL
)INHERITS (PatientDemographics);

------
create table patient_demography(
    patient_id int primary key,
    gender varchar,
    dob date
);
--child table

create table health_data(
    max_eda float,
    mean_hr float,
--inherit the columns from parent table
    patient_id int references patient_demography(patient_id)    
);

--7.What percentage of the dataset is male vs what percentage is female?

SELECT 
	(COUNT(CASE WHEN gender = 'MALE' THEN 1 END)*100/COUNT(*)) As Male,
	(COUNT(CASE WHEN gender = 'FEMALE' THEN 1 END)*100/COUNT(*)) As Female
FROM demographics


--8.Which patient has the highest max eda?
SELECT patientid,max_eda FROM eda
ORDER BY 2 DESC LIMIT 1

--9.Display details of the prediabetic patients.

select * FROM demographics
WHERE HbA1c >= 5.7 AND HbA1c <= 6.4

--10.List the patients that fall into the highest EDA category by name, gender and age
SELECT d.patientid, d.firstname,d.lasttname,d.gender,AGE(d.dob) AS pat_age
FROM demographics d
JOIN eda e ON d.patientid = e.patientid
WHERE max_eda = (SELECT MAX(max_eda) FROM eda)
-------
select distinct(d.patientid),d.firstname ,d.lasttname ,d.gender , (age(d.dob))from demographics d 
left join eda e on d.patientid =e.patientid
where e.max_eda > 40 
group by d.patientid,d.firstname ,d.lasttname ,d.gender , (age(d.dob)),e.max_eda
order by d.patientid

--11.How many patients have names starting with 'A'?
SELECT DISTINCT COUNT(*), firstname FROM demographics
WHERE firstname LIKE 'A%'
GROUP BY 2

--12.Show the distribution of patients across age.
WITH patientage AS(
	SELECT age(dob)::int AS pat_age FROM demographics
)
SELECT 
	WIDTH_BUCKET(pat_age,0,100,10) AS age_bucket,
	COUNT(*)
FROM patientage
GROUP bY age_bucket
ORDER BY  age_bucket;
--------
select  
SUM(case when DATE_PART('YEAR',AGE(current_date,dob)) between 30 and 39 then 1 else 0 end) as "30_39"  ,
SUM(case when DATE_PART('YEAR',AGE(current_date,dob)) between 40 and 49 then 1 else 0 end) as "40_49",
SUM(case when DATE_PART('YEAR',AGE(current_date,dob)) between 50 and 59 then 1 else 0 end) as "50_59"
 from public.demographics
------	
select 
     case
	     when age(dob) between '20 years' and '30 years' then '20-30'
	     when age(dob) between '30 years' and '40 years' then '30-40'
	     when age(dob) between '40 years' and '50 years' then '40-50'
	     when age(dob) between '50 years' and '60 years' then '50-60'
	     when age(dob) between '60 years' and '70 years' then '60-70'
	     else '71+'
	 end as age_range,
	 count(*) as count_of_patients
from
demographics
group by 
       age_range
order by 
       age_range;
	   -------

with AgePatients as (
 select date_part('YEAR',age(DOB))::int Age, PatientID PID from public.Demographics
 )
  select width_bucket(Age,0,100,20) as Agebucket,
  int4range(min(Age),max(Age),'[]') as range,
  count(*) as NumPatients
  from AgePatients
 group by 1
 order by 1	
 
--13.Display the Date and Time in 2 seperate columns for the patient who consumed only Egg
SELECT patientid,f.datetime::DATE AS Date_consumed,
		f.datetime::TIME AS Time_consumed,logged_food
FROM foodlog f
WHERE LOWER(logged_food) ~ '\yegg\y' OR LOWER(logged_food) ~ '\yeggs\y'

SELECT patientid,f.datetime::DATE AS Date_consumed,
		f.datetime::TIME AS Time_consumed,logged_food
FROM foodlog f
WHERE logged_food = 'Eggs' OR logged_food = 'Egg'

--14.Display list of patients along with the gender and hba1c for whom the glucose value is null.
SELECT DISTINCT d.patientid,d.gender,d.HbA1c
FROM demographics d 
JOIN dexcom dex ON dex.patientid = d.patientid
WHERE dex.glucose_value_mgdl ISNULL

--15.Rank patients in descending order of Max blood glucose value per day
WITH MaxGlucose AS(
	SELECT patientid,
			EXTRACT(DAY FROM datestamp),
			MAX(glucose_value_mgdl) AS max_glucose
	FROM dexcom
	GROUP BY 1,2
)
SELECT patientid,DENSE_RANK() OVER(ORDER BY max_glucose DESC) AS maxrank,max_glucose
FROM MaxGlucose
ORDER BY 1,max_glucose DESC

-----
SELECT PatientID, MAX(Glucose_Value_mgdl) Max_Glucose,
 RANK() OVER (ORDER BY MAX(Glucose_Value_mgdl) DESC) GlHighestRank
 FROM public.Dexcom
 GROUP BY PatientID
 ORDER BY GlHighestRank;
 
------- 

select patientid,DATE(datestamp),max(glucose_value_mgdl),
dense_rank() over(partition by patientid order by max(glucose_value_mgdl) desc)as rnk
from public.dexcom
group by patientid,DATE(datestamp)
order by 1
 
 --16.Assuming the IBI per patient is for every 10 milliseconds, calculate Patient-wise HRV from RMSSD.
SELECT patientid, ROUND(AVG(RMSSD_ms::NUMERIC)*600,2) AS HRV
FROM ibi
GROUP BY patientid

--17.What is the % of total daily calories consumed by patient 14 after 3pm Vs Before 3pm?

WITH TotalCalories AS(
	SELECT patientid,SUM(calorie) As SumCalories
	FROM foodlog
	WHERE patientid = 14
	GROUP BY 1
),
CaloriesBefore3 As(
	SELECT patientid,SUM(calorie) As SumCaloriesBefore3
	FROM foodlog
	WHERE patientid = 14 AND EXTRACT(HOUR from datetime) < 15
	GROUP BY 1
),
CaloriesAfter3 As(
	SELECT patientid,SUM(calorie) As SumCaloriesAfter3
	FROM foodlog
	WHERE patientid = 14 AND EXTRACT(HOUR from datetime) >= 15
	GROUP BY 1
) 
SELECT 
	ROUND(((t1.SumCaloriesBefore3*100)/t.SumCalories),2) AS Before3,
	ROUND(((t2.SumCaloriesAfter3*100)/t.SumCalories),2) As After3
FROM TotalCalories t, CaloriesBefore3 t1, CaloriesAfter3 t2

------------------------
WITH TotalCalories AS(
	SELECT patientid,SUM(calorie) As SumCalories
	FROM foodlog
	WHERE patientid = 14
	GROUP BY 1
),
CaloriesBeforeAfter3 As(
	SELECT patientid,
	SUM(CASE WHEN EXTRACT(HOUR from datetime) < 15 THEN calorie END) AS Before3,
	SUM(CASE WHEN EXTRACT(HOUR from datetime) >= 15 THEN calorie END) AS After3
FROM foodlog
WHERE patientid = 14
	GROUP BY 1
)
SELECT
	ROUND((t2.Before3*100)/t1.SumCalories,2) AS Before,
	ROUND((t2.After3*100)/t1.SumCalories,2) AS After
FROM TotalCalories t1, CaloriesBeforeAfter3 t2

------------------------------
--18.Display 5 random patients with HbA1c less than 6.
SELECT patientid, HbA1c
FROM demographics
WHERE HbA1c < 6
ORDER BY RANDOM()
LIMIT 5;

---19.Generate a random series of data using any column from any table as the base
SELECT GENERATE_SERIES(1,15)::numeric patientid

SELECT generate_series(1,10)::numeric  Patient_id, 
round(normal_rand(10,mean_hr,5)::numeric,1) as HRDist from public.hr

---20.Display the foods consumed by the youngest patient 
SELECT DISTINCT f.logged_food, AGE(d.dob) as patage
FROM foodlog f
JOIN demographics d ON d.patientid=f.patientid
WHERE AGE(d.dob) = (SELECT MIN(AGE(dob))FROM demographics)

----21.Identify the patients that has letter 'h' in their first name and print the last letter of their first name.

SELECT firstname,
		RIGHT(lasttname,1) AS last_letter
FROM demographics 
WHERE firstname LIKE '%h%'
-------------

SELECT firstname,
	SUBSTRING(firstname FROM '\w$') AS last_letter
FROM demographics
WHERE firstname ~ 'h'


----22.Calculate the time spent by each patient outside the recommended blood glucose range

SELECT patientid,
		ROUND(COUNT(*)/12::numeric,2) OutOfRangeHrs
FROM dexcom
WHERE glucose_value_mgdl > 200 OR glucose_value_mgdl < 55
GROUP BY 1
ORDER BY 1

--23.Show the time in minutes recorded by the Dexcom for every patient
SELECT patientid,
		COUNT(*)*5 AS TimeInMin
FROM dexcom
GROUP bY 1

----
select 
      patientid,
	  datestamp,
	  extract(epoch from datestamp)/ 60.0 as extacted_minute
from 
    dexcom;
	
	
--24.List all the food eaten by patient Phill Collins
SELECT d.patientid,d.firstname,d.lasttname,f.logged_food
FROM demographics d
JOIN foodlog f ON f.patientid = d.patientid
WHERE d.firstname = 'Phill' AND d.lasttname = 'Collins'

--25.Create a stored procedure to delete the min_EDA column in the table EDA
CREATE PROCEDURE DeleteMinEDA()
LANGUAGE SQL
BEGIN
	IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'EDA' AND column_name = 'min_EDA') THEN
	EXECUTE 'ALTER TABLE EDA DROP COLUMN min_EDA';
	ELSE
	END IF;
END
$$;


CALL DeleteMinEDAColumn();

-----
CREATE PROCEDURE CleanEDA() AS ' 
 alter table public.eda drop column min_eda;
 ' LANGUAGE SQL;
 
 CALL CleanEDA();
------

CREATE OR REPLACE PROCEDURE delete_min_eda_column()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Check if the column exists before attempting to drop it
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'edA' AND column_name = 'min_eda'
    ) THEN
        -- Drop the column
        EXECUTE 'ALTER TABLE eda DROP COLUMN min_eda;';
        RAISE NOTICE 'Column min_eda dropped successfully.';
    ELSE
        RAISE NOTICE 'Column min_eda does not exist.';
		
		
--26.When is the most common time of day for people to consume spinach?
SELECT datetime, COUNT(*) AS times
FROM foodlog
WHERE LOWER(logged_food) ~ '\yspinach\y'
GROUP BY 1
ORDER BY 2 DESC LIMIT 1 
-------
Select mode() within group (order by datetime) from foodlog 
where LOWER(logged_food) ~ '\yspinach\y'


--27.Classify each patient based on their HRV range as high, low or normal
SELECT patientid, ROUND(AVG(RMSSD_ms::NUMERIC)*600,2) AS HRV,
	CASE WHEN  ROUND(AVG(RMSSD_ms::NUMERIC)*600,2) < 20 THEN 'Low HRV' 
		WHEN  ROUND(AVG(RMSSD_ms::NUMERIC)*600,2) > 50 THEN 'High HRV'
		ELSE 'Normal HRV'
	END
FROM ibi
GROUP BY 1
ORDER BY 1
		
--28. List full name of all patients with 'an' in either their first or last names
SELECT patientid, CONCAT(firstname,' ',lasttname)
FROM demographics
WHERE firstname ~ 'an' OR lasttname ~ 'an'


--29.Display a pie chart of gender vs average HbA1c
SELECT
    gender,
    ROUND(AVG(HbA1c)::numeric,2) AS average_hba1c
FROM demographics
GROUP BY 1

--30.The recommended daily allowance of fiber is approximately 25 grams a day. 
--What % of this does every patient get on average?
SELECT patientid, EXTRACT(DAY FROM datetime) as days,
	ROUND((AVG(dietary_fiber)/25),2) AS fibre
FROM foodlog
GROUP BY 1,2

--31.What is the relationship between EDA and Mean HR? 
SELECT ROUND(CORR(mean_eda,mean_hr)::numeric,2) AS correlation
FROM eda JOIN hr ON eda.patientid = hr.patientid

--32.Show the patient that spent the maximum time out of range.

SELECT patientid, 
		ROUND(COUNT(*)/12::numeric,2) Hrs_out_of_range
FROM dexcom
WHERE glucose_value_mgdl < 55 OR glucose_value_mgdl > 200
GROUP BY 1
ORDER BY 2 DESC LIMIT 1

--33.Create a User Defined function that returns min glucose value and patient ID for any date entered.

CREATE OR REPLACE FUNCTION retrieve_min_Glucose(InputDate Date) RETURNS TABLE (Min_Glucose REAL, patient_id bigint) 
LANGUAGE plpgsql
AS $$
BEGIN
RETURN QUERY
	SELECT MIN(glucose_value_mgdl),patientid
	FROM dexcom
	WHERE DATE(datestamp)= InputDate
	GROUP BY 2;
END;
$$;

SELECT * FROM retrieve_min_Glucose('2020-2-13')


--34.Write a query to find the day of highest mean HR value for each patient and display it along with the patient id.
SELECT h.patientid, EXTRACT(DAY FROM h.datestamp) AS HR_highday,ROUND(MAX(h.mean_hr)::numeric,2) AS high_hr
FROM hr h
GROUP BY 1,2
-----
WITH high_mean_hr AS (
    SELECT
        patientid,
        datestamp,
        mean_hr,
        RANK() OVER (PARTITION BY patientid ORDER BY mean_hr DESC) AS rank
    FROM
        hr
)
SELECT
    h.patientid,
    h.mean_hr,
    DATE(h.datestamp) AS highest_mean_hr_day
FROM
    high_mean_hr h
WHERE
    h.rank = 1;

--35.Create view to store Patient ID, Date, Avg Glucose value and Patient Day to every patient, 
--ranging from 1-11 based on every patients minimum date and maximum date (eg: Day1,Day2 for each patient)

CREATE OR REPLACE VIEW patient_glucose_summary AS
WITH date_range AS (
    SELECT
        patientid,
        MIN(datestamp) AS min_date,
        MAX(datestamp) AS max_date
    FROM
        dexcom
    GROUP BY
        patientid
)
SELECT
    d.patientid,
    d.datestamp AS date,
    AVG(d.glucose_value_mgdl) AS avg_glucose,
    'Day' || (ROW_NUMBER() OVER (PARTITION BY d.patientid ORDER BY d.datestamp ASC)) AS patient_day
FROM
    dexcom d
JOIN
    date_range da ON d.patientid = da.patientid
WHERE
    d.datestamp BETWEEN da.min_date AND da.max_date
GROUP BY
    d.patientid, d.datestamp
ORDER BY
    d.patientid, d.datestamp;


select * from patient_glucose_summary;

---
create or replace view demo as with subquery as
(
SELECT
Date(DateStamp) PDate ,
PatientID PID,
AVG(Glucose_Value_mgdl) AvgGlucose
FROM public.Dexcom
GROUP BY 1,2
ORDER BY 1,2 DESC
)
SELECT
PDate,
PID,
round(AvgGlucose::numeric,2),
(PDate - MIN(PDate) OVER (PARTITION BY PID))+1 PatientDay
from subquery
group by 1,2,3
order by 2,4


select * from demo




--36.Using width bucket functions, group patients into 4 HRV categories.

WITH PatientsHRV as (
	SELECT patientid, 
	ROUND(AVG(RMSSD_ms::numeric)*600)::int as HRV
FROM ibi
GROUP BY 1 
)
SELECT WIDTH_BUCKET(HRV,0,100,5) AS category,
	INT4RANGE(min(HRV),max(HRV),'[]') As HRVRange,
	COUNT(*) As patsnum
FROM PatientsHRV
GROUP BY 1 

---------
with catagorized_hrv as(
    SELECT
         ibi.patientid,
	     avg(ibi.rmssd_ms*600)as hrv,
         width_bucket(avg(ibi.rmssd_ms*600), 0, 240, 4) AS hrv_category
    FROM
       ibi
	group by ibi.patientid
)
SELECT
    chrv.patientid,
    chrv.hrv,
    chrv.hrv_category
FROM
    catagorized_hrv chrv;
	
	
--37.Is there a correlation between High EDA and  HRV. If so, display this data by querying the relevant tables?

			
SELECT ROUND(CORR(HRV,maxeda)::numeric,2)
FROM (SELECT i.patientid,ROUND(AVG(i.RMSSD_ms::numeric)*600,2) HRV,
				MAX(e.max_eda) AS maxeda
		FROM ibi i JOIN eda e ON i.patientid = e.patientid
		GROUP BY 1
	)


with GetHRV_EDA as
(
select ibi.patientid, round((avg(rmssd_ms::numeric)*600),2) HRV, max(max_eda) EDA 
from public.ibi, public.eda
where
ibi.patientid=eda.patientid
group by 1
)
select round(Corr(HRV,EDA)::numeric,2) Corr_EDA_HRV from GetHRV_EDA


--38.List hypoglycemic patients by age and gender
SELECT DISTINCT dex.patientid, d.gender,age(d.dob)
FROM demographics d
JOIN dexcom dex ON d.patientid = dex.patientid
WHERE glucose_value_mgdl < 70
ORDER BY 1

select patientid, date_part('year',AGE(current_date,dob ))as age, gender from public.demographics
where patientid in (select distinct patientid from public.dexcom
where glucose_value_mgdl <55)

--39.Write a query using recursive view(use the given dataset only)


--40.Create a stored procedure that adds a column to table IBI. The column should just be the date part extracted from 
--IBI.Date
CREATE OR REPLACE PROCEDURE add_col_to_ibi()
LANGUAGE plpgsql
AS $$
BEGIN
	IF NOT EXISTS (SELECT column_name FROM information_schema.columns 
				  WHERE table_name = 'ibi' AND column_name = 'datepart')
	THEN ALTER TABLE ibi ADD column datepart DATE;
	UPDATE ibi SET datepart = DATE(datestamp);
	ELSE
		RAISE NOTICE 'datepart alreday exists in the table';
	END IF;
END;
$$;

CALL  add_col_to_ibi()

SELECT * from ibi		

--41.Fetch the list of Patient ID's whose sugar consumption exceeded 30 grams on a meal from FoodLog table. 
SELECT DISTINCT patientid
FROM foodlog
WHERE sugar > 30

SELECT patientid from foodlog WHERE sugar>30 GROUP BY 1

--42.How many patients are celebrating their birthday this month?
SELECT COUNT(*) as Cnt
FROM demographics
WHERE EXTRACT(MONTH FROM dob) = EXTRACT(MONTH FROM NOW())

--43.How many different types of events were recorded in the Dexcom tables?
--Display counts against each Event type.
SELECT COUNT(*),event_type
FROM eventtype
GROUP BY event_type

--44.How many prediabetic/diabetic patients also had a high level of stress? 

WITH prediabetic_diabetic_patients AS (
    SELECT patientid
    FROM demographics
    WHERE hba1c >= 5.7
)
SELECT COUNT(DISTINCT pd.patientid)
FROM prediabetic_diabetic_patients pd
JOIN eda e ON pd.patientid = e.patientid
JOIN ibi i ON pd.patientid = i.patientid
JOIN hr h ON pd.patientid = h.patientid
HAVING
    MAX(e.max_eda) > 40 OR
    AVG(RMSSD_ms) * 600 < 20 OR
    MAX(h.max_hr) > 100;
	
--45.List the food that coincided with the time of highest blood sugar for every patient


SELECT DISTINCT f.patientID, f.logged_food
FROM FoodLog f
JOIN (
    SELECT DISTINCT d.patientID, MAX(d.glucose_value_mgdl) AS max_glucose, d.datestamp
    FROM dexcom d
    GROUP BY 1,3
	ORDER BY 1
) a
ON f.PatientID = a.PatientID AND f.datetime = a.datestamp
-----------
WITH MaxBloodSugarPerPatient AS (
    SELECT
        d.PatientID,
        MAX(d.Glucose_Value_mgdl) AS MaxBloodSugar,
        (d.DateStamp) AS TimeOfHighestBloodSugar
    FROM
        Dexcom d
    GROUP BY
        1,3
)
SELECT
    m.PatientID,
    f.logged_food
FROM
    MaxBloodSugarPerPatient m
JOIN
    FoodLog f
ON
    m.PatientID = f.PatientID
    AND m.TimeOfHighestBloodSugar = f.datetime;
------------

--46.How many patients have first names with length >7 letters?
SELECT COUNT(*) 
FROM demographics 
WHERE LENGTH(firstname) > 7


--47.List all foods logged that end with 'se'. Ensure that the output is in Title Case.
SELECT DISTINCT INITCAP(logged_food)
FROM foodlog
WHERE logged_food ~ 'se$'

--48.List the patients who had a birthday the same week as their glucose or IBI readings


SELECT d.patientid, DATE_PART('WEEK',d.dob) As dob,  DATE_PART('WEEK',dex.datestamp) As GlucoseWeek 
FROM demographics d 
JOIN dexcom dex ON d.patientid = dex.patientid
WHERE  DATE_PART('WEEK',d.dob) =  DATE_PART('WEEK',dex.datestamp)
GROUP BY 1,2,3		

UNION

SELECT d.patientid,  DATE_PART('WEEK',d.dob) As dob,  DATE_PART('WEEK',i.datestamp) IBIWeek
FROM demographics d 
JOIN ibi i ON d.patientid = i.patientid
WHERE  DATE_PART('WEEK',d.dob) =  DATE_PART('WEEK',i.datestamp)
GROUP BY 1,2,3	

--49.Assuming breakfast is between 8 am and 11 am. How many patients ate a meal with bananas in it?
SELECT COUNT(DISTINCT patientid)
FROM foodlog
WHERE logged_food ~* 'banana' AND DATE_PART('HOUR',datetime) between 8 AND 11

--50.Create a User defined function that returns the age of any patient based on input
CREATE OR REPLACE FUNCTION retrieve_age(Pat_ID integer) RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    Age integer;
	AgeInterval interval;
BEGIN
    SELECT AGE(dob) INTO AgeInterval
    FROM demographics
    WHERE patientid = Pat_ID;
    Age:= EXTRACT ('year' FROM AgeInterval)::integer;
    RETURN Age;
END;
$$;

SELECT * FROM retrieve_age(5);
-----------------------
CREATE FUNCTION AGE_PATIENT(ID integer) RETURNS integer AS $$
        SELECT date_part('year', age(current_date, dob)) as age_patient
        FROM demographics d WHERE d.patientid=id;
$$ LANGUAGE SQL;

---Give patient id as input
SELECT AGE_PATIENT(5); 


--51.Based on Number of hyper and hypoglycemic incidents per patient, which patient has the least control over their blood sugar?
SELECT patientid, COUNT(*) AS incidents
FROM dexcom
WHERE glucose_value_mgdl <=70 OR glucose_value_mgdl >=126
GROUP BY 1
ORDER BY 2 DESC LIMIT 1


--52.Display patients details with event details and minimum heart rate
SELECT d.patientid,d.firstname,d.lasttname,d.gender,e.event_subtype,e.event_type,MIN(h.min_hr)
FROM demographics d
JOIN dexcom dex ON dex.patientid = d.patientid
JOIN eventtype e ON e.id = dex.eventid
LEFT JOIN hr h ON h.patientid = d.patientid
GROUP BY 1,2,3,4,5,6
ORDER BY 1
---
--53.Display a list of patients whose daily max_eda lies between 40 and 50.

SELECT patientid, MAX(max_eda) FROM eda
WHERE max_eda BETWEEN 40 AND 50
GROUP BY 1

------
select PatientID,max(max_eda) Max_EDA from eda
 group by 1
 having max(max_eda)>40 and max(max_eda)<50
 order by 2
 
--54. Count the number of hyper and hypoglycemic incidents per patient
SELECT patientid,
	SUM(CASE WHEN glucose_value_mgdl <= 70 THEn 1 ELSE 0 END) AS hypoglycemic_incidents,
	SUM(CASE WHEN glucose_value_mgdl >= 126 THEN 1 ELSE 0 END) AS hyperglycemic_incidents
FROM dexcom
GROUP BY 1
ORDER BY 1 

--55.What is the variance from mean  for all patients for the table IBI?
SELECT patientid, round(VAR_POP(mean_ibi_ms)::numeric,2) AS variance
FROM ibi
GROUP BY 1

--56.Create a view that combines all relevant patient demographics and lab markers into one. Call this view ‘Patient_Overview’.
CREATE OR REPLACE VIEW Patient_Overview As (
	SELECT d.patientid,
			d.firstname,d.lasttname,
			d.gender,AVG(d.hba1c)as hba1c,
			dex.eventid, AVG(dex.glucose_value_mgdl)avg_glucose,
			AVG(e.mean_eda)as avg_eda,
			MAX(e.max_eda) as maxeda,
			MIN(h.min_hr) as minhr,
			Max(h.max_hr) as maxhr,
			AVG(h.mean_hr) as meanhr,
			AVG(i.mean_ibi_ms) as meanibi,
			AVG(RMSSD_ms) as rmssd_avg
FROm demographics d
LEFT JOIN dexcom dex ON d.patientid = dex.patientid
LEFT JOIN eda e ON dex.patientid = e.patientid
LEFT JOIN hr h ON e.patientid = h.patientid
LEFT JOIN ibi i ON h.patientid = i.patientid
	GROUP BY 1,6
)
-------
SELECT * FROM Patient_Overview
-------
create view patientOverview as
(
SELECT 
a.PatientID PID,        a.Gender Gender,a.HbA1c HBA1C,a.DOB DOB,b.EventID EventID,d.MeanIBI_Patient_ms MeanIBI,
min(b.Glucose_Value_mgdl ) MinGlucose,max(c.max_eda) MaxEDA, 
avg(e.mean_hr) AvgHR,min(e.min_hr) MinHR,max(e.max_hr) MaxHR,min(d.RMSSD_ms) MinRMSSD
from 
public.Dexcom b JOIN public.Demographics a
ON a.PatientID=b.PatientID 
LEFT JOIN public.eda c
ON b.PatientID=c.PatientID and
date(b.DateStamp)=date(c.datestamp)
LEFT JOIN public.ibi d 
ON         c.PatientID=d.PatientID and
date(c.datestamp)=date(d.Datestamp) 
LEFT JOIN public.hr e 
ON d.PatientID=e.PatientID  and
date(d.Datestamp)=date(e.datestamp)
group by 1,2,3,4,5,6
)

---

select * from patientOverview

---
--57.Create a table that stores an array of biomarkers: Min(Glucose Value), Avg(Mean_HR), Max(Max_EDA) for every patient. The result should look like this: (Link in next cell)
CREATE TABLE IF NOT EXISTS patient_biomarkers (
	pid int PRIMARY KEY,
	biomarkers numeric[]
);
INSERT INTO patient_biomarkers(pid,biomarkers)
SELECT d.patientid,
	ARRAY[min(glucose_value_mgdl),avg(mean_hr),max(max_eda)] 
FROM dexcom d,hr h,eda e
WHERE d.patientid = h.patientid
AND h.patientid = e.patientid
GROUP BY 1
----

SELECT * FROM patient_biomarkers
ORDER BY pid

--58. Assuming lunch is between 12pm and 2pm. Calculate the total number of calories consumed by each patient for lunch on "2020-02-24"

SELECT patientid, SUM(calorie) 
FROM foodlog
WHERE datetime BETWEEN ('2020-02-24 12:00:00') and ('2020-02-24 14:00:00')
GROUP BY 1

--59.What is the total length of time recorded for each patient(in hours) in the Dexcom table?

SELECT patientid, (COUNT(*)*5)/60 AS TimeInHrs
FROM dexcom
GROUP BY 1
ORDER BY 1

SELECT
    patientid,
    ROUND(EXTRACT(EPOCH FROM (MAX(datestamp) - MIN(datestamp))) / 3600,2) AS total_hours_recorded
FROM
    dexcom
GROUP BY 1
ORDER BY 1
   
select * from dexcom where patientid = 1

--60.Display the first, last name, patient age and max glucose reading in one string for every patient
With PatientDetails AS (
	SELECT d.firstname|| ' '|| d.lasttname||' '|| age(d.dob) As Details,max(dex.glucose_value_mgdl) maxglucose
	FROM demographics d
	JOIN dexcom dex ON d.patientid = dex.patientid
	GROUP BY 1
)
SELECT (Details ||' '|| maxglucose) DetailsString
FROM PatientDetails

--61.What is the average age of all patients in the database?
SELECT AVG(AGE(dob))
FROM demographics
----
SELECT ROUND(AVG(EXTRACT(YEAR FROM AGE(dob)))::numeric, 0) AS average_age
FROM demographics;

--62.Display All female patients with age less than 50
SELECT patientid,gender
FROM demographics
WHERE gender = 'FEMALE' AND EXTRACT(YEAR FROM AGE(dob)) < 50

--63.Display count of Event ID, Event Subtype and the first letter of the event subtype. Display all events 

SELECT COUNT(d.eventid) event_id_count, e.event_subtype, LEFT(e.event_subtype,1) As firstletter
FROM eventtype e
LEFT JOIN dexcom d ON e.id = d.eventid
GROUP BY 2

--64.List the foods consumed by  the patient(s) whose eventype is "Estimated Glucose Value".
SELECT DISTINCT f.logged_food
FROM foodlog f 
JOIN dexcom d ON d.patientid = f.patientid
JOIN eventtype e ON e.id = d.eventid
WHERE e.event_subtype = 'Estimated Glucose Value'
------------
SELECT DISTINCT f.logged_food
FROM
    foodlog AS f WHERE f.patientid IN(
SELECT DISTINCT d.patientid FROM dexcom d JOIN 
eventtype e ON d.eventid=e.id
WHERE e.event_subtype='Estimated Glucose Value')

--65.Rank the patients' health based on HRV and Control of blood sugar(AKA min time spent out of range)

WITH TimeOutOfRange AS(
	SELECT d.patientid, d.glucose_value_mgdl, d.datetime
	FROM dexcom d
	WHERE d.glucose_value_mgdl 
	
)



--66.Create a trigger on the food log table that warns a person about any food logged that has more than 20 grams of sugar.
--The user should not be stopped from inserting the row. Only a warning is needed.

CREATE OR REPLACE FUNCTION check_sugar_content() RETURNS TRIGGER AS $$
BEGIN
	IF NEW.sugar > 20 THEN
	RAISE NOTICE 'HIGH sugar content (%) detected for food : %',NEW.sugar,NEW.logged_food;
	END IF;
	RETURN nEW;
END;
$$ LANGUAGE plpgsql;


----creating before insert trigger
CREATE TRIGGER check_sugar
BEFORE INSERT ON foodlog
FOR EACH ROW
EXECUTE FUNCTION check_sugar_content()


INSERT INTO foodlog(PatientID,logged_food,sugar)
values
(1,'pie',32)


--67.Display all the patients with high heart rate and prediabetic
SELECT DISTINCT d.patientid, ROUND(MAX(h.max_hr)::numeric,2)as hr,d.hba1c
FROM demographics d
LEFT JOIN hr h ON d.patientid = h.patientid
WHERE hba1c between 5.7 and 6.4
GROUP BY 1,3
HAVING MAX(h.max_hr) > 100
ORDER BY 1


--68.Display patients information who have tachycardia HR and a glucose value greater than 200.

SELECt d.patientid,d.gender,d.dob
FROm demographics d, hr h,dexcom dex
WHERE d.patientid = h.patientid
AND h.patientid = dex.patientid
AND h.max_hr > 100 AND dex.glucose_value_mgdl > 200
GROUP BY 1,2,3


--69.Calculate the number of hypoglycemic incident per patient per day where glucose drops under 55
SELECT patientid,
	SUM(CASE WHEN glucose_value_mgdl <= 70 THEn 1 ELSE 0 END) AS hypoglycemic_incidents
FROM dexcom
GROUP BY 1
ORDER BY 1 

--70.List the day wise calories intake for each patient.

SELECT patientid, DATE(datetime) AS date,SUM(calorie) AS total_calories
FROM foodlog
GROUP BY 1,2
ORDER BY 1,2

--71.Display the demographic details for the patient that had the maximum time below recommended blood glucose range


--72.How many patients have a minimum HR below the medically recommended level?
SELECT COUNT (DISTINCT patientid) FROm hr WHERE min_hr < 60

--73.Create a trigger to raise notice and prevent the deletion of a record from ‘Patient_Overview’ .
CREATE OR REPLACE FUNCTION prevent_Deletion() RETURNS TRIGGER AS $$
BEGIN
	RAISE NOTICE 'Deletion of Records from Patient_Overview table not allowed';
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prevent_deletion_trigger
INSTEAD OF DELETE ON Patient_Overview
FOR EACH ROW
EXECUTE FUNCTION prevent_Deletion()

select * from patient_overview

DELETE FROM patient_overview
WHERE patientid = 1


--74.What is the average heart rate, age and gender of the every patient in the dataset?
SELECT d.patientid,ROUND(AVG(h.mean_hr)::numeric,2), EXTRACT ('year' FROM age(d.dob)),d.gender
FROM demographics d 
JOIN hr h ON d.patientid=h.patientid
GROUP BY 1

--75.What is the daily total calories consumed by every patient?
SELECT patientid, CAST(datetime AS DATE) AS date,SUM(calorie) AS total_calories
FROM foodlog
GROUP BY 1,2
ORDER BY 1,2

--76.Write a query to classify max EDA into 5 categories and display the number of patients in each category.


WITH PatientsEDA as (
	SELECT patientid, 
	max_eda::numeric
FROM eda
GROUP BY 1,2 
)
SELECT WIDTH_BUCKET(max_eda,0,100,5) AS category,
	NUMRANGE(min(max_eda),max(max_eda),'[]') As EDARange,
	COUNT(*) As patsnum
FROM PatientsEDA
GROUP BY 1 
ORDER BY 1

WITH patients_hrv AS (
    SELECT patientid, max_eda
    FROM eda
    GROUP BY 1, 2
)
SELECT WIDTH_BUCKET(max_eda, min_value, max_value, 5) AS bucket,
       COUNT(*) AS NoOfPatients
FROM patients_hrv,
    (SELECT MIN(max_eda) AS min_value FROM patients_hrv), 
    (SELECT MAX(max_eda) AS max_value FROM patients_hrv)
GROUP BY bucket
ORDER BY bucket

--77.List the daily max HR for patient with event type Exercise.
SELECT h.patientid, max(h.max_hr),e.event_type , date(d.datestamp)
FROM hr h,dexcom d, eventtype e
WHERE h.patientid = d.patientid
AND d.eventid = e.id
AND e.event_type = 'Exercise'
GROUP BY 1,3,4

--78.What is the standard deviation from mean for all patients for the table HR?
SELECT ROUND(STDDEV(mean_hr)::numeric,2)
FROM hr

--79.Give the demographic details of the patient with event type ID of 16.
SELECT * FROM demographics d
JOIN dexcom dex ON d.patientid=dex.patientid
WHERE eventid = 16


--80.Display list of patients along with their gender having a tachycardia mean HR.

SELECT d.patientid, d.gender,ROUND(AVG(mean_hr)::numeric,2)
FROM demographics d
LEFT JOIN hr h ON h.patientid = d.patientid
GROUP BY 1,2
HAVING AVG(mean_hr) > 100
