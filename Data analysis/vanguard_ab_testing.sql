use vanguard_ab_test;

-- Overview of each dataset:

select *
from client_profile;

select *
from experiment;

select *
from web_data;

-- Average age of group 'Control':

SELECT AVG(clnt_age) AS mean_clnt_age
FROM (
    SELECT client_profile.clnt_age
    FROM client_profile
    INNER JOIN experiment ON client_profile.client_id = experiment.client_id
    WHERE experiment.variation = 'Control'
) AS age_mean_control;

SELECT *
FROM client_profile
INNER JOIN experiment 
	ON client_profile.client_id = experiment.client_id
WHERE experiment.variation = 'Control';

-- Unique client ids per dataset :

select client_id
from client_profile; -- 70 594 rows

select distinct client_id
from client_profile; -- 70 594 distinct client ids

select client_id
from experiment; -- 50 500 rows

select distinct client_id
from experiment; -- 50 500 distinct client ids

select client_id
from web_data; -- 744 641 rows

select distinct client_id
from web_data; -- 120 157 distinct client ids

-- Unique client ids after join : 

-- web_data & client_profile:

select *
from web_data
inner join client_profile
	using(client_id)
where client_id in (select distinct client_id from web_data)
and client_id in (select distinct client_id from client_profile); -- 443 772 rows

-- web_data & experiment:

select *
from web_data
inner join experiment
	using(client_id)
where client_id in (select distinct client_id from web_data)
and client_id in (select distinct client_id from experiment); -- 317 235 rows

-- client_profile & experiment:

select *
from client_profile
inner join experiment
	using(client_id)
where client_id in (select distinct client_id from client_profile)
and client_id in (select distinct client_id from experiment); -- 50 487 distinct client ids in the joined tables

-- join all 3 datasets:

select *
from client_profile
inner join experiment
	using(client_id)
inner join web_data
	using(client_id); -- 317 123 rows

-- create a table with all 3 datasets:

CREATE TABLE all_merged AS
SELECT *
FROM client_profile
INNER JOIN experiment USING(client_id)
INNER JOIN web_data USING(client_id)
WHERE client_id IN (SELECT DISTINCT client_id FROM client_profile)
  AND client_id IN (SELECT DISTINCT client_id FROM experiment)
  AND client_id IN (SELECT DISTINCT client_id FROM web_data);
  
select *
from all_merged
where client_id = '9999729';

select *
from all_merged
where variation = 'Test'; -- 176 641 rows

select *
from all_merged
where variation = 'Control'; -- 140 482 rows

select *
from all_merged;

SELECT client_id,
    SUBSTRING(date_time, 12, 8) AS extracted_time
FROM 
    all_merged;

WITH step_durations AS (
    SELECT 
        variation,
        process_step,
        date_time AS start_time,
        LEAD(date_time) OVER (PARTITION BY client_id ORDER BY date_time) AS next_step_time
    FROM 
        all_merged
)
SELECT 
    variation,
    process_step,
    AVG(TIMESTAMPDIFF(SECOND, start_time, next_step_time)) AS average_duration_seconds,
    AVG(TIMESTAMPDIFF(MINUTE, start_time, next_step_time)) AS average_duration_minutes,
    AVG(TIMESTAMPDIFF(HOUR, start_time, next_step_time)) AS average_duration_hours
FROM 
    step_durations
GROUP BY 
    variation, process_step;
    
    

select distinct client_id -- 55 487 unique client_ids
from all_merged;

select distinct client_id
from all_merged
where variation = 'Control'; -- 23 526 unique client_ids 'control'

select distinct client_id
from all_merged
where variation = 'Test'; -- 26 961 unique client_ids 'test'


select distinct visitor_id -- 55 994 unique visitor_ids
from all_merged;

select distinct visitor_id
from all_merged
where variation = 'Control'; -- 26 271 unique visitor_id 'control'

select distinct visitor_id
from all_merged
where variation = 'Control' and process_step = 'confirm'; -- 15 560 rows

select distinct visitor_id
from all_merged
where variation = 'Test'; -- 29 908 unique visitor_id 'test'

select distinct visitor_id
from all_merged
where variation = 'Test' and process_step = 'confirm'; -- 19 499 rows


select distinct visit_id -- 69 183 unique visit_ids
from all_merged;

select distinct visit_id
from all_merged
where variation = 'Control'; -- 32 181 unique visit_ids 'control'

select distinct visit_id
from all_merged
where variation = 'Test'; -- 37 122 unique visit_ids 'test'

select client_id, count(distinct visit_id) as nb_of_visits_per_client_test
from all_merged
where variation = 'Test'
group by client_id;

select client_id, count(distinct visit_id) as nb_of_visits_per_client_control
from all_merged
where variation = 'Control'
group by client_id;

-- total number of confirms per client_id:

select client_id, count(process_step='confirm') as total_confirms
from all_merged
group by client_id
order by total_confirms desc;

-- total steps per client_id:

select client_id, count(process_step) as total_steps
from all_merged
group by client_id
order by total_steps desc;

-- create table with total number of confirms per client_id:

CREATE TABLE confirm_counts AS
SELECT client_id, COUNT(process_step='confirm') AS total_confirms
FROM all_merged
GROUP BY client_id
ORDER BY total_confirms DESC;

-- create table with total steps per client_id:

CREATE TABLE total_steps_per_client AS
SELECT client_id, COUNT(process_step) AS total_steps
FROM all_merged
GROUP BY client_id
ORDER BY total_steps DESC;

select *
from all_merged;

select client_id, count(distinct process_step)
from all_merged
where variation = 'Control'
group by client_id
having count(distinct process_step) = 1;

SELECT visit_id, COUNT(DISTINCT visit_id) AS total_test_visits
FROM all_merged
WHERE variation = 'Test'
group by visit_id;

SELECT COUNT(visit_id) AS test_single_step_visits
FROM (
    SELECT visit_id
    FROM all_merged
    WHERE variation = 'Test'
    GROUP BY visit_id
    HAVING COUNT(DISTINCT process_step) = 1
) AS single_step_visits;

-- completion rate

with a as (
SELECT client_id, variation
, max(CASE WHEN process_step='confirm' THEN 1 ELSE 0 END) AS is_confirmed
, SUM(CASE WHEN process_step='confirm' THEN 1 ELSE 0 END) AS total_confirms
FROM all_merged
GROUP BY client_id, variation)
select variation, avg(is_confirmed) from a group by 1;

-- -----------------------------------


-- create table is confirmed ( for completion rate ) 

create table is_confirmed as  
SELECT client_id, variation
, MAX(CASE WHEN process_step='confirm' THEN 1 ELSE 0 END) AS step_confirm
, SUM(CASE WHEN process_step='confirm' THEN 1 ELSE 0 END) AS total_confirms
FROM all_merged
GROUP BY client_id, variation ;

select variation, avg(is_confirmed) from a group by 1;

-- error rate
create table error_rate as  
WITH mapped_data AS (
    SELECT 
        
        client_id,
        visit_id,
        variation,
        process_step,
        date_time,
        CASE 
            WHEN process_step = 'start' THEN 1
            WHEN process_step = 'step_1' THEN 2
            WHEN process_step = 'step_2' THEN 3
            WHEN process_step = 'step_3' THEN 4
            WHEN process_step = 'confirm' THEN 5
END AS mapped_step
    FROM all_merged
),
lagged_data AS (
    SELECT *,
        LAG(mapped_step) OVER (PARTITION BY client_id, visit_id ORDER BY date_time) AS lagged_step
    FROM mapped_data
), number_errors as (

SELECT *,
    CASE WHEN mapped_step <> 1 AND mapped_step < lagged_step THEN 1 
        ELSE 0
    END AS is_error
FROM lagged_data) 
select client_id,  max(is_error) as had_error
from number_errors 
group by client_id ;


-- bounce rate 
create table bounce_rate as 
WITH visit_steps AS (
    SELECT 
        client_id,
        COUNT(DISTINCT process_step) AS num_steps
    FROM all_merged
    GROUP BY client_id
),
client_bounce AS (
    SELECT 
        client_id,
        CASE 
            WHEN num_steps = 1 THEN 1
            ELSE 0
        END AS has_bounce
    FROM visit_steps
)
SELECT 
    client_id,
    MAX(has_bounce) AS has_bounce
FROM client_bounce
GROUP BY client_id; 

WITH mapped_data AS (
    SELECT 
        
        client_id,
        visit_id,
        variation,
        process_step,
        date_time,
        CASE 
            WHEN process_step = 'start' THEN 1
            WHEN process_step = 'step_1' THEN 2
            WHEN process_step = 'step_2' THEN 3
            WHEN process_step = 'step_3' THEN 4
            WHEN process_step = 'confirm' THEN 5
END AS mapped_step
    FROM all_merged
),
lagged_data AS (
    SELECT *,
        LAG(mapped_step) OVER (PARTITION BY client_id, visit_id ORDER BY date_time) AS lagged_step
    FROM mapped_data
), number_errors as (

SELECT *,
    CASE WHEN mapped_step <> 1 AND mapped_step < lagged_step THEN 1 
        ELSE 0
    END AS is_error
FROM lagged_data) 
select client_id,  max(is_error) as had_error
from number_errors 
group by client_id ;

create table vanguard_ab_test as 
SELECT * 
FROM client_profile
INNER JOIN bounce_rate USING (client_id)
INNER JOIN error_rate USING (client_id)
INNER JOIN is_confirmed USING (client_id);
