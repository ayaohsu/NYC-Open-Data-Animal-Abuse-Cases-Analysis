-- geo map with count
SELECT incident_address, COUNT(*) AS count_of_requests
FROM fact_service_request
WHERE created_date_key BETWEEN 20220928 AND 20221003
AND complaint_type_key = 4 AND descriptor = 'Tortured'
GROUP BY incident_address
ORDER BY count_of_requests DESC;

-- bar chart with yearly stats (possibly other type of complaints)
SELECT dim_date.year, COUNT(*)
FROM fact_service_request, dim_date
WHERE fact_service_request.created_date_key = dim_date.date_key
AND complaint_type_key = 4 AND descriptor = 'Tortured'
AND dim_date.dayofyear BETWEEN 271 AND 276
GROUP BY dim_date.year
ORDER BY dim_date.year;

-- geo map with interested zip code
SELECT DISTINCT incident_zip
FROM fact_service_request
WHERE created_date_key BETWEEN 20220928 AND 20221003
AND complaint_type_key = 4 AND descriptor = 'Tortured';

-- bar chart of complaint type with year avg 
SELECT c.complaint_type_name, COUNT(*)
FROM fact_service_request f
LEFT JOIN dim_complaint_type c
ON f.complaint_type_key = c.complaint_type_key
WHERE 
f.created_date_key BETWEEN 20220928 AND 20221003
AND 
f.incident_zip IN
(SELECT DISTINCT incident_zip
FROM fact_service_request
WHERE created_date_key BETWEEN 20220928 AND 20221003
AND complaint_type_key = 4 AND descriptor = 'Tortured')
GROUP BY c.complaint_type_name
ORDER BY count DESC;

-- table of detailed description
SELECT c.complaint_type_name, descriptor, COUNT(*)
FROM fact_service_request f
LEFT JOIN dim_complaint_type c
ON f.complaint_type_key = c.complaint_type_key
WHERE 
f.created_date_key BETWEEN 20220928 AND 20221003
AND 
f.incident_zip IN
(SELECT DISTINCT incident_zip
FROM fact_service_request
WHERE created_date_key BETWEEN 20220928 AND 20221003
AND complaint_type_key = 4 AND descriptor = 'Tortured')
GROUP BY c.complaint_type_name, descriptor
ORDER BY count DESC;

-- pie chart of resolution descriptions
SELECT resolution_description_1, COUNT(*)
FROM fact_service_request
WHERE created_date_key BETWEEN 20220928 AND 20221003
AND complaint_type_key = 4 AND descriptor = 'Tortured'
GROUP BY resolution_description_1;

SELECT * 
FROM fact_service_request 
WHERE resolution_description_1 = 'The Police Department responded to the complaint and took action to fix the condition.'
AND created_date_key BETWEEN 20220928 AND 20221003
AND complaint_type_key = 4 AND descriptor = 'Tortured'