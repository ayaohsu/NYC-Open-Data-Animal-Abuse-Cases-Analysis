-- 1. 2022 kaporos sites discovery
SELECT latitude, longitude, COUNT(*) AS count_of_the_location, ANY_VALUE(incident_address) AS incident_address
FROM fact_service_request
WHERE created_date_key BETWEEN 20220928 AND 20221004
AND complaint_type_key = 1 AND descriptor = 'Tortured'
GROUP BY latitude, longitude
HAVING count_of_the_location >= 10;

-- 2. 2014-2021 kaporos sites discovery
SELECT latitude, longitude, COUNT(*) AS count_of_the_location, ANY_VALUE(incident_address) AS incident_address
FROM fact_service_request r
LEFT JOIN dim_date d ON r.created_date_key = d.date_key
WHERE d.year < 2022
AND d.month = 9
AND complaint_type_key = 1 AND descriptor = 'Tortured'
GROUP BY latitude, longitude;

-- 3. count by complaint type
WITH kaporos_site_address AS
(SELECT latitude, longitude, COUNT(*) AS count_of_the_location, ANY_VALUE(incident_address) AS incident_address
FROM fact_service_request
WHERE created_date_key BETWEEN 20220928 AND 20221004
AND complaint_type_key = 1 AND descriptor = 'Tortured'
GROUP BY latitude, longitude
HAVING count_of_the_location >= 10)

SELECT c.complaint_type_name, f.descriptor, COUNT(*) AS count_of_descriptor, CONCAT(CONCAT(c.complaint_type_name, ', '), f.descriptor) AS complaint_type
FROM fact_service_request f
JOIN kaporos_site_address a ON f.incident_address = a.incident_address
LEFT JOIN dim_complaint_type c ON c.complaint_type_key = f.complaint_type_key
WHERE created_date_key BETWEEN 20220928 AND 20221004
GROUP BY c.complaint_type_name, f.descriptor
ORDER BY count_of_descriptor DESC;

-- 4. yearly stats of september
SELECT dim_date.year, COUNT(*)
FROM fact_service_request, dim_date
WHERE fact_service_request.created_date_key = dim_date.date_key
AND dim_date.month = 9
AND complaint_type_key = 1 AND descriptor = 'Tortured'
GROUP BY dim_date.year
ORDER BY dim_date.year;

-- 5. pie chart of resolution descriptions
SELECT resolution_description_1, COUNT(*)
FROM fact_service_request
WHERE created_date_key BETWEEN 20220928 AND 20221003
AND complaint_type_key = 1 AND descriptor = 'Tortured'
GROUP BY resolution_description_1;

-- 6. action rate by complaint type
WITH kaporos_site_address AS
(SELECT latitude, longitude, COUNT(*) AS count_of_the_location, ANY_VALUE(incident_address) AS incident_address
FROM fact_service_request
WHERE created_date_key BETWEEN 20220928 AND 20221004
AND complaint_type_key = 1 AND descriptor = 'Tortured'
GROUP BY latitude, longitude
HAVING count_of_the_location >= 10)

SELECT 
    c.complaint_type_name, 
    f.descriptor, COUNT(*) AS count_of_descriptor, 
    CONCAT(CONCAT(c.complaint_type_name, ', '), f.descriptor) AS complaint_type, 
    SUM(CASE WHEN resolution_description_1 LIKE '%took action%' THEN 1 ELSE 0 END) AS sum_of_actions,
    (sum_of_actions::FLOAT4/count_of_descriptor::FLOAT4*100)::DECIMAL(4,2) AS action_rate
FROM fact_service_request f
JOIN kaporos_site_address a ON f.incident_address = a.incident_address
LEFT JOIN dim_complaint_type c ON c.complaint_type_key = f.complaint_type_key
WHERE created_date_key BETWEEN 20220928 AND 20221004
GROUP BY c.complaint_type_name, f.descriptor
ORDER BY count_of_descriptor DESC;