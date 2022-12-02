CREATE DATABASE requests;

CREATE TABLE dim_complaint_type(
    compliant_type_key INT NOT NULL,
    complaint_type_name VARCHAR(20)
);

CREATE TABLE fact_service_request(
    unique_key VARCHAR(12) NOT NULL,
    created_date_key INT,
    closed_date_key INT,
    compliant_type_key INT,
    incident_zip_key INT,
    address_type_key INT,
    descriptor VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    resolution_description VARCHAR(100),
    cross_street_1 VARCHAR(40),
    cross_street_2 VARCHAR(40),
    intersection_street_1 VARCHAR(40),
    intersection_street_2 VARCHAR(40)
);