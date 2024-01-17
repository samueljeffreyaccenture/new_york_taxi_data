use role useradmin;
create role if not exists taxi_data_admin;

use role accountadmin;
grant create integration on account to role taxi_data_admin;
grant create database on account to role taxi_data_admin;
grant create warehouse on account to role taxi_data_admin;
grant role taxi_data_admin to role accountadmin;

use role taxi_data_admin;
create database if not exists new_york_taxi_data_project;
use database new_york_taxi_data_project;
create schema if not exists raw_taxi_data;

-- create the storage integration
create or replace storage integration gcs_taxi
  type = external_stage
  storage_provider = 'gcs'
  enabled = true
  storage_allowed_locations = ('gcs://new-york-taxi-tlc-public/');

-- create the external stage in the raw_taxi_data schema
use database new_york_taxi_data_project;

use schema raw_taxi_data;

create or replace stage taxi_stage
url='gcs://new-york-taxi-tlc-public/'
storage_integration = gcs_taxi
file_format = (type = parquet);


-- check there's data in the stage
list @taxi_stage;

-- create an xs warehouse so we can run queries
create or replace warehouse launchpad_xs
warehouse_size = xsmall
auto_suspend = 600
auto_resume = True
initially_suspended = True;

use warehouse launchpad_xs;


-- explore the data in the stage
select $1 from @taxi_stage limit 1;


--create the table to load the data into
create or replace table taxi_yellow (
    vendorid int,                         
    tpep_pickup_datetime timestamp_ntz,     
    tpep_dropoff_datetime timestamp_ntz,    
    passenger_count int,                    
    trip_distance float,                    
    ratecodeid int,                         
    store_and_fwd_flag varchar,             
    pulocationid int,                       
    dolocationid int,                       
    payment_type int,                       
    fare_amount float,                      
    extra float,                            
    mta_tax float,                          
    tip_amount float,                       
    tolls_amount float,                     
    improvement_surcharge float,            
    total_amount float,                     
    congestion_surcharge float             
);

-- copy the data into the stage
copy into taxi_yellow from (
select
$1:VendorID,
to_timestamp($1:tpep_pickup_datetime::int, 6) as tpep_pickup_datetime,
to_timestamp($1:tpep_dropoff_datetime::int, 6) as tpep_dropoff_datetime,
$1:passenger_count,
$1:trip_distance,
$1:RatecodeID,
$1:store_and_fwd_flag,
$1:PULocationID,
$1:DOLocationID,
$1:payment_type,
$1:fare_amount,
$1:extra,
$1:mta_tax,
$1:tip_amount,
$1:tolls_amount,
$1:improvement_surcharge,
$1:total_amount,
$1:congestion_surcharge
from @taxi_stage/yellow_trips
);


-- take a look at your table
select * from taxi_yellow limit 10;