use role taxi_data_admin;
use database new_york_taxi_data_project;
use schema raw_taxi_data;
use warehouse launchpad_xs;

-- create table for overall daily data
create or replace table daily_overall_data(
    date date,
    journey_count number,
    average_distance_miles float,
    average_duration_minutes float,
    average_speed_mph float,
    average_tip_percentage float,
    average_temperature_fahrenheit float,
    average_humidity_percentage float,
    average_windspeed_mph float,
    total_precipitation_inches float
);

insert into daily_overall_data(
    date,
    journey_count,
    average_distance_miles,
    average_duration_minutes,
    average_speed_mph,
    average_tip_percentage,
    average_temperature_fahrenheit,
    average_humidity_percentage,
    average_windspeed_mph,
    total_precipitation_inches
)
select
    journey_date as date,
    number_of_journeys,
    average_distance_miles,
    average_duration_minutes,
    average_speed_mph,
    average_tip_percentage,
    average_temperature,
    average_humidity,
    average_windspeed,
    total_precipitation
from daily_taxi_data
outer join daily_weather_data on journey_date = weather_date;

select * from daily_overall_data;
