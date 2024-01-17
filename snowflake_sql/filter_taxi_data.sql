use role taxi_data_admin;
use database new_york_taxi_data_project;
use schema raw_taxi_data;
use warehouse launchpad_xs;

-- create table for filtered data
create or replace table filtered_taxi_data_jul_to_sep(
    trip_distance_miles float,
    pickup_datetime timestamp_ntz,
    dropoff_datetime timestamp_ntz,
    trip_duration_seconds int,
    average_speed_mph float,
    total_amount_usd float,
    tip_amount_usd float,
    tip_percentage float
);

-- insert correctly filtered data into new table
insert into filtered_taxi_data_jul_to_sep(
    trip_distance_miles,
    pickup_datetime,
    dropoff_datetime,
    trip_duration_seconds,
    average_speed_mph,
    total_amount_usd,
    tip_amount_usd,
    tip_percentage
)
select
    trip_distance,
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    datediff(second, pickup_datetime, dropoff_datetime) as trip_duration,
    trip_distance / trip_duration * 3600,
    total_amount,
    tip_amount,
    100 * tip_amount / (total_amount - tip_amount)
from taxi_yellow
where pickup_datetime between '2023-07-01' and '2023-10-01'
and trip_duration > 0
and total_amount - tip_amount > 0
and total_amount > 0;

-- read filtered table
select * from filtered_taxi_data_jul_to_sep limit 10;

select
    distinct cast(pickup_datetime as date) as journey_date,
    count(*) as number_of_journeys,
    round(sum(trip_distance_miles)/count(*), 1) as average_distance_miles,
    round(sum(trip_duration_seconds)/count(*)/60, 1) as average_duration_minutes,
    round(sum(tip_percentage)/count(*), 1) as average_tip_percentage,
    round(sum(average_speed_mph)/count(*), 1) as average_speed_mph
from filtered_taxi_data_jul_to_sep
group by journey_date
order by journey_date;
