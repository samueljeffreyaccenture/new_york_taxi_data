use role taxi_data_admin;
use database de_launchpad;
use schema raw_taxi_data;
use warehouse launchpad_xs;

-- view one row to see columns
select * from taxi_yellow limit 1;

-- get average journey time in minutes
select sum(datediff(minute, tpep_pickup_datetime, tpep_dropoff_datetime))/count(*)
as average_journey_time_in_minutes
from taxi_yellow
limit 10;

-- get average passenger count
select sum(passenger_count)/count(*)
as average_passenger_count
from taxi_yellow;

-- get average trip distance
select sum(trip_distance)/count(*)
as average_trip_distance
from taxi_yellow;

-- get average tip amount
select sum(tip_amount)/count(*)
as average_tip_amount
from taxi_yellow;

-- get average tip percentage
select 100 * sum(tip_amount / (total_amount - tip_amount)) / count(*)
as average_tip_percentage
from taxi_yellow
where total_amount - tip_amount > 0;

-- get unique dates and number of journeys made on each
select cast(tpep_pickup_datetime as date)
-- select tpep_pickup_datetime
as unique_dates,
count(*)
as number_of_journeys
from taxi_yellow
group by unique_dates
order by unique_dates;

-- create new table for july to september
select count(*)
from taxi_yellow
where tpep_pickup_datetime;



-- explore
select congestion_surcharge,
count(*) as count
from taxi_yellow
group by congestion_surcharge;

