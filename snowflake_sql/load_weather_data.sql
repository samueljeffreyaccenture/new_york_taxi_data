use role taxi_data_admin;
use database new_york_taxi_data_project;
use schema raw_taxi_data;
use warehouse launchpad_xs;

create or replace stage weather_stage;

-- create staging for the csv files to go into
create or replace file format my_csv_format type = csv;
create or replace table daily_weather_data (
    weather_date date,
    average_temperature number(4, 1),
    average_humidity number(4, 1),
    average_windspeed number(4, 1),
    total_precipitation number(4, 2)
);

-- load daily data from monthly files into weather table
put file://weather_data/july.csv @raw_taxi_data.weather_stage;
copy into daily_weather_data from @weather_stage file_format = (format_name = 'my_csv_format') pattern = '.*july.csv.gz';
put file://weather_data/august.csv @raw_taxi_data.weather_stage;
copy into daily_weather_data from @weather_stage file_format = (format_name = 'my_csv_format') pattern = '.*august.csv.gz';
put file://weather_data/september.csv @raw_taxi_data.weather_stage;
copy into daily_weather_data from @weather_stage file_format = (format_name = 'my_csv_format') pattern = '.*september.csv.gz';

-- view table data
select * from daily_weather_data;