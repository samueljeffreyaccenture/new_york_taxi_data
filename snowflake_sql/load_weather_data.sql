use role taxi_data_admin;
use database de_launchpad;
use warehouse launchpad_xs;

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
put file://weather_data/july.csv @DE_LAUNCHPAD.PUBLIC.%daily_weather_data;
copy into daily_weather_data from @%daily_weather_data file_format = (format_name = 'my_csv_format') pattern = '.*july.csv.gz';
put file://weather_data/august.csv @DE_LAUNCHPAD.PUBLIC.%daily_weather_data;
copy into daily_weather_data from @%daily_weather_data file_format = (format_name = 'my_csv_format') pattern = '.*august.csv.gz';
put file://weather_data/september.csv @DE_LAUNCHPAD.PUBLIC.%daily_weather_data;
copy into daily_weather_data from @%daily_weather_data file_format = (format_name = 'my_csv_format') pattern = '.*september.csv.gz';

-- view table data
select * from daily_weather_data;