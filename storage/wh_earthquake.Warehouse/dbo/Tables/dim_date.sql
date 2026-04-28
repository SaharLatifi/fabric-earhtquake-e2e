CREATE TABLE [dbo].[dim_date] (

	[date_key] bigint NULL, 
	[date] date NULL, 
	[year] float NULL, 
	[month_num] bigint NULL, 
	[month_name] varchar(8000) NULL, 
	[month_short_name] varchar(8000) NULL, 
	[northern_hemispher_season] varchar(8000) NULL, 
	[southern_hemoispher_season] varchar(8000) NULL
);