CREATE TABLE [dbo].[dim_country] (

	[country_id] bigint NULL, 
	[country_name] varchar(8000) NULL, 
	[country_code] varchar(8000) NULL, 
	[region] varchar(8000) NULL, 
	[sub-region] varchar(8000) NULL, 
	[intermediate-region] varchar(8000) NULL, 
	[region-code] varchar(8000) NULL, 
	[sub-region-code] varchar(8000) NULL, 
	[intermediate-region-code] varchar(8000) NULL
);