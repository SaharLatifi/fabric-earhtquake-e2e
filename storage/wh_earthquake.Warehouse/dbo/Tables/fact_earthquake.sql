CREATE TABLE [dbo].[fact_earthquake] (

	[event_id] varchar(100) NOT NULL, 
	[event_date] datetime2(0) NOT NULL, 
	[event_time] datetime2(0) NOT NULL, 
	[country_key] int NOT NULL, 
	[sig] int NOT NULL, 
	[mag] float NOT NULL, 
	[mag_category_key] int NOT NULL, 
	[sig_category_key] int NOT NULL, 
	[depth_category_key] int NOT NULL, 
	[is_tsunami] int NULL, 
	[lat] float NOT NULL, 
	[lon] float NOT NULL, 
	[depth] float NOT NULL, 
	[hemisphere] varchar(100) NOT NULL, 
	[updated_at] datetime2(0) NULL, 
	[ingested_at] datetime2(0) NULL, 
	[location] varchar(500) NULL
);


GO
ALTER TABLE [dbo].[fact_earthquake] ADD CONSTRAINT PK_fact_earthquake primary key NONCLUSTERED ([event_id]);