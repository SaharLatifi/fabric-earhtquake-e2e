CREATE TABLE [dbo].[dim_mag_category] (

	[mag_category_key] smallint NOT NULL, 
	[category_name] varchar(50) NOT NULL, 
	[min_value] float NOT NULL, 
	[max_value] float NULL
);


GO
ALTER TABLE [dbo].[dim_mag_category] ADD CONSTRAINT PK_dim_mag_category primary key NONCLUSTERED ([mag_category_key]);