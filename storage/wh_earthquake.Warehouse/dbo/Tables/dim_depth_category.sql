CREATE TABLE [dbo].[dim_depth_category] (

	[depth_category_key] smallint NOT NULL, 
	[category_name] varchar(50) NOT NULL, 
	[min_value] float NOT NULL, 
	[max_value] float NULL
);


GO
ALTER TABLE [dbo].[dim_depth_category] ADD CONSTRAINT PK_dim_depth_category primary key NONCLUSTERED ([depth_category_key]);