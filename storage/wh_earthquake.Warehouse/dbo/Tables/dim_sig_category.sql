CREATE TABLE [dbo].[dim_sig_category] (

	[sig_category_key] smallint NOT NULL, 
	[category_name] varchar(50) NOT NULL, 
	[min_value] int NOT NULL, 
	[max_value] int NULL, 
	[sort_order] smallint NULL
);


GO
ALTER TABLE [dbo].[dim_sig_category] ADD CONSTRAINT PK_dim_sig_category primary key NONCLUSTERED ([sig_category_key]);