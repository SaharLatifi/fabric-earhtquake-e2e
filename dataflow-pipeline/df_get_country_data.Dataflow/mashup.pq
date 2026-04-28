[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "country_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "country_id", DestinationColumnName = "country_id"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "country_code", DestinationColumnName = "country_code"], [SourceColumnName = "region", DestinationColumnName = "region"], [SourceColumnName = "sub-region", DestinationColumnName = "sub-region"], [SourceColumnName = "intermediate-region", DestinationColumnName = "intermediate-region"], [SourceColumnName = "region-code", DestinationColumnName = "region-code"], [SourceColumnName = "sub-region-code", DestinationColumnName = "sub-region-code"], [SourceColumnName = "intermediate-region-code", DestinationColumnName = "intermediate-region-code"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared country = let
  Source = Csv.Document(Web.Contents("https://6x2gqg-my.sharepoint.com/personal/sahar_6x2gqg_onmicrosoft_com/Documents/Apps/Microsoft Power Query/Uploaded Files/country.csv"), [Delimiter = ",", Columns = 12, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"country_id", Int64.Type}, {"country_name", type text}, {"country_code", type text}, {"alpha-3", type text}, {"country-code", Int64.Type}, {"iso_3166-2", type text}, {"region", type text}, {"sub-region", type text}, {"intermediate-region", type text}, {"region-code", type text}, {"sub-region-code", type text}, {"intermediate-region-code", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"alpha-3", "country-code", "iso_3166-2"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns", {"country_id", "country_code", "country_name", "region", "sub-region", "intermediate-region", "region-code", "sub-region-code", "intermediate-region-code"})
in
  #"Reordered columns";
shared country_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "bb1444cd-93a8-4c08-a21a-87ee9a1ca8ad"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ff092a76-b4ad-4ff9-80c6-4c30ba503591"]}[Data],
  TableNavigation = Navigation_2{[Item = "dim_country", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
