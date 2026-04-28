[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_country_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "country_key", DestinationColumnName = "country_key"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "country_code", DestinationColumnName = "country_code"], [SourceColumnName = "region", DestinationColumnName = "region"], [SourceColumnName = "sub-region", DestinationColumnName = "sub-region"], [SourceColumnName = "intermediate-region", DestinationColumnName = "intermediate-region"], [SourceColumnName = "region-code", DestinationColumnName = "region-code"], [SourceColumnName = "sub-region-code", DestinationColumnName = "sub-region-code"], [SourceColumnName = "intermediate-region-code", DestinationColumnName = "intermediate-region-code"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_country = let
  Source = Csv.Document(Web.Contents("https://6x2gqg-my.sharepoint.com/personal/sahar_6x2gqg_onmicrosoft_com/Documents/Apps/Microsoft Power Query/Uploaded Files/dim_country.csv"), [Delimiter = ",", Columns = 12, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"country_id", Int64.Type}, {"country_name", type text}, {"country_code", type text}, {"alpha-3", type text}, {"country-code", Int64.Type}, {"iso_3166-2", type text}, {"region", type text}, {"sub-region", type text}, {"intermediate-region", type text}, {"region-code", type text}, {"sub-region-code", type text}, {"intermediate-region-code", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"country_id", "country_key"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"alpha-3", "country-code", "iso_3166-2"})
in
  #"Removed columns";
shared dim_country_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[displayName = "wh_earthquake"]}[Data],
  TableNavigation = Navigation_2{[Item = "dim_country", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
