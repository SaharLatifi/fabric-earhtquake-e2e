[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_date_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "date_key", DestinationColumnName = "date_key"], [SourceColumnName = "date", DestinationColumnName = "date"], [SourceColumnName = "year", DestinationColumnName = "year"], [SourceColumnName = "month_num", DestinationColumnName = "month_num"], [SourceColumnName = "month_name", DestinationColumnName = "month_name"], [SourceColumnName = "month_short_name", DestinationColumnName = "month_short_name"], [SourceColumnName = "northern_hemispher_season", DestinationColumnName = "northern_hemispher_season"], [SourceColumnName = "southern_hemoispher_season", DestinationColumnName = "southern_hemoispher_season"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_date = let
  Source = Csv.Document(Web.Contents("https://6x2gqg-my.sharepoint.com/personal/sahar_6x2gqg_onmicrosoft_com/Documents/Apps/Microsoft Power Query/Uploaded Files/dim_date.csv"), [Delimiter = ",", Columns = 18, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"DateNum", Int64.Type}, {"Date", type date}, {"YearMonthNum", Int64.Type}, {"Calendar_Quarter", type text}, {"MonthNum", Int64.Type}, {"MonthName", type text}, {"MonthShortName", type text}, {"WeekNum", Int64.Type}, {"DayNumOfYear", Int64.Type}, {"DayNumOfMonth", Int64.Type}, {"DayNumOfWeek", Int64.Type}, {"DayName", type text}, {"DayShortName", type text}, {"Quarter", Int64.Type}, {"YearQuarterNum", Int64.Type}, {"DayNumOfQuarter", Int64.Type}, {"NorthernHemispherSeason", type text}, {"SouthernHemispherSeason", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"DateNum", "date_key"}, {"Date", "date"}, {"MonthNum", "month_num"}, {"MonthName", "month_name"}, {"MonthShortName", "month_short_name"}, {"NorthernHemispherSeason", "northern_hemispher_season"}, {"SouthernHemispherSeason", "southern_hemoispher_season"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"YearMonthNum", "Calendar_Quarter", "WeekNum", "DayNumOfYear", "DayNumOfMonth", "DayNumOfWeek", "DayName", "DayShortName", "Quarter", "YearQuarterNum", "DayNumOfQuarter"}),
  #"Inserted year" = Table.AddColumn(#"Removed columns", "Year", each Date.Year([date]), type nullable number),
  #"Renamed columns 1" = Table.RenameColumns(#"Inserted year", {{"Year", "year"}}),
  #"Reordered columns" = Table.ReorderColumns(#"Renamed columns 1", {"date_key", "date", "year", "month_num", "month_name", "month_short_name", "northern_hemispher_season", "southern_hemoispher_season"})
in
  #"Reordered columns";
shared dim_date_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[displayName = "wh_earthquake"]}[Data],
  TableNavigation = Navigation_2{[Item = "dim_date", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
