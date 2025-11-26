[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "2fe9abb6-9a56-4088-8062-472540353376"]}[Data]{[lakehouseId = "e8d4a3cf-987f-49a4-80c0-995473dc8732"]}[Data];
[BindToDefaultDestination = true]
shared customer_subscriptions = let
  Source = Csv.Document(Web.Contents("https://dreamitcs2020-my.sharepoint.com/personal/aryan_dreamitcs_com/Documents/Apps/Microsoft Power Query/Uploaded Files/customer_subscriptions.csv"), [Delimiter = ",", Columns = 5, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"CustomerID", Int64.Type}, {"SubscriptionDate", type date}, {"ServiceTier", type text}, {"MonthlyRevenue", type number}, {"CountryCode", type text}})
in
  #"Changed column type";
