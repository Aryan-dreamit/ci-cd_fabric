[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "2fe9abb6-9a56-4088-8062-472540353376"]}[Data]{[lakehouseId = "45cf811a-6020-4c2c-8dab-eb53a8aa7511"]}[Data];
[BindToDefaultDestination = true]
shared unstructured_data = let
  Source = Excel.Workbook(Web.Contents("https://dreamitcs2020-my.sharepoint.com/personal/anuj_dreamitcs_com/Documents/Apps/Microsoft Power Query/Uploaded Files/unstructured_data_unclean.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "unstructured_data_unclean", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true])
in
  #"Promoted headers";
