let
  FolderPath = FolderPath,  // from parameters.pq
  Source = ( #shared[GetCsv] )(FolderPath, "dim_campaign.csv")
in
  Source