let
  FolderPath = FolderPath,  // from parameters.pq
  Source = ( #shared[GetCsv] )(FolderPath, "fact_sales.csv")
in
  Source