let
  FolderPath = FolderPath,  // from parameters.pq
  Source = ( #shared[GetCsv] )(FolderPath, "dim_customer.csv")
in
  Source