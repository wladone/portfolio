let
  FolderPath = FolderPath,  // from parameters.pq
  Source = ( #shared[GetCsv] )(FolderPath, "dim_region.csv")
in
  Source