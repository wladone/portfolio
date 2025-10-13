let
  FolderPath = FolderPath,  // from parameters.pq
  Source = ( #shared[GetCsv] )(FolderPath, "dim_product.csv")
in
  Source