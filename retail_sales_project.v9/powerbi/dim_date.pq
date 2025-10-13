let
    Source = Csv.Document(File.Contents("/mnt/data/retail_sales_project/data/processed/dim_date.csv"),[Delimiter=",", Columns=null, Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promote = Table.PromoteHeaders(Source, [PromoteAllScalars=true])
in
    Promote