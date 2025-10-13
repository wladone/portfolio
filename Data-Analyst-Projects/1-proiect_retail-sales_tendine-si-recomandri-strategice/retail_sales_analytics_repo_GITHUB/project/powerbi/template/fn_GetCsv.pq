
let
  GetCsv = (FolderPath as text, FileName as text) as table =>
    let
      Source = Folder.Files(FolderPath),
      Match = Table.SelectRows(Source, each [Name] = FileName),
      Bin = if Table.IsEmpty(Match) then error "File not found: " & FileName else Match{0}[Content],
      Csv = Csv.Document(Bin, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
      Promote = Table.PromoteHeaders(Csv, [PromoteAllScalars=true])
    in
      Promote
in
  GetCsv
