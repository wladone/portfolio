
let
    FolderPath = Text.Combine({Text.From(Extension.Contents(".."))}, ""),  // placeholder for relative
    SourcePath = Folder.Files(FolderPath),
    GetCsv = (name as text) as table =>
        let
            File = Table.SelectRows(SourcePath, each Text.EndsWith([Name], name)){0}[Content],
            CSV = Csv.Document(File,[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
            Promoted = Table.PromoteHeaders(CSV, [PromoteAllScalars=true])
        in
            Promoted
in
    GetCsv
