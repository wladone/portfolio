
// Create core relationships between fact_sales and dimensions
// Assumes tables/columns exist in the model (import CSVs first).

void EnsureRel(string fromTable, string fromCol, string toTable, string toCol, bool active=true) {
    var existing = Model.Relationships
        .Where(r => r.FromTable.Name == fromTable && r.FromColumn.Name == fromCol
                 && r.ToTable.Name == toTable   && r.ToColumn.Name   == toCol)
        .FirstOrDefault();
    if(existing == null) {
        var rel = Model.Relationships.Add($"{fromTable}_{fromCol}_to_{toTable}_{toCol}");
        rel.FromColumn = Model.Tables[fromTable].Columns[fromCol];
        rel.ToColumn   = Model.Tables[toTable].Columns[toCol];
        rel.CrossFilteringBehavior = CrossFilteringBehavior.OneDirection;
        rel.Cardinality = RelationshipEndCardinality.ManyToOne;
        rel.IsActive = active;
    } else {
        existing.IsActive = active;
    }
}

// Date -> Fact (Date to OrderDate)
EnsureRel("dim_date", "Date", "fact_sales", "OrderDate", true);

// Product -> Fact
EnsureRel("dim_product", "ProductID", "fact_sales", "ProductID", true);

// Customer -> Fact
EnsureRel("dim_customer", "CustomerID", "fact_sales", "CustomerID", true);

// Channel -> Fact
EnsureRel("dim_channel", "ChannelID", "fact_sales", "ChannelID", true);

// Region -> Fact
EnsureRel("dim_region", "RegionID", "fact_sales", "RegionID", true);

// Campaign -> Fact
EnsureRel("dim_campaign", "CampaignID", "fact_sales", "CampaignID", true);

// Optional: mark date table
try {
    var dt = Model.Tables["dim_date"];
    dt.SetDateTableAttributes(dt.Columns["Date"]);
} catch {}
