# 🛒 E-Commerce Data Project

A comprehensive Scala/Spark data processing pipeline with modern web dashboard for e-commerce analytics.

## 📋 Project Overview

This project demonstrates a complete data processing pipeline including:
- **API Data Ingestion** from public e-commerce APIs
- **Spark Batch ETL** processing (Bronze → Silver layers)
- **Real-time Streaming** analytics
- **Modern Web Dashboard** with interactive visualizations
- **Data Quality** monitoring and metrics

## 🚀 Quick Start

### Option 1: Interactive Launcher (Recommended)
```cmd
launch_project.bat
```
Choose from:
- **[1] Full Pipeline Demo** - Complete data processing showcase
- **[2] Dashboard Only** - View results in modern web interface
- **[3] Server Restart** - Fix port conflicts and restart server

### Option 2: PowerShell Scripts
```powershell
# Full pipeline demonstration
.\scripts\run_ecommerce_showcase.ps1

# Dashboard only (after data processing)
.\start_dashboard.ps1
```

### Option 3: Manual Control
```cmd
# Start dashboard server
restart_server.bat

# Or use Python directly
python -m http.server 8000
```

## 🌐 Dashboard Access

Once the server is running, access the dashboard at:
- **Main Page**: `http://localhost:8000/`
- **Dashboard**: `http://localhost:8000/test_dashboard.html`

## 📁 Project Structure

```
project-root/
├── 🚀 LAUNCHERS
│   ├── launch_project.bat          # Interactive menu launcher
│   ├── start_dashboard.ps1         # Dashboard server starter
│   ├── start_dashboard.bat         # Windows dashboard launcher
│   └── restart_server.bat           # Server restart utility
│
├── 📊 DASHBOARD
│   ├── index.html                  # Auto-redirect to dashboard
│   ├── test_dashboard.html         # Main dashboard file
│   └── DASHBOARD_README.md         # Dashboard documentation
│
├── ⚡ SCRIPTS
│   ├── scripts/
│   │   ├── run_ecommerce_showcase.ps1    # Full pipeline demo
│   │   ├── run_ecommerce_showcase.bat    # Windows pipeline demo
│   │   └── ecom_download.py              # Python fallback
│
├── 🔧 CONFIGURATION
│   ├── build.sbt                   # Scala build configuration
│   ├── project/plugins.sbt          # SBT plugins
│   └── cassandra/                  # Database schemas
│
├── 📦 SOURCE CODE
│   └── src/main/scala/             # Scala application code
│
└── 📈 DATA
    ├── data/ecommerce/raw/         # Raw ingested data
    ├── data/ecommerce/silver/      # Processed Parquet data
    └── data/ecommerce/orders_incoming/  # Generated orders
```

## 🎯 Usage Scenarios

### Scenario 1: Full Pipeline Demo
```powershell
.\scripts\run_ecommerce_showcase.ps1
```
**What it does:**
- Compiles the Scala project
- Ingests data from DummyJSON API
- Processes data through Spark ETL pipeline
- Generates synthetic order data
- Runs streaming analytics
- Shows comprehensive results

### Scenario 2: Dashboard Only
```powershell
.\start_dashboard.ps1
```
**What it does:**
- Starts Python HTTP server on port 8000
- Serves all project files (HTML, CSS, JS, data)
- Auto-opens browser to modern dashboard
- Displays interactive charts and metrics

### Scenario 3: Development Mode
```cmd
# Terminal 1: Start dashboard server
restart_server.bat

# Terminal 2: Run data processing
.\scripts\run_ecommerce_showcase.ps1
```

## 🔧 Troubleshooting

### Port 8000 Issues
```cmd
# Kill conflicting processes
restart_server.bat
```

### SBT Compilation Issues
```powershell
# Clean and restart
taskkill /F /IM sbt.exe /T 2>$null
Remove-Item .bloop -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item target -Recurse -Force -ErrorAction SilentlyContinue
sbt clean compile
```

### Python Not Found
```cmd
# Check if Python is installed
python --version
# If not found, install Python 3.x from python.org
```

## 📊 Dashboard Features

### Visual Design
- **Glassmorphism UI** with backdrop blur effects
- **Dark Mode Toggle** with theme persistence
- **Responsive Layout** for all screen sizes
- **Smooth Animations** and micro-interactions

### Data Visualizations
- **Price Distribution** (Doughnut chart)
- **Rating Distribution** (Bar chart)
- **Sales Trend** (Line chart)
- **Revenue by Category** (Horizontal bar chart)
- **Geographic Distribution** (Polar area chart)

### Key Metrics
- Total Products, Brands, Categories
- Average Price and Growth Rate
- Response Time and User Satisfaction
- Data Accuracy and System Uptime

## 🏗️ Architecture

### Data Pipeline Layers
1. **Bronze Layer**: Raw API data (NDJSON format)
2. **Silver Layer**: Processed and cleaned data (Parquet format)
3. **Analytics Layer**: Aggregated metrics and KPIs

### Technology Stack
- **Backend**: Scala + Apache Spark + Akka HTTP
- **Frontend**: HTML5 + CSS3 + JavaScript + Chart.js
- **Data Storage**: Parquet files + Apache Cassandra (optional)
- **Streaming**: Spark Structured Streaming

## 🎨 Customization

### Dashboard Theming
Edit CSS variables in `test_dashboard.html`:
```css
:root {
  --primary-color: #6366f1;
  --glass-bg: rgba(255, 255, 255, 0.25);
  --transition: all 0.2s ease;
}
```

### Data Sources
Modify API endpoints in the Scala source code:
```scala
case object DummyJsonSource extends DataSource {
  val baseUrl = "https://dummyjson.com"
  // ... endpoint configuration
}
```

## 📈 Sample Data

The dashboard includes realistic sample data for:
- **1,247+ products** across 24 categories
- **89 brands** with pricing from $10 to $500+
- **Rating distributions** and sales trends
- **Geographic distribution** data

## 🔍 Monitoring

### Real-time Metrics
- Data freshness indicators
- System uptime tracking
- Query performance metrics
- Data quality scores

### Log Files
- SBT build logs: `target/streams/`
- Application logs: Console output
- Server logs: Terminal output

## 🚨 Common Issues

### Issue: "Port already in use"
**Solution:**
```cmd
restart_server.bat
```

### Issue: "SBT server lock"
**Solution:**
```powershell
taskkill /F /IM sbt.exe /T 2>$null
Remove-Item .bloop -Recurse -Force -ErrorAction SilentlyContinue
```

### Issue: "Python not found"
**Solution:**
- Install Python 3.x from [python.org](https://python.org)
- Ensure it's in your system PATH

## 📞 Support

For issues or questions:
1. Check the troubleshooting section above
2. Verify Python installation (`python --version`)
3. Ensure port 8000 is not blocked by firewall
4. Check that you're running from the project root directory

---

**🎉 Enjoy your E-Commerce Data Pipeline Project!**