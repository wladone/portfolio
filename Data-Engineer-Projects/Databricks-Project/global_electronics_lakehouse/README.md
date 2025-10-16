# Global Electronics Lakehouse Dashboard

A modern, interactive dashboard for monitoring Global Electronics Lakehouse operations. This dashboard provides observability for revenue, inventory, and pipeline health across EU Databricks regions.

## Frontend Options

This project offers two dashboard implementations to suit different development preferences and deployment needs:

- **Streamlit Dashboard (Primary)**: Python-native dashboard built with Streamlit, recommended for production use in Databricks environments. Provides rapid development and seamless integration with Python data workflows.
- **React/TypeScript Dashboard (Legacy)**: Original implementation using React and TypeScript, available as an alternative for users preferring JavaScript-based frontends.

For detailed documentation on both implementations, including setup instructions, features comparison, and migration guidance, see [FRONTEND.md](FRONTEND.md).

## 🚀 Features

- **7 Main Views**: Overview, Inventory Management, Sales Analytics, Supplier Network, System Logs, Alerts & Notifications, Performance Metrics
- **Real-time Metrics**: Revenue, orders, inventory coverage, supplier reliability
- **Interactive Charts**: Built with Plotly for professional visualizations
- **Responsive Design**: Mobile-friendly layout with collapsible sidebar
- **Theme Support**: Light/dark mode toggle
- **Timeframe Selection**: 7d, 30d, 90d options for trend analysis
- **Monitoring & Observability**: Comprehensive system monitoring with logs, alerts, and performance metrics

## 📊 Dashboard Views

### Overview
- Hero card with revenue velocity metrics
- Key performance indicators (KPIs) with delta indicators
- Revenue vs orders trend chart
- Warehouse coverage and category allocation charts
- Pipeline health monitoring
- Reorder alerts and supplier reliability panels

### Inventory Management
- Inventory exposure metrics
- Warehouse distribution charts
- Category concentration analysis
- SKU details table with status indicators

### Sales Analytics
- Commercial momentum metrics
- Revenue pattern visualization
- Category share analysis
- Top performing days table

### Supplier Network
- Supplier ecosystem overview
- Performance rating charts
- Focus area distribution
- Supplier directory with status indicators

### System Logs
- Application and system log aggregation
- Log level filtering (ERROR, WARNING, INFO)
- Time-based filtering (Last Hour, Last 24 Hours, Last 7 Days)
- Log statistics and error tracking
- Full log details with expandable views

### Alerts & Notifications
- Real-time system alerts and notifications
- Alert severity levels and status tracking
- Pipeline health monitoring
- System resource alerts
- Alert management and acknowledgment

### Performance Metrics
- Query performance monitoring
- Pipeline runtime metrics
- System resource utilization (CPU, Memory, Disk)
- Cache hit rates and throughput
- Performance trends and benchmarks

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.8+
- pip package manager

### Installation

1. **Clone or navigate to the project directory:**
   ```bash
   cd global_electronics_lakehouse
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Streamlit application:**
   ```bash
   streamlit run dashboard/app.py
   ```

4. **Open your browser** to `http://localhost:8501`

## 📁 Project Structure

```
global_electronics_lakehouse/
├── dashboard/
│   └── app.py                          # Main Streamlit application
├── data/
│   ├── inventory/                      # Inventory CSV files
│   ├── sales_stream/                   # Sales JSON files
│   └── suppliers/                      # Supplier data
├── src/                               # Source code
├── tests/                             # Test files
├── bundle/                            # Databricks bundle config
├── notebooks_local/                   # Local PySpark notebooks
├── queries/                           # Dashboard SQL queries
├── resources/                         # Schema definitions
├── requirements.txt                   # Python dependencies
├── README.md                          # This documentation
├── LICENSE
└── .gitignore
```

## 🎨 Customization

### Themes
The dashboard supports light and dark themes. Theme selection is available in the sidebar and persists during the session.

### Timeframes
Select different timeframes (7d, 30d, 90d) to analyze trends over various periods. This affects all time-series charts and metrics.

### Data Models
The application uses dataclasses for type safety:

- `InventoryItem`: Product inventory data
- `SalesData`: Sales transaction data
- `Supplier`: Supplier information and ratings

## 📈 Data Sources

The dashboard uses mock data that simulates:
- 6 inventory products across 4 warehouses (FRA, BUC, MAD, AMS)
- 5 days of sales data with trend patterns
- 5 suppliers with ratings and focus areas
- 4 DLT pipeline statuses

## 🚀 Deployment

### Local Development
```bash
streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0
```

### Production Deployment
For production deployment, consider:
- Using Streamlit Cloud
- Docker containerization
- Integration with actual Databricks data sources

### Docker Deployment
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8501

CMD ["streamlit", "run", "dashboard/app.py", "--server.address", "0.0.0.0"]
```
## 🔄 CI/CD Pipeline

This project includes a comprehensive CI/CD pipeline that ensures code quality, security, and performance before deployment.

### Pipeline Features

- **Automated Testing**: Runs pytest with coverage reporting
- **Code Quality**: Linting with flake8, formatting checks with black and isort
- **Security Scanning**: Bandit for Python security issues, Safety for dependency vulnerabilities
- **Load Testing**: Locust-based performance testing with configurable thresholds
- **Containerization**: Docker build and push for consistent deployments
- **Multi-Environment**: Separate development and production deployments

### Pipeline Stages

1. **Lint**: Code style and formatting checks
2. **Test**: Unit and integration tests with coverage
3. **Security Scan**: Automated vulnerability detection
4. **Build**: Docker image creation and registry push
5. **Load Test**: Performance validation under simulated load
6. **Deploy**: Environment-specific deployments

### Performance Benchmarks

The pipeline enforces these performance thresholds:
- Response time (50th percentile): < 1000ms
- Response time (95th percentile): < 2000ms
- Response time (99th percentile): < 5000ms
- Minimum throughput: 10 requests/second
- Maximum failure rate: 5%

### Environment Configuration

- **Development**: Automatic deployment on pushes to `develop` branch
- **Production**: Deployment triggered by GitHub releases

### Required Secrets

Configure these GitHub repository secrets:
- `DATABRICKS_HOST`: Databricks workspace URL
- `DATABRICKS_TOKEN`: Databricks access token
- `DATABRICKS_HTTP_PATH`: Databricks SQL warehouse path
- `DOCKER_USERNAME`: Docker Hub username
- `DOCKER_PASSWORD`: Docker Hub password
- `DOCKER_REGISTRY`: Docker registry URL (optional)

## 🔍 Troubleshooting

## 🔍 Troubleshooting

### Common Issues

1. **Port already in use**: Change the port with `--server.port 8502`
2. **Dependencies not found**: Ensure all packages from `requirements.txt` are installed
3. **Charts not rendering**: Check Plotly version compatibility

### Monitoring & Alerts

1. **No logs displayed**: Check that `_logs/` directory exists and contains log files
2. **Alerts not updating**: Ensure data service connection is active and alerting system is configured
3. **Performance metrics not showing**: Verify system monitoring permissions and data collection is enabled
4. **High memory usage**: Consider increasing cache timeout or implementing data pagination for large datasets

### Performance
- The app uses session state for efficient re-renders
- Charts are optimized for real-time updates
- Data processing is memoized where possible
- Monitoring views include filtering to handle large datasets efficiently

### Alert Configuration

For production deployments, configure alerting channels in `config.py`:
- Email alerts: Set SMTP server and credentials
- Webhook alerts: Configure endpoint URLs
- Log aggregation: Ensure proper log file permissions

See [MONITORING.md](MONITORING.md) for detailed monitoring setup and troubleshooting guides.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is part of the Global Electronics Lakehouse demonstration and follows the same licensing as the main project.

## 🏢 About

Built for the Global Electronics Lakehouse deployment across EU Databricks regions. This Streamlit version provides the same functionality as the original React dashboard while providing a Python-native alternative for Databricks users who prefer Streamlit for rapid dashboard development.

---

**Note**: This Streamlit application replicates the functionality of the original React/TypeScript dashboard, providing a Python-native alternative for Databricks users who prefer Streamlit for rapid dashboard development.
