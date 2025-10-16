# Frontend Architecture Documentation

This document provides comprehensive information about the two frontend dashboard implementations available in the Global Electronics Lakehouse project.

## Overview

The project offers two dashboard implementations to cater to different development preferences and deployment scenarios:

- **Streamlit Dashboard**: Python-native implementation (Primary)
- **React/TypeScript Dashboard**: JavaScript-based implementation (Legacy)

Both implementations provide identical functionality for monitoring Global Electronics Lakehouse operations, including revenue tracking, inventory management, sales analytics, and supplier network oversight.

## Streamlit Dashboard (Primary)

### Description
The Streamlit dashboard is the recommended primary frontend for production use, particularly in Databricks environments. It provides a Python-native solution that integrates seamlessly with data science workflows and requires minimal setup.

### Key Features
- **4 Main Views**: Overview, Inventory Management, Sales Analytics, Supplier Network
- **Real-time Metrics**: Revenue, orders, inventory coverage, supplier reliability
- **Interactive Charts**: Built with Plotly for professional visualizations
- **Responsive Design**: Mobile-friendly layout with collapsible sidebar
- **Theme Support**: Light/dark mode toggle
- **Timeframe Selection**: 7d, 30d, 90d options for trend analysis
- **Python Integration**: Direct access to Python data processing libraries

### Setup Instructions

#### Prerequisites
- Python 3.8+
- pip package manager

#### Installation
1. Navigate to the project directory:
   ```bash
   cd global_electronics_lakehouse
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the Streamlit application:
   ```bash
   streamlit run dashboard/app.py
   ```

4. Open your browser to `http://localhost:8501`

#### Production Deployment
- **Streamlit Cloud**: Direct deployment from GitHub
- **Docker**: Use the provided Dockerfile for containerized deployment
- **Databricks Integration**: Seamless integration with Databricks notebooks and jobs

### When to Use
- Primary choice for Databricks users
- Teams preferring Python-based development
- Rapid prototyping and development
- Environments where Python is the primary language
- Production deployments requiring minimal infrastructure

## React/TypeScript Dashboard (Legacy)

### Description
The React/TypeScript dashboard is the original implementation, providing a modern JavaScript-based frontend. While still functional, it is considered legacy and primarily maintained for compatibility.

### Key Features
- **4 Main Views**: Overview, Inventory Management, Sales Analytics, Supplier Network
- **Real-time Metrics**: Revenue, orders, inventory coverage, supplier reliability
- **Interactive Charts**: Built with modern JavaScript charting libraries
- **Responsive Design**: Mobile-friendly layout
- **Theme Support**: Light/dark mode capabilities
- **Timeframe Selection**: Multiple timeframe options for analysis
- **TypeScript**: Type-safe development experience

### Setup Instructions

#### Prerequisites
- Node.js 16+
- npm or yarn package manager

#### Installation
1. Navigate to the frontend directory:
   ```bash
   cd global_electronics_lakehouse/frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Start the development server:
   ```bash
   npm run dev
   ```

4. Open your browser to the displayed local URL (typically `http://localhost:5173`)

#### Production Build
1. Build the application:
   ```bash
   npm run build
   ```

2. Serve the `dist` directory using a static server or deploy to a hosting platform

### When to Use
- Teams requiring extensive frontend customization
- JavaScript/TypeScript development environments
- Projects needing advanced UI/UX features
- Legacy systems requiring React integration
- Development teams more comfortable with JavaScript ecosystem

## Feature Comparison

| Feature | Streamlit | React/TypeScript |
|---------|-----------|------------------|
| Language | Python | JavaScript/TypeScript |
| Setup Complexity | Low | Medium |
| Customization | Moderate | High |
| Performance | Good | Excellent |
| Deployment | Simple | Flexible |
| Databricks Integration | Native | Requires API |
| Development Speed | Fast | Moderate |
| Maintenance | Low | Medium |

## Migration Guide

### Switching from React/TypeScript to Streamlit

If you're currently using the React dashboard and want to migrate to the Streamlit implementation, follow these steps:

#### 1. Environment Setup
- Ensure Python 3.8+ is installed
- Install requirements: `pip install -r requirements.txt`
- No Node.js dependencies required

#### 2. Code Migration Considerations
- **Data Fetching**: Streamlit uses Python functions instead of API calls
- **State Management**: Streamlit handles state automatically; no need for React state hooks
- **Routing**: Streamlit uses sidebar navigation instead of React Router
- **Styling**: Streamlit provides built-in theming; custom CSS not required

#### 3. Functional Equivalency
- All dashboard views are available with identical functionality
- Charts and visualizations use Plotly in both implementations
- Data models and business logic remain the same

#### 4. Deployment Migration
- Replace React build process with `streamlit run dashboard/app.py`
- Update deployment scripts to use Python instead of Node.js
- Consider Streamlit Cloud for simplified hosting

#### 5. Team Training
- Minimal training required for Python developers
- Focus on Streamlit-specific patterns (widgets, session state)
- Leverage existing Python skills for data processing

### Benefits of Migration
- **Simplified Deployment**: Single Python application
- **Better Databricks Integration**: Native Python environment
- **Reduced Complexity**: Fewer dependencies and build steps
- **Faster Development**: Rapid iteration with Streamlit's hot reload

### Rollback Plan
- Keep React implementation available during transition
- Test thoroughly in staging environment
- Maintain both implementations during migration period

## Troubleshooting

### Streamlit Issues
- **Port conflicts**: Use `streamlit run dashboard/app.py --server.port 8502`
- **Dependencies**: Ensure all packages in `requirements.txt` are installed
- **Charts not loading**: Verify Plotly version compatibility

### React Issues
- **Build failures**: Clear node_modules and reinstall: `rm -rf node_modules && npm install`
- **Port conflicts**: Update vite.config.ts port settings
- **TypeScript errors**: Run `npm run type-check` for detailed error information

## Contributing

When contributing to frontend implementations:
- Maintain feature parity between both dashboards
- Update this documentation for any new features
- Test changes in both implementations
- Follow the respective framework's best practices

## Future Considerations

- The Streamlit dashboard is the focus for new features and improvements
- React dashboard maintenance is minimal, primarily for bug fixes
- Consider migrating to Streamlit for long-term support and active development