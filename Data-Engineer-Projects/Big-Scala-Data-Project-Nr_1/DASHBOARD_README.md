# 🛒 E-Commerce Analytics Dashboard

A modern, interactive web dashboard for analyzing e-commerce product data with beautiful visualizations and real-time insights.

## ✨ Features

- **Modern Design**: Glassmorphism UI with smooth animations
- **Dark Mode**: Toggle between light and dark themes
- **Interactive Charts**: 5 different chart types with sample data
- **Performance Metrics**: Real-time KPI tracking
- **Responsive**: Works on all devices and screen sizes
- **Fast Loading**: Optimized for performance

## 🚀 Quick Start

### Option 1: PowerShell Script (Recommended)
```powershell
.\start_dashboard.ps1
```
This will automatically start the server and open your browser to the dashboard.

### Option 2: Batch File
```cmd
start_dashboard.bat
```
Double-click this file to start the dashboard.

### Option 3: Manual Start
```bash
python -m http.server 8000
```
Then open `http://localhost:8000/` in your browser.

## 📊 Dashboard Features

### Charts & Visualizations
- **Price Distribution**: Doughnut chart showing price ranges
- **Rating Distribution**: Bar chart of product ratings
- **Sales Trend**: Line chart with monthly sales data
- **Revenue by Category**: Horizontal bar chart
- **Geographic Distribution**: Polar area chart

### Key Metrics
- Total Products, Brands, Categories
- Average Price and Growth Rate
- Response Time and User Satisfaction
- Data Accuracy and System Uptime

### Interactive Elements
- **Dark Mode Toggle**: Click the moon/sun icon
- **Theme Persistence**: Remembers your preference
- **Hover Effects**: Cards and charts respond to interaction
- **Loading States**: Smooth transitions and feedback

## 🛠️ Technical Details

- **Frontend**: HTML5, CSS3, JavaScript (ES6+)
- **Charts**: Chart.js library
- **Styling**: Custom CSS with CSS Variables
- **Server**: Python HTTP Server (built-in)
- **Responsive**: Mobile-first design approach

## 📁 Project Structure

```
project-root/
├── index.html              # Auto-redirect to dashboard
├── test_dashboard.html     # Main dashboard file
├── start_dashboard.ps1     # PowerShell launcher
├── start_dashboard.bat     # Windows batch launcher
└── DASHBOARD_README.md     # This file
```

## 🎨 Customization

The dashboard uses CSS custom properties (variables) for easy theming:

```css
:root {
  --primary-color: #6366f1;
  --glass-bg: rgba(255, 255, 255, 0.25);
  --transition: all 0.2s ease;
}
```

## 🐛 Troubleshooting

### Server Won't Start
- Ensure Python is installed (`python --version`)
- Check if port 8000 is available
- Try `python3` instead of `python`

### Dashboard Not Loading
- Clear browser cache (Ctrl+F5)
- Try incognito/private mode
- Check firewall settings

### Performance Issues
- Close other browser tabs
- Disable browser extensions temporarily
- Try a different browser

## 📈 Sample Data

The dashboard includes realistic sample data for:
- 1,247 products across 24 categories
- 89 brands with pricing from $10 to $500+
- Rating distributions and sales trends
- Geographic distribution data

## 🔧 Development

To modify the dashboard:
1. Edit `test_dashboard.html`
2. Add new chart data in the JavaScript section
3. Customize colors in the CSS `:root` variables
4. Test changes by refreshing the browser

## 📞 Support

For issues or questions:
- Check the troubleshooting section above
- Verify Python installation
- Ensure port 8000 is not blocked by firewall

---

**Enjoy your modern E-Commerce Analytics Dashboard! 🎉**