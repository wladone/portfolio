@echo off
echo 🚀 Starting Fashion Retail Dashboard...
echo 📂 Navigating to project directory...
cd fashion-retail-pipeline
echo 📂 Navigating to showcase directory...
cd showcase
echo 🌐 Starting Streamlit dashboard on http://localhost:8501
echo 📊 Dashboard will show your fashion retail analytics data
echo 🔗 Access it at: http://localhost:8501
echo.
echo Press Ctrl+C to stop the dashboard
echo.
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
pause