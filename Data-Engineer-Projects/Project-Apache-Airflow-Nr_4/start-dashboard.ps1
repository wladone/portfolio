# Fashion Retail Dashboard Startup Script
Write-Host "Starting Fashion Retail Dashboard..."
cd fashion-retail-pipeline
cd showcase
Write-Host "Dashboard starting on http://localhost:8501"
streamlit run app.py --server.port 8501 --server.address 0.0.0.0