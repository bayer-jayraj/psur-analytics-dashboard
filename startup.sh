#!/bin/bash
# Install system dependencies if needed
# Start Streamlit
python -m streamlit run app.py --server.port $PORT --server.address 0.0.0.0 --server.headless true