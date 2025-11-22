# AI-Driven Water Stress Management in Plants

## Overview

This is an interactive Streamlit web application that predicts plant water stress levels using machine learning models. The application analyzes various environmental and nutritional parameters to classify plant health into three categories: Healthy, Moderate Stress, or High Stress.

**Current State:** The application is fully configured and running on Replit with all dependencies installed.

**Live URL:** Available via Replit webview on port 5000

## Recent Changes

**2025-11-13:** Initial Replit environment setup
- Configured Python 3.11 environment with all dependencies
- Set up Streamlit to run on 0.0.0.0:5000 for Replit compatibility
- Fixed deprecation warnings by replacing `use_container_width` with `width='stretch'`
- Created comprehensive .gitignore for Python and Node.js
- Configured deployment settings for Replit autoscale deployment
- Added Streamlit configuration file for proper hosting

**2025-11-13:** Big Data Analytics Extension Added
- Implemented complete Big Data Analytics layer for academic coursework
- Created custom MapReduce engine with multiprocessing support
- Built Hadoop HDFS simulator with block-based partitioning
- Developed Spark RDD simulator with transformations and actions
- Implemented NoSQL database wrapper using TinyDB
- Added real-time stream processing with sliding windows
- Created interactive Big Data dashboard (dashboard.py) on port 8501
- Generated PowerBI-ready exports in powerbi_exports/ directory
- Comprehensive documentation in README_BIGDATA.md

## Project Architecture

### Technology Stack
- **Frontend:** Streamlit (Python web framework)
- **ML Libraries:** scikit-learn, pandas, numpy
- **Visualization:** Plotly, matplotlib, seaborn
- **Styling:** Custom CSS with glassmorphism design
- **Big Data:** Hadoop HDFS simulation, Spark RDD, MapReduce, NoSQL (TinyDB), Stream Processing

### Project Structure
```
.
├── app.py                          # Main ML Streamlit application (port 5000)
├── dashboard.py                    # Big Data Analytics dashboard (port 8501)
├── components/
│   ├── charts.py                   # Plotly chart components
│   ├── layout.py                   # Layout components (header, footer)
│   └── ui_theme.py                 # Theme and UI primitives
├── models/                         # Trained ML models (7 different models)
│   ├── hybrid_(voting)_model.pkl
│   ├── gradient_boosting_model.pkl
│   ├── random_forest_model.pkl
│   ├── svm_model.pkl
│   ├── decision_tree_model.pkl
│   ├── knn_model.pkl
│   └── logistic_regression_model.pkl
├── bigdata_pipeline.py             # Hadoop & Spark simulation
├── mapreduce_engine.py             # Custom MapReduce implementation
├── nosql_db.py                     # NoSQL database wrapper (TinyDB)
├── stream_processor.py             # Real-time stream analytics
├── generate_exports.py             # Export generation script
├── hdfs_storage/                   # HDFS partitioned data
├── nosql_data/                     # NoSQL JSON documents
├── powerbi_exports/                # PowerBI-ready CSV exports
│   ├── cleaned_data.csv
│   ├── analytics_summary.csv
│   ├── correlations.csv
│   └── time_series.csv
├── assets/
│   └── theme.css                   # Custom CSS styling
├── scaler.pkl                      # Feature scaler
├── label_encoder.pkl               # Label encoder
├── model_metrics.json              # Model performance metrics
├── plant_health_data.csv           # Training dataset
├── requirements.txt                # Python dependencies
├── README_BIGDATA.md               # Big Data documentation
└── .streamlit/
    └── config.toml                 # Streamlit configuration

```

### Machine Learning Models
The application includes 7 different ML models:
1. Hybrid (Voting Ensemble)
2. Gradient Boosting
3. Random Forest
4. Support Vector Machine (SVM)
5. Decision Tree
6. K-Nearest Neighbors (KNN)
7. Logistic Regression

### Features
- **Real-time Prediction:** Input sensor data and get instant plant health predictions
- **Model Comparison:** Compare performance metrics across different ML models
- **Data Visualization:** Interactive charts showing dataset insights and correlations
- **Confidence Metrics:** Visual confidence gauges and probability distributions
- **Feature Importance:** See which sensor inputs matter most for predictions

### Input Parameters (11 features)
1. Soil Moisture
2. Ambient Temperature
3. Soil Temperature
4. Humidity
5. Light Intensity
6. Soil pH
7. Nitrogen Level
8. Phosphorus Level
9. Potassium Level
10. Chlorophyll Content
11. Electrochemical Signal

## Running the Applications

### Machine Learning Application (app.py)
The ML application is configured to run automatically on Replit. The workflow "Streamlit App" starts the application on port 5000.

**Manual Start:**
```bash
streamlit run app.py
```
The app will be available at http://0.0.0.0:5000

### Big Data Analytics Dashboard (dashboard.py)
Run the Big Data dashboard on a separate port:
```bash
streamlit run dashboard.py --server.port=8501 --server.address=0.0.0.0
```
The dashboard will be available at http://0.0.0.0:8501

**Dashboard Features:**
- Hadoop HDFS visualization
- Spark RDD analytics
- MapReduce job execution
- NoSQL database operations
- Real-time stream processing
- PowerBI export downloads

### Generate Big Data Exports
To regenerate all Big Data exports:
```bash
python generate_exports.py
```
This will create/update all files in powerbi_exports/

### Configuration
Streamlit is configured via `.streamlit/config.toml`:
- Port: 5000
- Host: 0.0.0.0 (required for Replit)
- CORS: Disabled for Replit proxy compatibility
- XSRF Protection: Disabled for Replit iframe

## Deployment

The application is configured for Replit's autoscale deployment:
- **Type:** Autoscale (stateless web app)
- **Command:** `streamlit run app.py --server.port=5000 --server.address=0.0.0.0 --server.headless=true`

To deploy, use Replit's publish feature.

## Team

**Team Members:**
- Kirtthan Duvvi (22BCE0061)
- Kushal Sharma (22BCE2561)
- Akshar Varma (22BCT0372)

**Project Guide:**
- Dr. Perepi Raja Rajeshwari

**Institution:** Vellore Institute of Technology - 2025

## Dependencies

### Python (requirements.txt)
- streamlit >= 1.38.0
- pandas >= 2.2.3
- numpy >= 2.1.2
- scikit-learn >= 1.5.0
- joblib >= 1.4.2
- plotly >= 5.24.1
- matplotlib >= 3.9.2
- seaborn >= 0.13.2
- kaleido == 0.2.1
- tinydb >= 4.8.0 (Big Data - NoSQL)
- pyarrow >= 14.0.0 (Big Data - Parquet files)

### Node.js (package.json)
- TailwindCSS and related packages (for potential future enhancements)

## Big Data Analytics

The project now includes a complete Big Data Analytics extension for academic coursework.

### Implemented Big Data Concepts
✅ **Hadoop HDFS**: Block-based distributed storage simulation  
✅ **Apache Spark**: RDD transformations and actions (map, filter, reduceByKey)  
✅ **MapReduce**: Custom implementation with map-shuffle-reduce phases  
✅ **NoSQL Database**: Document store using TinyDB with queries and aggregations  
✅ **Stream Processing**: Real-time analytics with sliding windows  
✅ **Data Exports**: PowerBI-ready datasets for business intelligence  

### Big Data Files
- `bigdata_pipeline.py` - Hadoop & Spark simulation  
- `mapreduce_engine.py` - MapReduce framework  
- `nosql_db.py` - NoSQL database wrapper  
- `stream_processor.py` - Stream analytics  
- `dashboard.py` - Interactive dashboard  
- `README_BIGDATA.md` - Comprehensive documentation with viva Q&A

See **README_BIGDATA.md** for complete Big Data documentation, examples, and viva preparation.

## Future Enhancements
- IoT-based automated irrigation system
- Mobile companion application
- Deep learning sensor fusion
- Kafka/Flink integration for production streaming
- Distributed cluster deployment
