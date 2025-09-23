# Flight Delays ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for processing flight delays data into a dimensional model optimized for analytical queries and flight delay prediction.

## Project Overview

This project implements a dimensional modeling approach to store and analyze 2015 flight data from three CSV sources:
- **airlines.csv**: 14 airline carriers with IATA codes
- **airports.csv**: 300+ airports with geographic information
- **flights.csv**: 5.8M+ flight records with delay information

The pipeline transforms this data into a star schema with one fact table (`fact_flights`) and five dimension tables optimized for the eight analytical objectives defined in the project requirements.

## Architecture

### Dimensional Model
- **Fact Table**: `fact_flights` - Central table with flight metrics
- **Dimension Tables**:
  - `dim_airline` - Airline information and categorization
  - `dim_airport` - Airport details with geographic data
  - `dim_date` - Calendar dimension with business attributes
  - `dim_time` - Time dimension for departure analysis
  - `dim_delay_cause` - Categorized delay causes

### ETL Pipeline Components
```
├── src/
│   ├── extractors/        # CSV data extraction
│   ├── transformers/      # Data transformation logic
│   ├── loaders/          # MySQL database loading
│   ├── config/           # Configuration management
│   └── utils/            # Logging and utilities
├── run_etl.py           # Main pipeline orchestrator
├── flights_db.sql       # Database schema DDL
└── flights_views.sql    # Analytical views
```

## Prerequisites

### System Requirements
- Python 3.8+ 
- MySQL 8.0+ database server
- Minimum 8GB RAM (for processing 5.8M flight records)
- 2GB free disk space

### Database Setup
Ensure MySQL is running and accessible with your configured credentials. 


## Installation & Setup


### 1. Navigation 
```bash
# Go to lab1_flights direcroty
cd lab1_flights
# Create necessary folders
mkdir -p logs data
```
### 2. Data files 
All three .csv data files (airlines.csv, airports.csv, flights.csv) should be in lab1_flight/data folder. They are available in the [Kaggle](https://www.kaggle.com/code/fabiendaniel/predicting-flight-delays-tutorial/input).

### 3. Set Up Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 4. Configure Environment
In .env.example file following credencials should be replaced with yours:
- DATABASE_HOST
- DATABASE_PORT
- DATABASE_USER
- DATABASE_PASSWORD


```bash
# Copy environment template
cp .env.example .env
```

### 4. Verify Data Files
Ensure all CSV files are present in the `data/` directory:
```bash
ls -la data/
# Should show: airlines.csv, airports.csv, flights.csv
```

## Usage

### Basic Execution
Create database:
```bash
mysql -u username -pYourPassword < flights_db.sql
```
Run the complete ETL pipeline with default settings:
```bash
# With virtual environment activated
python run_etl.py

# Or use executable directly
./run_etl.py
```

### Command Line Options
```bash
# Skip database schema creation (if already exists)
python run_etl.py --skip-schema

# Skip analytical views creation
python run_etl.py --skip-views

# Skip data integrity validation
python run_etl.py --skip-validation

# Process only a sample of flights (for testing)
python run_etl.py --sample-size 10000

# Combine options
python run_etl.py --skip-schema --sample-size 50000
```

### Help
```bash
python run_etl.py --help
```

## Pipeline Stages

### 1. **Data Extraction**
- Validates CSV file existence and structure
- Reads data with proper data types and encoding
- Handles chunked processing for large datasets
- **Memory Management**: Processes flights.csv in 100K record batches

### 2. **Data Transformation**
**Dimensions:**
- Airlines: Categorizes by type (Major, Regional, Low-Cost)
- Airports: Adds timezone and size categorization
- Date: Creates business calendar (weekends, holidays, seasons)
- Time: Categorizes departure times into periods
- Delay Causes: Maps delay types to standardized categories

**Facts:**
- Cleans and validates flight records
- Resolves foreign keys to dimensions
- Calculates derived metrics (delay indicators, recovery times)
- Determines primary delay causes

### 3. **Data Loading**
- **Strategy**: Truncate and reload (full refresh)
- **Batch Processing**: Configurable batch sizes for performance
- **Transaction Management**: Rollback capability on failures
- **Foreign Key Resolution**: Automatic lookup table generation

### 4. **Post-Load Operations**
- Creates optimized indexes for query performance
- Generates 19 analytical views supporting all objectives
- Validates data integrity and referential constraints
- Provides comprehensive execution statistics


## Monitoring & Logging

### Log Locations
- **Console**: Real-time progress and status
- **File**: `logs/etl_pipeline.log` (detailed execution log)

### Progress Tracking
The pipeline provides detailed progress information:
- Step-by-step completion status
- Record processing counts and rates
- Performance metrics and timing
- Data quality warnings and statistics

### Error Handling
- **Graceful Degradation**: Continues processing despite non-critical errors
- **Data Quality Issues**: Logged as warnings with counts
- **System Errors**: Immediate failure with detailed error messages
- **Rollback**: Database transactions rolled back on failures

## Data Quality & Validation

### Automated Checks
- **Referential Integrity**: Validates all foreign key relationships
- **Data Completeness**: Reports missing critical fields
- **Range Validation**: Identifies extreme values (delays > 24hrs)
- **Consistency Checks**: Cross-field validation (arrival after departure)

### Known Data Quality Issues
- ~0.1% of flights have extreme delays (>24 hours)
- Some flights missing specific delay cause breakdown
- Occasional missing departure/arrival times for cancelled flights

## Troubleshooting

### Common Issues

**1. Database Connection Failed**
```bash
# Check MySQL service
sudo systemctl status mysql

# Test connection
mysql -u azalia2 -p123456 -h 127.0.0.1 -P 3306
```

**2. Memory Issues**
```bash
# Reduce batch size in .env
BATCH_SIZE=50000

# Or process smaller sample
python run_etl.py --sample-size 100000
```

**3. Missing CSV Files**
```bash
# Verify files exist and have correct permissions
ls -la data/
chmod 644 data/*.csv
```

**4. Import Errors**
```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

### Debug Mode
Enable detailed SQL logging by editing `src/loaders/mysql_loader.py`:
```python
self.engine = create_engine(
    settings.database_url,
    echo=True  # Enable SQL statement logging
)
```

## Analytical Capabilities

Once loaded, the dimensional model supports all eight analytical objectives:

### 1. **Carrier Performance Analysis**
```sql
-- Average delays by airline
SELECT a.airline_name, AVG(f.departure_delay_minutes) as avg_delay
FROM fact_flights f
JOIN dim_airline a ON f.airline_key = a.airline_key
GROUP BY a.airline_name
ORDER BY avg_delay DESC;
```

### 2. **Airport Congestion Impact**
```sql
-- Delays by origin airport
SELECT ap.airport_name, ap.city, COUNT(*) as flights, AVG(f.departure_delay_minutes) as avg_delay
FROM fact_flights f
JOIN dim_airport ap ON f.origin_airport_key = ap.airport_key
WHERE f.departure_delay_minutes > 0
GROUP BY ap.airport_key
ORDER BY avg_delay DESC;
```

### 3. **Weather Impact Assessment**
```sql
-- Weather-related delays by month
SELECT d.month_name, COUNT(*) as weather_delays, AVG(f.departure_delay_minutes) as avg_delay
FROM fact_flights f
JOIN dim_date d ON f.date_key = d.date
JOIN dim_delay_cause dc ON f.delay_cause_key = dc.delay_cause_key
WHERE dc.cause_category = 'Weather'
GROUP BY d.month, d.month_name
ORDER BY d.month;
```

## File Structure

```
lab1_flights/
├── README.md                          # This documentation
├── requirements.txt                   # Python dependencies
├── .env.example                       # Environment configuration template
├── .python-version                    # Python version specification
├── run_etl.py                         # Main ETL pipeline script
├── flights_db.sql                     # Database schema DDL
├── flights_views.sql                  # Analytical views
├── data/                              # Source CSV files
│   ├── airlines.csv
│   ├── airports.csv
│   └── flights.csv
├── venv/                              # Virtual environment
├── logs/                              # Execution logs (created at runtime)
├── src/                               # Source code
│   ├── config/
│   │   └── settings.py                # Configuration management
│   ├── extractors/
│   │   └── csv_extractor.py           # CSV data extraction
│   ├── transformers/
│   │   ├── dimension_transformer.py   # Dimensional data transformation
│   │   └── fact_transformer.py        # Fact table transformation
│   ├── loaders/
│   │   └── mysql_loader.py            # MySQL database loading
│   └── utils/
│       └── logging_config.py          # Logging configuration
└── reports/                           # Analysis reports
```


## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review log files in `logs/etl_pipeline.log`
3. Verify all prerequisites are met
4. Ensure data files are accessible and properly formatted
