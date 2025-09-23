-- =============================================================================
-- FLIGHTS DATABASE ANALYTICAL VIEWS
-- =============================================================================
-- 
-- This script creates analytical views for flight delays prediction and analysis
-- supporting the 8 key analytical objectives:
-- 1. Carrier Performance Analysis
-- 2. Airport Congestion Impact  
-- 3. Route Optimization
-- 4. Temporal Pattern Recognition
-- 5. Weather Impact Assessment
-- 6. Root Cause Analysis
-- 7. Severity Classification
-- 8. Disruption Prediction
--
-- Prerequisites: Execute flights_db.sql first to create the base schema
-- Usage: Execute this script after data has been loaded into the fact and dimension tables
--
-- =============================================================================

USE flights_db;

-- Drop existing views if they exist
DROP VIEW IF EXISTS v_executive_dashboard;
DROP VIEW IF EXISTS v_flight_delay_features;
DROP VIEW IF EXISTS v_historical_delay_trends;
DROP VIEW IF EXISTS v_peak_congestion_times;
DROP VIEW IF EXISTS v_airline_rankings;
DROP VIEW IF EXISTS v_top_delayed_routes;
DROP VIEW IF EXISTS v_hourly_delay_patterns;
DROP VIEW IF EXISTS v_airport_daily_metrics;
DROP VIEW IF EXISTS v_airline_monthly_summary;
DROP VIEW IF EXISTS v_disruption_analysis;
DROP VIEW IF EXISTS v_severity_classification;
DROP VIEW IF EXISTS v_root_cause_analysis;
DROP VIEW IF EXISTS v_weather_impact;
DROP VIEW IF EXISTS v_temporal_patterns;
DROP VIEW IF EXISTS v_route_optimization;
DROP VIEW IF EXISTS v_airport_congestion;
DROP VIEW IF EXISTS v_carrier_performance;

-- =============================================================================
-- CORE ANALYTICAL VIEWS - Supporting 8 Primary Objectives
-- =============================================================================

-- -----------------------------------------------------------------------------
-- VIEW 1: Carrier Performance Analysis
-- Purpose: Compare airline punctuality and operational efficiency
-- Usage: Airline benchmarking, performance monitoring, contract negotiations
-- -----------------------------------------------------------------------------
CREATE VIEW v_carrier_performance AS
SELECT 
    da.airline_code,
    da.airline_name,
    da.airline_type,
    dd.year,
    dd.quarter,
    dd.month,
    
    -- Flight Volume Metrics
    COUNT(*) as total_flights,
    SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END) as completed_flights,
    SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) as cancelled_flights,
    SUM(CASE WHEN ff.is_diverted = 1 THEN 1 ELSE 0 END) as diverted_flights,
    
    -- Delay Performance Metrics
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) as avg_departure_delay,
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.arrival_delay_minutes END) as avg_arrival_delay,
    
    -- On-Time Performance (<=15 minutes delay)
    SUM(CASE WHEN ff.departure_delay_minutes <= 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) as ontime_departures,
    ROUND(
        SUM(CASE WHEN ff.departure_delay_minutes <= 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
        SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 2
    ) as ontime_departure_rate,
    
    -- Delay Categories
    SUM(CASE WHEN ff.departure_delay_minutes BETWEEN 16 AND 60 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) as moderate_delays,
    SUM(CASE WHEN ff.departure_delay_minutes > 60 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) as significant_delays,
    
    -- Cancellation Rate
    ROUND(SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cancellation_rate,
    
    -- Operational Efficiency
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.actual_elapsed_minutes END) as avg_actual_flight_time,
    AVG(ff.scheduled_elapsed_minutes) as avg_scheduled_flight_time,
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.taxi_out_minutes END) as avg_taxi_out_time,
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.taxi_in_minutes END) as avg_taxi_in_time

FROM fact_flights ff
JOIN dim_airline da ON ff.airline_key = da.airline_key
JOIN dim_date dd ON ff.date_key = dd.date_key
GROUP BY da.airline_key, da.airline_code, da.airline_name, da.airline_type, dd.year, dd.quarter, dd.month;

-- -----------------------------------------------------------------------------
-- VIEW 2: Airport Congestion Impact
-- Purpose: Analyze delay patterns at origin and destination airports
-- Usage: Infrastructure planning, capacity management, slot allocation
-- -----------------------------------------------------------------------------
CREATE VIEW v_airport_congestion AS
SELECT 
    dap.airport_code,
    dap.airport_name,
    dap.city,
    dap.state,
    dap.airport_size_category,
    dd.year,
    dd.quarter,
    dd.month,
    
    -- Departure Traffic Metrics
    COUNT(*) as total_departures,
    SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END) as completed_departures,
    
    -- Departure Delay Performance
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) as avg_departure_delay,
    SUM(CASE WHEN ff.departure_delay_minutes > 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) as delayed_departures,
    ROUND(
        SUM(CASE WHEN ff.departure_delay_minutes > 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
        SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 2
    ) as departure_delay_rate,
    
    -- Ground Operations Impact
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.taxi_out_minutes END) as avg_taxi_out_time,
    MAX(CASE WHEN ff.is_cancelled = 0 THEN ff.taxi_out_minutes END) as max_taxi_out_time,
    
    -- Severe Delay Analysis
    SUM(CASE WHEN ff.departure_delay_minutes > 60 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) as severe_delays,
    SUM(CASE WHEN ff.departure_delay_minutes > 180 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) as extreme_delays,
    
    -- Cancellation Impact
    SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) as cancelled_departures,
    ROUND(SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cancellation_rate,
    
    -- Peak Hour Analysis
    COUNT(CASE WHEN dt.hour BETWEEN 6 AND 9 THEN 1 END) as morning_rush_flights,
    COUNT(CASE WHEN dt.hour BETWEEN 17 AND 20 THEN 1 END) as evening_rush_flights,
    AVG(CASE WHEN dt.hour BETWEEN 6 AND 9 AND ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) as morning_rush_avg_delay,
    AVG(CASE WHEN dt.hour BETWEEN 17 AND 20 AND ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) as evening_rush_avg_delay

FROM fact_flights ff
JOIN dim_airport dap ON ff.origin_airport_key = dap.airport_key
JOIN dim_date dd ON ff.date_key = dd.date_key
LEFT JOIN dim_time dt ON ff.departure_time_key = dt.time_key
GROUP BY dap.airport_key, dap.airport_code, dap.airport_name, dap.city, dap.state, dap.airport_size_category, dd.year, dd.quarter, dd.month
HAVING COUNT(*) >= 10; -- Only include airports with meaningful flight volume

-- -----------------------------------------------------------------------------
-- VIEW 3: Route Optimization
-- Purpose: Identify high-risk city pairs and route-specific delays
-- Usage: Route planning, pricing strategies, passenger advisories
-- -----------------------------------------------------------------------------
CREATE VIEW v_route_optimization AS
SELECT 
    CONCAT(dao.airport_code, '-', dad.airport_code) as route_code,
    dao.city as origin_city,
    dao.state as origin_state,
    dad.city as destination_city,
    dad.state as destination_state,
    ff.distance_miles,
    dd.year,
    dd.quarter,
    
    -- Route Traffic Analysis
    COUNT(*) as total_flights,
    COUNT(DISTINCT da.airline_code) as airlines_serving_route,
    SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END) as completed_flights,
    
    -- Route Performance Metrics
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) as avg_departure_delay,
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.arrival_delay_minutes END) as avg_arrival_delay,
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.actual_elapsed_minutes END) as avg_flight_time,
    AVG(ff.scheduled_elapsed_minutes) as scheduled_flight_time,
    
    -- Reliability Metrics
    ROUND(
        SUM(CASE WHEN ff.departure_delay_minutes <= 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
        SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 2
    ) as ontime_performance_rate,
    
    SUM(CASE WHEN ff.departure_delay_minutes > 60 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) as significant_delays,
    ROUND(SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cancellation_rate,
    
    -- Route Efficiency
    ROUND(AVG(ff.distance_miles) / NULLIF(AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.air_time_minutes END), 0) * 60, 0) as avg_speed_mph,
    
    -- Seasonal Patterns
    AVG(CASE WHEN dd.season = 'Winter' AND ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) as winter_avg_delay,
    AVG(CASE WHEN dd.season = 'Summer' AND ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) as summer_avg_delay,
    
    -- Delay Impact Score (weighted by flight volume and delay severity)
    ROUND(
        (AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) * COUNT(*)) / 1000, 2
    ) as delay_impact_score

FROM fact_flights ff
JOIN dim_airport dao ON ff.origin_airport_key = dao.airport_key
JOIN dim_airport dad ON ff.destination_airport_key = dad.airport_key
JOIN dim_airline da ON ff.airline_key = da.airline_key
JOIN dim_date dd ON ff.date_key = dd.date_key
GROUP BY dao.airport_code, dao.city, dao.state, dad.airport_code, dad.city, dad.state, ff.distance_miles, dd.year, dd.quarter
HAVING COUNT(*) >= 50; -- Only routes with significant traffic

-- -----------------------------------------------------------------------------
-- VIEW 4: Temporal Pattern Recognition
-- Purpose: Understand delay patterns by time, day, and season
-- Usage: Schedule optimization, capacity planning, operational staffing
-- -----------------------------------------------------------------------------
CREATE VIEW v_temporal_patterns AS
SELECT 
    dd.year,
    dd.quarter,
    dd.month,
    dd.day_name,
    dd.day_of_week,
    dd.season,
    dd.is_weekend,
    dd.is_holiday,
    dt.hour,
    dt.hour_category,
    dt.time_period,
    dt.is_business_hours,
    
    -- Flight Volume by Time Periods
    COUNT(*) as total_flights,
    SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END) as completed_flights,
    
    -- Delay Performance by Time
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) as avg_departure_delay,
    STDDEV(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) as delay_variability,
    
    -- Time-based Delay Rates
    SUM(CASE WHEN ff.departure_delay_minutes > 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) as delayed_flights,
    ROUND(
        SUM(CASE WHEN ff.departure_delay_minutes > 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 0), 2
    ) as delay_rate,
    
    -- Severe Delay Analysis
    SUM(CASE WHEN ff.departure_delay_minutes > 60 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) as severe_delays,
    SUM(CASE WHEN ff.departure_delay_minutes > 180 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) as extreme_delays,
    
    -- Cancellation Patterns
    SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) as cancelled_flights,
    ROUND(SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cancellation_rate,
    
    -- Weather Impact by Time
    SUM(CASE WHEN ff.weather_delay_minutes > 0 THEN 1 ELSE 0 END) as weather_affected_flights,
    AVG(CASE WHEN ff.weather_delay_minutes > 0 THEN ff.weather_delay_minutes END) as avg_weather_delay,
    
    -- Peak vs Off-Peak Analysis
    CASE 
        WHEN dt.hour BETWEEN 6 AND 9 THEN 'Morning Peak'
        WHEN dt.hour BETWEEN 17 AND 20 THEN 'Evening Peak'
        WHEN dt.hour BETWEEN 10 AND 16 THEN 'Midday'
        WHEN dt.hour BETWEEN 21 AND 23 THEN 'Late Evening'
        ELSE 'Overnight'
    END as traffic_period

FROM fact_flights ff
JOIN dim_date dd ON ff.date_key = dd.date_key
LEFT JOIN dim_time dt ON ff.departure_time_key = dt.time_key
GROUP BY dd.year, dd.quarter, dd.month, dd.day_name, dd.day_of_week, dd.season, dd.is_weekend, dd.is_holiday, 
         dt.hour, dt.hour_category, dt.time_period, dt.is_business_hours;

-- -----------------------------------------------------------------------------
-- VIEW 5: Weather Impact Assessment
-- Purpose: Quantify weather-related delay impacts
-- Usage: Weather contingency planning, risk assessment, operational decisions
-- -----------------------------------------------------------------------------
CREATE VIEW v_weather_impact AS
SELECT 
    dd.year,
    dd.quarter,
    dd.month,
    dd.season,
    dao.airport_code as origin_airport,
    dao.city as origin_city,
    dao.state as origin_state,
    
    -- Weather Delay Overview
    COUNT(*) as total_flights,
    SUM(CASE WHEN ff.weather_delay_minutes > 0 THEN 1 ELSE 0 END) as weather_affected_flights,
    ROUND(
        SUM(CASE WHEN ff.weather_delay_minutes > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    ) as weather_impact_rate,
    
    -- Weather Delay Metrics
    SUM(ff.weather_delay_minutes) as total_weather_delay_minutes,
    AVG(CASE WHEN ff.weather_delay_minutes > 0 THEN ff.weather_delay_minutes END) as avg_weather_delay_when_affected,
    MAX(ff.weather_delay_minutes) as max_weather_delay,
    
    -- Weather vs Other Delays
    SUM(ff.weather_delay_minutes) as weather_delays,
    SUM(ff.air_system_delay_minutes) as air_system_delays,
    SUM(ff.airline_delay_minutes) as airline_delays,
    SUM(ff.late_aircraft_delay_minutes) as late_aircraft_delays,
    SUM(ff.security_delay_minutes) as security_delays,
    
    -- Weather Impact Severity
    SUM(CASE WHEN ff.weather_delay_minutes BETWEEN 1 AND 30 THEN 1 ELSE 0 END) as minor_weather_delays,
    SUM(CASE WHEN ff.weather_delay_minutes BETWEEN 31 AND 120 THEN 1 ELSE 0 END) as moderate_weather_delays,
    SUM(CASE WHEN ff.weather_delay_minutes > 120 THEN 1 ELSE 0 END) as severe_weather_delays,
    
    -- Weather Cancellations
    SUM(CASE WHEN ff.is_cancelled = 1 AND ff.cancellation_reason = 'B' THEN 1 ELSE 0 END) as weather_cancellations,
    
    -- Seasonal Weather Patterns
    AVG(CASE WHEN dd.season = 'Winter' THEN ff.weather_delay_minutes ELSE 0 END) as winter_avg_weather_delay,
    AVG(CASE WHEN dd.season = 'Spring' THEN ff.weather_delay_minutes ELSE 0 END) as spring_avg_weather_delay,
    AVG(CASE WHEN dd.season = 'Summer' THEN ff.weather_delay_minutes ELSE 0 END) as summer_avg_weather_delay,
    AVG(CASE WHEN dd.season = 'Fall' THEN ff.weather_delay_minutes ELSE 0 END) as fall_avg_weather_delay,
    
    -- Weather Recovery Metrics
    AVG(CASE WHEN ff.weather_delay_minutes > 0 THEN ff.arrival_delay_minutes - ff.departure_delay_minutes END) as avg_recovery_time

FROM fact_flights ff
JOIN dim_date dd ON ff.date_key = dd.date_key
JOIN dim_airport dao ON ff.origin_airport_key = dao.airport_key
GROUP BY dd.year, dd.quarter, dd.month, dd.season, dao.airport_code, dao.city, dao.state
HAVING COUNT(*) >= 100; -- Only airports with significant traffic

-- -----------------------------------------------------------------------------
-- VIEW 6: Root Cause Analysis  
-- Purpose: Categorize delay causes and identify improvement opportunities
-- Usage: Operational improvements, accountability analysis, targeted interventions
-- -----------------------------------------------------------------------------
CREATE VIEW v_root_cause_analysis AS
SELECT 
    ddc.cause_code,
    ddc.cause_name,
    ddc.cause_category,
    ddc.is_controllable,
    ddc.severity_weight,
    da.airline_code,
    da.airline_name,
    da.airline_type,
    dd.year,
    dd.quarter,
    
    -- Delay Cause Distribution
    COUNT(*) as total_affected_flights,
    SUM(ff.departure_delay_minutes) as total_delay_minutes,
    AVG(ff.departure_delay_minutes) as avg_delay_minutes,
    
    -- Impact Analysis
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY da.airline_code, dd.year, dd.quarter), 2
    ) as pct_of_airline_delays,
    
    -- Delay Severity Distribution
    SUM(CASE WHEN ff.departure_delay_minutes BETWEEN 1 AND 15 THEN 1 ELSE 0 END) as minor_delays,
    SUM(CASE WHEN ff.departure_delay_minutes BETWEEN 16 AND 60 THEN 1 ELSE 0 END) as moderate_delays,
    SUM(CASE WHEN ff.departure_delay_minutes BETWEEN 61 AND 180 THEN 1 ELSE 0 END) as significant_delays,
    SUM(CASE WHEN ff.departure_delay_minutes > 180 THEN 1 ELSE 0 END) as severe_delays,
    
    -- Financial Impact Estimation (assuming $50/minute delay cost)
    ROUND(SUM(ff.departure_delay_minutes) * 50, 0) as estimated_delay_cost_usd,
    
    -- Controllability Analysis
    CASE WHEN ddc.is_controllable THEN 'Controllable' ELSE 'External' END as controllability,
    
    -- Improvement Opportunity Score
    ROUND(
        (COUNT(*) * AVG(ff.departure_delay_minutes) * CASE WHEN ddc.is_controllable THEN 1.5 ELSE 0.8 END) / 1000, 2
    ) as improvement_opportunity_score

FROM fact_flights ff
JOIN dim_delay_cause ddc ON ff.delay_cause_key = ddc.delay_cause_key
JOIN dim_airline da ON ff.airline_key = da.airline_key  
JOIN dim_date dd ON ff.date_key = dd.date_key
WHERE ff.departure_delay_minutes > 0  -- Only delayed flights
GROUP BY ddc.delay_cause_key, ddc.cause_code, ddc.cause_name, ddc.cause_category, ddc.is_controllable, ddc.severity_weight,
         da.airline_code, da.airline_name, da.airline_type, dd.year, dd.quarter;

-- -----------------------------------------------------------------------------
-- VIEW 7: Severity Classification
-- Purpose: Distinguish between minor operational delays and severe disruptions  
-- Usage: Resource allocation, customer communication, escalation procedures
-- -----------------------------------------------------------------------------
CREATE VIEW v_severity_classification AS
SELECT 
    ff.delay_severity_category,
    da.airline_code,
    da.airline_name,
    dao.airport_code as origin_airport,
    dao.city as origin_city,
    dd.year,
    dd.quarter,
    dd.month,
    
    -- Volume by Severity
    COUNT(*) as flight_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY da.airline_code, dd.year, dd.quarter), 2) as pct_of_airline_flights,
    
    -- Delay Characteristics by Severity
    AVG(ff.departure_delay_minutes) as avg_departure_delay,
    AVG(ff.arrival_delay_minutes) as avg_arrival_delay,
    MIN(ff.departure_delay_minutes) as min_delay,
    MAX(ff.departure_delay_minutes) as max_delay,
    STDDEV(ff.departure_delay_minutes) as delay_std_dev,
    
    -- Operational Impact
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.actual_elapsed_minutes - ff.scheduled_elapsed_minutes END) as avg_schedule_variance,
    SUM(CASE WHEN ff.is_diverted = 1 THEN 1 ELSE 0 END) as diversions,
    
    -- Passenger Impact Estimation (assuming average 150 passengers per flight)
    COUNT(*) * 150 as estimated_affected_passengers,
    SUM(ff.departure_delay_minutes) * 150 as total_passenger_delay_minutes,
    
    -- Recovery Analysis
    AVG(ff.arrival_delay_minutes - ff.departure_delay_minutes) as avg_airborne_recovery,
    SUM(CASE WHEN ff.arrival_delay_minutes < ff.departure_delay_minutes THEN 1 ELSE 0 END) as flights_with_recovery,
    
    -- Time to Next Available Flight (proxy for passenger rebooking complexity)
    CASE 
        WHEN ff.delay_severity_category = 'Cancelled' THEN 'Rebooking Required'
        WHEN ff.delay_severity_category = 'Severe Delay' THEN 'High Rebooking Risk'
        WHEN ff.delay_severity_category = 'Significant Delay' THEN 'Moderate Rebooking Risk'
        ELSE 'Low Rebooking Risk'
    END as rebooking_risk_category,
    
    -- Financial Impact Categories
    CASE 
        WHEN ff.delay_severity_category IN ('Cancelled', 'Severe Delay') THEN 'High Cost'
        WHEN ff.delay_severity_category = 'Significant Delay' THEN 'Medium Cost'
        ELSE 'Low Cost'
    END as cost_impact_category

FROM fact_flights ff
JOIN dim_airline da ON ff.airline_key = da.airline_key
JOIN dim_airport dao ON ff.origin_airport_key = dao.airport_key
JOIN dim_date dd ON ff.date_key = dd.date_key
GROUP BY ff.delay_severity_category, da.airline_code, da.airline_name, 
         dao.airport_code, dao.city, dd.year, dd.quarter, dd.month;

-- -----------------------------------------------------------------------------
-- VIEW 8: Disruption Prediction
-- Purpose: Forecast cancellations and diversions beyond simple delays
-- Usage: Crisis management, passenger rebooking, resource allocation
-- -----------------------------------------------------------------------------
CREATE VIEW v_disruption_analysis AS
SELECT 
    da.airline_code,
    da.airline_name,
    dao.airport_code as origin_airport,
    dao.city as origin_city,
    dad.airport_code as destination_airport,
    dad.city as destination_city,
    dd.year,
    dd.quarter,
    dd.season,
    dd.day_name,
    
    -- Flight Volume
    COUNT(*) as total_scheduled_flights,
    SUM(CASE WHEN ff.is_cancelled = 0 AND ff.is_diverted = 0 THEN 1 ELSE 0 END) as completed_as_planned,
    
    -- Disruption Metrics
    SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) as cancellations,
    SUM(CASE WHEN ff.is_diverted = 1 THEN 1 ELSE 0 END) as diversions,
    SUM(CASE WHEN ff.is_cancelled = 1 OR ff.is_diverted = 1 THEN 1 ELSE 0 END) as total_disruptions,
    
    -- Disruption Rates
    ROUND(SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cancellation_rate,
    ROUND(SUM(CASE WHEN ff.is_diverted = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as diversion_rate,
    ROUND(SUM(CASE WHEN ff.is_cancelled = 1 OR ff.is_diverted = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as disruption_rate,
    
    -- Cancellation Reason Analysis
    SUM(CASE WHEN ff.cancellation_reason = 'A' THEN 1 ELSE 0 END) as carrier_cancellations,
    SUM(CASE WHEN ff.cancellation_reason = 'B' THEN 1 ELSE 0 END) as weather_cancellations,
    SUM(CASE WHEN ff.cancellation_reason = 'C' THEN 1 ELSE 0 END) as nas_cancellations,
    SUM(CASE WHEN ff.cancellation_reason = 'D' THEN 1 ELSE 0 END) as security_cancellations,
    
    -- Disruption Predictors
    AVG(CASE WHEN ff.departure_delay_minutes > 0 THEN ff.departure_delay_minutes END) as avg_delay_when_delayed,
    SUM(CASE WHEN ff.departure_delay_minutes > 180 THEN 1 ELSE 0 END) as extreme_delays,
    SUM(CASE WHEN ff.weather_delay_minutes > 60 THEN 1 ELSE 0 END) as severe_weather_delays,
    
    -- Network Effect Indicators
    AVG(ff.late_aircraft_delay_minutes) as avg_late_aircraft_delay,
    SUM(CASE WHEN ff.late_aircraft_delay_minutes > 0 THEN 1 ELSE 0 END) as flights_with_late_aircraft,
    
    -- Disruption Impact Score (weighted by severity and frequency)
    ROUND(
        (SUM(CASE WHEN ff.is_cancelled = 1 THEN 2 ELSE 0 END) + 
         SUM(CASE WHEN ff.is_diverted = 1 THEN 1.5 ELSE 0 END) +
         SUM(CASE WHEN ff.departure_delay_minutes > 180 THEN 1 ELSE 0 END)) * 
        COUNT(*) / 10000, 2
    ) as disruption_impact_score,
    
    -- Recovery Capability
    AVG(CASE WHEN ff.is_cancelled = 0 AND ff.departure_delay_minutes > 60 
             THEN ff.arrival_delay_minutes - ff.departure_delay_minutes END) as avg_delay_recovery

FROM fact_flights ff
JOIN dim_airline da ON ff.airline_key = da.airline_key
JOIN dim_airport dao ON ff.origin_airport_key = dao.airport_key
JOIN dim_airport dad ON ff.destination_airport_key = dad.airport_key
JOIN dim_date dd ON ff.date_key = dd.date_key
GROUP BY da.airline_code, da.airline_name, dao.airport_code, dao.city, 
         dad.airport_code, dad.city, dd.year, dd.quarter, dd.season, dd.day_name
HAVING COUNT(*) >= 20; -- Only meaningful volume

-- =============================================================================
-- PERFORMANCE-OPTIMIZED AGGREGATED VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Airline Monthly Summary - Pre-aggregated for Dashboard Performance
-- -----------------------------------------------------------------------------
CREATE VIEW v_airline_monthly_summary AS
SELECT 
    da.airline_code,
    da.airline_name,
    da.airline_type,
    dd.year,
    dd.month,
    dd.month_name,
    
    -- Core Performance Metrics
    COUNT(*) as total_flights,
    SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END) as completed_flights,
    ROUND(AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END), 1) as avg_departure_delay,
    ROUND(
        SUM(CASE WHEN ff.departure_delay_minutes <= 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 0), 1
    ) as ontime_performance_pct,
    ROUND(SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cancellation_rate,
    
    -- Ranking Metrics
    RANK() OVER (PARTITION BY dd.year, dd.month ORDER BY 
        SUM(CASE WHEN ff.departure_delay_minutes <= 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 0) DESC
    ) as ontime_rank,
    RANK() OVER (PARTITION BY dd.year, dd.month ORDER BY 
        AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END)
    ) as delay_rank

FROM fact_flights ff
JOIN dim_airline da ON ff.airline_key = da.airline_key
JOIN dim_date dd ON ff.date_key = dd.date_key
GROUP BY da.airline_code, da.airline_name, da.airline_type, dd.year, dd.month, dd.month_name;

-- -----------------------------------------------------------------------------
-- Airport Daily Metrics - High-frequency operational dashboard
-- -----------------------------------------------------------------------------
CREATE VIEW v_airport_daily_metrics AS
SELECT 
    dap.airport_code,
    dap.airport_name,
    dap.city,
    dap.state,
    dd.date,
    dd.day_name,
    dd.is_weekend,
    dd.is_holiday,
    
    -- Daily Operations
    COUNT(*) as daily_departures,
    SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END) as completed_departures,
    SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) as cancelled_departures,
    
    -- Daily Performance
    ROUND(AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END), 1) as avg_departure_delay,
    MAX(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) as max_departure_delay,
    SUM(CASE WHEN ff.departure_delay_minutes > 60 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) as severe_delays,
    
    -- Weather Impact
    SUM(CASE WHEN ff.weather_delay_minutes > 0 THEN 1 ELSE 0 END) as weather_affected_flights,
    SUM(ff.weather_delay_minutes) as total_weather_delay_minutes,
    
    -- Operational Efficiency
    ROUND(AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.taxi_out_minutes END), 1) as avg_taxi_out_minutes,
    
    -- Traffic Distribution
    COUNT(CASE WHEN dt.hour BETWEEN 6 AND 9 THEN 1 END) as morning_peak_flights,
    COUNT(CASE WHEN dt.hour BETWEEN 17 AND 20 THEN 1 END) as evening_peak_flights

FROM fact_flights ff
JOIN dim_airport dap ON ff.origin_airport_key = dap.airport_key
JOIN dim_date dd ON ff.date_key = dd.date_key
LEFT JOIN dim_time dt ON ff.departure_time_key = dt.time_key
GROUP BY dap.airport_code, dap.airport_name, dap.city, dap.state, dd.date, dd.day_name, dd.is_weekend, dd.is_holiday
HAVING COUNT(*) >= 5; -- Only days with meaningful traffic

-- -----------------------------------------------------------------------------
-- Hourly Delay Patterns - Time-based operational insights
-- -----------------------------------------------------------------------------
CREATE VIEW v_hourly_delay_patterns AS
SELECT 
    dt.hour,
    dt.hour_category,
    dd.day_of_week,
    dd.day_name,
    dd.is_weekend,
    
    -- Hourly Flight Volume
    COUNT(*) as flights_per_hour,
    SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END) as completed_flights_per_hour,
    
    -- Hourly Delay Performance
    ROUND(AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END), 1) as avg_hourly_delay,
    ROUND(STDDEV(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END), 1) as delay_std_dev,
    
    -- Delay Rate by Hour
    ROUND(
        SUM(CASE WHEN ff.departure_delay_minutes > 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 0), 1
    ) as hourly_delay_rate,
    
    -- Peak Hour Indicators
    CASE 
        WHEN dt.hour BETWEEN 6 AND 9 THEN 1
        WHEN dt.hour BETWEEN 17 AND 20 THEN 1
        ELSE 0
    END as is_peak_hour

FROM fact_flights ff
JOIN dim_time dt ON ff.departure_time_key = dt.time_key
JOIN dim_date dd ON ff.date_key = dd.date_key
GROUP BY dt.hour, dt.hour_category, dd.day_of_week, dd.day_name, dd.is_weekend
HAVING COUNT(*) >= 100; -- Only hours with meaningful volume

-- =============================================================================
-- EXECUTIVE DASHBOARD VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Top Delayed Routes - Worst performing routes for management attention
-- -----------------------------------------------------------------------------
CREATE VIEW v_top_delayed_routes AS
SELECT 
    CONCAT(dao.airport_code, '-', dad.airport_code) as route_code,
    CONCAT(dao.city, ', ', dao.state, ' → ', dad.city, ', ', dad.state) as route_description,
    ff.distance_miles,
    
    -- Route Performance Summary
    COUNT(*) as total_flights,
    COUNT(DISTINCT da.airline_code) as airlines_serving,
    ROUND(AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END), 1) as avg_departure_delay,
    
    -- Delay Impact Metrics
    SUM(CASE WHEN ff.departure_delay_minutes > 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) as delayed_flights,
    ROUND(
        SUM(CASE WHEN ff.departure_delay_minutes > 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 0), 1
    ) as delay_rate_pct,
    
    -- Business Impact Score
    ROUND(
        (AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) * COUNT(*)) / 100, 1
    ) as delay_impact_score,
    
    -- Cancellation Impact
    ROUND(SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cancellation_rate

FROM fact_flights ff
JOIN dim_airport dao ON ff.origin_airport_key = dao.airport_key
JOIN dim_airport dad ON ff.destination_airport_key = dad.airport_key
JOIN dim_airline da ON ff.airline_key = da.airline_key
GROUP BY dao.airport_code, dao.city, dao.state, dad.airport_code, dad.city, dad.state, ff.distance_miles
HAVING COUNT(*) >= 100
ORDER BY delay_impact_score DESC
LIMIT 50;

-- -----------------------------------------------------------------------------
-- Airline Rankings - Comprehensive airline performance comparison
-- -----------------------------------------------------------------------------
CREATE VIEW v_airline_rankings AS
SELECT 
    da.airline_code,
    da.airline_name,
    da.airline_type,
    
    -- Volume Metrics
    COUNT(*) as total_flights,
    SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END) as completed_flights,
    
    -- Performance Metrics
    ROUND(AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END), 2) as avg_departure_delay,
    ROUND(
        SUM(CASE WHEN ff.departure_delay_minutes <= 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 0), 2
    ) as ontime_performance_pct,
    ROUND(SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cancellation_rate,
    
    -- Ranking Positions
    RANK() OVER (ORDER BY 
        SUM(CASE WHEN ff.departure_delay_minutes <= 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 0) DESC
    ) as ontime_rank,
    RANK() OVER (ORDER BY AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END)) as delay_rank,
    RANK() OVER (ORDER BY SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as cancellation_rank,
    
    -- Performance Categories
    CASE 
        WHEN SUM(CASE WHEN ff.departure_delay_minutes <= 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
             NULLIF(SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 0) >= 80 THEN 'Excellent'
        WHEN SUM(CASE WHEN ff.departure_delay_minutes <= 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
             NULLIF(SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 0) >= 70 THEN 'Good'
        WHEN SUM(CASE WHEN ff.departure_delay_minutes <= 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
             NULLIF(SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 0) >= 60 THEN 'Fair'
        ELSE 'Poor'
    END as performance_grade

FROM fact_flights ff
JOIN dim_airline da ON ff.airline_key = da.airline_key
GROUP BY da.airline_code, da.airline_name, da.airline_type
HAVING COUNT(*) >= 1000
ORDER BY ontime_performance_pct DESC;

-- -----------------------------------------------------------------------------
-- Peak Congestion Times - Operational capacity planning
-- -----------------------------------------------------------------------------
CREATE VIEW v_peak_congestion_times AS
SELECT 
    dap.airport_code,
    dap.airport_name,
    dap.city,
    dap.state,
    dap.airport_size_category,
    dt.hour,
    dt.hour_category,
    dd.day_of_week,
    dd.day_name,
    
    -- Traffic Volume
    COUNT(*) as flights_per_hour,
    AVG(COUNT(*)) OVER (PARTITION BY dap.airport_code) as avg_hourly_flights,
    
    -- Congestion Indicators
    ROUND(AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END), 1) as avg_departure_delay,
    ROUND(AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.taxi_out_minutes END), 1) as avg_taxi_out_minutes,
    MAX(CASE WHEN ff.is_cancelled = 0 THEN ff.taxi_out_minutes END) as max_taxi_out_minutes,
    
    -- Congestion Score (combines volume and delay impact)
    ROUND(
        (COUNT(*) / NULLIF(AVG(COUNT(*)) OVER (PARTITION BY dap.airport_code), 0)) * 
        (1 + AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) / 60.0), 2
    ) as congestion_score,
    
    -- Peak Hour Classification
    CASE 
        WHEN COUNT(*) >= AVG(COUNT(*)) OVER (PARTITION BY dap.airport_code) * 1.5 THEN 'Peak'
        WHEN COUNT(*) >= AVG(COUNT(*)) OVER (PARTITION BY dap.airport_code) * 1.2 THEN 'High'
        WHEN COUNT(*) >= AVG(COUNT(*)) OVER (PARTITION BY dap.airport_code) * 0.8 THEN 'Normal'
        ELSE 'Low'
    END as traffic_level

FROM fact_flights ff
JOIN dim_airport dap ON ff.origin_airport_key = dap.airport_key
JOIN dim_time dt ON ff.departure_time_key = dt.time_key
JOIN dim_date dd ON ff.date_key = dd.date_key
GROUP BY dap.airport_code, dap.airport_name, dap.city, dap.state, dap.airport_size_category,
         dt.hour, dt.hour_category, dd.day_of_week, dd.day_name
HAVING COUNT(*) >= 50
ORDER BY dap.airport_code, dd.day_of_week, dt.hour;

-- -----------------------------------------------------------------------------
-- Executive Dashboard - High-level KPI summary
-- -----------------------------------------------------------------------------
CREATE VIEW v_executive_dashboard AS
SELECT 
    'Overall Performance' as metric_category,
    dd.year,
    dd.quarter,
    
    -- System-wide Volume
    COUNT(*) as total_system_flights,
    SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END) as completed_flights,
    SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) as cancelled_flights,
    
    -- System Performance KPIs
    ROUND(AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END), 2) as avg_system_delay,
    ROUND(
        SUM(CASE WHEN ff.departure_delay_minutes <= 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 0), 2
    ) as system_ontime_performance,
    ROUND(SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as system_cancellation_rate,
    
    -- Delay Cost Impact (estimated at $50/minute)
    ROUND(SUM(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes ELSE 0 END) * 50 / 1000000, 1) as delay_cost_millions_usd,
    
    -- Top Problem Areas
    (SELECT da.airline_name FROM fact_flights ff2 
     JOIN dim_airline da ON ff2.airline_key = da.airline_key 
     JOIN dim_date dd2 ON ff2.date_key = dd2.date_key
     WHERE dd2.year = dd.year AND dd2.quarter = dd.quarter
     GROUP BY da.airline_name 
     ORDER BY AVG(CASE WHEN ff2.is_cancelled = 0 THEN ff2.departure_delay_minutes END) DESC LIMIT 1
    ) as worst_performing_airline,
    
    -- Weather Impact
    ROUND(
        SUM(CASE WHEN ff.weather_delay_minutes > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    ) as weather_impact_rate,
    
    -- Severe Disruption Rate
    ROUND(
        SUM(CASE WHEN ff.departure_delay_minutes > 180 OR ff.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    ) as severe_disruption_rate

FROM fact_flights ff
JOIN dim_date dd ON ff.date_key = dd.date_key
GROUP BY dd.year, dd.quarter
ORDER BY dd.year, dd.quarter;

-- =============================================================================
-- PREDICTIVE ANALYSIS SUPPORT VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Flight Delay Features - ML-ready feature set for predictive modeling
-- -----------------------------------------------------------------------------
CREATE VIEW v_flight_delay_features AS
SELECT 
    ff.flight_key,
    
    -- Categorical Features
    da.airline_code,
    da.airline_type,
    dao.airport_code as origin_airport,
    dao.state as origin_state,
    dad.airport_code as destination_airport,
    dad.state as destination_state,
    dd.month,
    dd.day_of_week,
    dd.season,
    dt.hour,
    dt.hour_category,
    
    -- Boolean Features
    dd.is_weekend,
    dd.is_holiday,
    dt.is_business_hours,
    CASE WHEN dt.hour BETWEEN 6 AND 9 OR dt.hour BETWEEN 17 AND 20 THEN 1 ELSE 0 END as is_peak_hour,
    
    -- Numerical Features
    ff.distance_miles,
    ff.scheduled_elapsed_minutes,
    
    -- Historical Performance Features (airline-airport combinations)
    AVG(ff2.departure_delay_minutes) OVER (
        PARTITION BY da.airline_code, dao.airport_code 
        ORDER BY dd.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) as historical_avg_delay_airline_airport,
    
    -- Route-based Historical Features
    AVG(ff3.departure_delay_minutes) OVER (
        PARTITION BY dao.airport_code, dad.airport_code
        ORDER BY dd.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING  
    ) as historical_avg_delay_route,
    
    -- Time-based Historical Features
    AVG(ff4.departure_delay_minutes) OVER (
        PARTITION BY dt.hour, dd.day_of_week
        ORDER BY dd.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) as historical_avg_delay_time,
    
    -- Weather Features
    CASE WHEN ff.weather_delay_minutes > 0 THEN 1 ELSE 0 END as has_weather_delay,
    
    -- Target Variables
    ff.departure_delay_minutes as target_departure_delay,
    CASE WHEN ff.departure_delay_minutes > 15 THEN 1 ELSE 0 END as target_is_delayed,
    ff.is_cancelled as target_is_cancelled

FROM fact_flights ff
JOIN dim_airline da ON ff.airline_key = da.airline_key
JOIN dim_airport dao ON ff.origin_airport_key = dao.airport_key
JOIN dim_airport dad ON ff.destination_airport_key = dad.airport_key
JOIN dim_date dd ON ff.date_key = dd.date_key
LEFT JOIN dim_time dt ON ff.departure_time_key = dt.time_key
LEFT JOIN fact_flights ff2 ON ff2.airline_key = ff.airline_key AND ff2.origin_airport_key = ff.origin_airport_key
LEFT JOIN fact_flights ff3 ON ff3.origin_airport_key = ff.origin_airport_key AND ff3.destination_airport_key = ff.destination_airport_key  
LEFT JOIN fact_flights ff4 ON ff4.departure_time_key = ff.departure_time_key;

-- -----------------------------------------------------------------------------
-- Historical Delay Trends - Time series analysis ready
-- -----------------------------------------------------------------------------
CREATE VIEW v_historical_delay_trends AS
SELECT 
    dd.date,
    dd.year,
    dd.month,
    dd.day_of_year,
    dd.day_of_week,
    dd.season,
    
    -- Daily System Metrics
    COUNT(*) as daily_flights,
    AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) as daily_avg_delay,
    STDDEV(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END) as daily_delay_std,
    
    -- Daily Performance Rates
    ROUND(
        SUM(CASE WHEN ff.departure_delay_minutes <= 15 AND ff.is_cancelled = 0 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN ff.is_cancelled = 0 THEN 1 ELSE 0 END), 0), 2
    ) as daily_ontime_rate,
    ROUND(SUM(CASE WHEN ff.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as daily_cancellation_rate,
    
    -- Moving Averages for Trend Analysis
    AVG(AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END)) OVER (
        ORDER BY dd.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as delay_7day_moving_avg,
    AVG(AVG(CASE WHEN ff.is_cancelled = 0 THEN ff.departure_delay_minutes END)) OVER (
        ORDER BY dd.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as delay_30day_moving_avg,
    
    -- Weather Impact Trends
    SUM(CASE WHEN ff.weather_delay_minutes > 0 THEN 1 ELSE 0 END) as daily_weather_affected_flights,
    SUM(ff.weather_delay_minutes) as daily_total_weather_delay,
    
    -- Seasonal Indicators
    CASE WHEN dd.month IN (12, 1, 2) THEN 1 ELSE 0 END as is_winter,
    CASE WHEN dd.month IN (6, 7, 8) THEN 1 ELSE 0 END as is_summer,
    
    -- Holiday Proximity (days to nearest holiday)
    DATEDIFF(
        (SELECT MIN(dd2.date) FROM dim_date dd2 WHERE dd2.is_holiday = 1 AND dd2.date >= dd.date),
        dd.date
    ) as days_to_next_holiday

FROM fact_flights ff
JOIN dim_date dd ON ff.date_key = dd.date_key
GROUP BY dd.date, dd.year, dd.month, dd.day_of_year, dd.day_of_week, dd.season
ORDER BY dd.date;

-- =============================================================================
-- VIEW USAGE DOCUMENTATION
-- =============================================================================

/*
ANALYTICAL VIEWS USAGE GUIDE

CORE ANALYTICAL VIEWS (8 Primary Objectives):
---------------------------------------------
1. v_carrier_performance      - Airline benchmarking and performance monitoring
2. v_airport_congestion       - Infrastructure planning and capacity management  
3. v_route_optimization       - Route planning and pricing strategies
4. v_temporal_patterns        - Schedule optimization and staffing
5. v_weather_impact          - Weather contingency planning
6. v_root_cause_analysis     - Operational improvements and accountability
7. v_severity_classification - Resource allocation and customer communication
8. v_disruption_analysis     - Crisis management and passenger rebooking

PERFORMANCE-OPTIMIZED VIEWS:
----------------------------
- v_airline_monthly_summary   - Pre-aggregated monthly metrics for dashboards
- v_airport_daily_metrics     - High-frequency operational monitoring
- v_hourly_delay_patterns     - Time-based operational insights

EXECUTIVE DASHBOARD VIEWS:
--------------------------
- v_top_delayed_routes        - Management attention and improvement focus
- v_airline_rankings          - Comprehensive performance comparison
- v_peak_congestion_times     - Capacity planning and slot management
- v_executive_dashboard       - System-wide KPI summary

PREDICTIVE ANALYSIS VIEWS:
--------------------------
- v_flight_delay_features     - ML-ready feature set for modeling
- v_historical_delay_trends   - Time series analysis and forecasting

EXAMPLE USAGE QUERIES:
---------------------

-- Get top 5 worst performing airlines by on-time performance:
SELECT airline_name, ontime_performance_pct, delay_rank 
FROM v_airline_rankings 
ORDER BY ontime_performance_pct ASC 
LIMIT 5;

-- Find most congested airport hours:
SELECT airport_code, hour, congestion_score, traffic_level
FROM v_peak_congestion_times 
WHERE traffic_level = 'Peak'
ORDER BY congestion_score DESC;

-- Analyze weather impact by season:
SELECT season, weather_impact_rate, avg_weather_delay_when_affected
FROM v_weather_impact
GROUP BY season
ORDER BY weather_impact_rate DESC;

-- Monitor daily performance trends:
SELECT date, daily_avg_delay, delay_7day_moving_avg, daily_ontime_rate
FROM v_historical_delay_trends
WHERE date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
ORDER BY date;

PERFORMANCE NOTES:
-----------------
- Views use indexes created in flights_db.sql for optimal performance
- Aggregated views provide sub-second response for dashboard queries
- Use HAVING clauses to filter low-volume data for statistical significance
- Historical features in ML view may require additional indexing for large datasets

*/

-- End of flights_views.sql
