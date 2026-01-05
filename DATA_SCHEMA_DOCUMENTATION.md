# Data Schema Documentation

## Table of Contents

1. [Bronze Layer Tables](#bronze-layer-tables)
   - [1.1 raw_traffic](#11-raw_traffic)
   - [1.2 raw_roads](#12-raw_roads)
2. [Silver Layer Tables](#silver-layer-tables)
   - [2.1 silver_traffic](#21-silver_traffic)
   - [2.2 silver_roads](#22-silver_roads)
3. [Gold Layer Tables](#gold-layer-tables)
   - [3.1 gold_traffic](#31-gold_traffic)
   - [3.2 gold_roads](#32-gold_roads)
4. [Data Quality Tables](#data-quality-tables)
   - [4.1 data_quality_quarantine](#41-data_quality_quarantine)

---

## Bronze Layer Tables

### 1.1 raw_traffic

**Description:** Raw traffic count data ingested from CSV files in the landing zone. Contains vehicle count records by location, time, and vehicle type.

**Storage Location:** `{bronze_url}raw_traffic/`  
**Table Format:** Delta Lake  
**Catalog:** `traffic_{environment}_catalog.traffic_db.raw_traffic`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| Record_ID | INT | No | Unique identifier for each traffic count record |
| Count_point_id | INT | Yes | Identifier for the traffic counting point/location |
| Direction_of_travel | VARCHAR(255) | Yes | Direction of vehicle travel (e.g., 'E', 'W', 'N', 'S') |
| Year | INT | Yes | Year of the traffic count |
| Count_date | VARCHAR(255) | Yes | Date and time of the count in string format (e.g., '6/25/2014 0:00') |
| hour | INT | Yes | Hour of the day when the count was taken (1-24) |
| Region_id | INT | Yes | Numeric identifier for the region |
| Region_name | VARCHAR(255) | Yes | Name of the region (e.g., 'Scotland', 'South West') |
| Local_authority_name | VARCHAR(255) | Yes | Name of the local authority area |
| Road_name | VARCHAR(255) | Yes | Name or identifier of the road (e.g., 'A77') |
| Road_Category_ID | INT | Yes | Numeric identifier for road category (1-5) |
| Start_junction_road_name | VARCHAR(255) | Yes | Name of the road at the start junction |
| End_junction_road_name | VARCHAR(255) | Yes | Name of the road at the end junction |
| Latitude | DOUBLE | Yes | Geographic latitude coordinate of the count point |
| Longitude | DOUBLE | Yes | Geographic longitude coordinate of the count point |
| Link_length_km | DOUBLE | Yes | Length of the road link in kilometers |
| Pedal_cycles | INT | Yes | Count of pedal cycles (bicycles) |
| Two_wheeled_motor_vehicles | INT | Yes | Count of two-wheeled motor vehicles (motorcycles) |
| Cars_and_taxis | INT | Yes | Count of cars and taxis |
| Buses_and_coaches | INT | Yes | Count of buses and coaches |
| LGV_Type | INT | Yes | Count of Light Goods Vehicles |
| HGV_Type | INT | Yes | Count of Heavy Goods Vehicles |
| EV_Car | INT | Yes | Count of electric cars |
| EV_Bike | INT | Yes | Count of electric bikes |
| Extract_Time | TIMESTAMP | No | Timestamp when the record was extracted/ingested into the bronze layer |

**Data Quality Rules:**
- Record_ID must not be null
- Count_date must match format: dd/mm/yyyy HH:mm
- hour must be between 1 and 24
- Road_Category_ID must be between 1 and 5

---

### 1.2 raw_roads

**Description:** Raw road infrastructure data containing road categories, regions, and aggregate motor vehicle statistics.

**Storage Location:** `{bronze_url}raw_roads/`  
**Table Format:** Delta Lake  
**Catalog:** `traffic_{environment}_catalog.traffic_db.raw_roads`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| Road_ID | INT | No | Unique identifier for each road record |
| Road_Category_Id | INT | Yes | Numeric identifier for road category (1-5) |
| Road_Category | VARCHAR(255) | Yes | Road category code (e.g., 'TM', 'TA', 'PA', 'PM', 'M') |
| Region_ID | INT | Yes | Numeric identifier for the region |
| Region_Name | VARCHAR(255) | Yes | Name of the region |
| Total_Link_Length_Km | DOUBLE | Yes | Total length of road links in kilometers |
| Total_Link_Length_Miles | DOUBLE | Yes | Total length of road links in miles |
| All_Motor_Vehicles | DOUBLE | Yes | Total count of all motor vehicles on the road |

**Data Quality Rules:**
- Road_ID must not be null
- Total_Link_Length_Km must be >= 0
- Road_Category_Id must be between 1 and 5

**Road Category Codes:**
- TM: Class A Trunk Motor
- TA: Class A Trunk Road
- PA: Class A Principal Road
- PM: Class A Principal Motorway
- M: Class B Road

---

## Silver Layer Tables

### 2.1 silver_traffic

**Description:** Cleaned and enriched traffic data from the bronze layer. Includes data quality improvements and calculated vehicle counts.

**Storage Location:** `{silver_url}silver_traffic/`  
**Table Format:** Delta Lake  
**Catalog:** `traffic_{environment}_catalog.traffic_db.silver_traffic`

**Source:** Transformed from `raw_traffic` table

**Transformations Applied:**
- Duplicate records removed
- Null values handled: strings → 'Unknown', numeric → 0
- Calculated columns added

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| Record_ID | INT | No | Unique identifier for each traffic count record |
| Count_point_id | INT | Yes | Identifier for the traffic counting point/location |
| Direction_of_travel | VARCHAR(255) | Yes | Direction of vehicle travel |
| Year | INT | Yes | Year of the traffic count |
| Count_date | VARCHAR(255) | Yes | Date and time of the count in string format |
| hour | INT | Yes | Hour of the day when the count was taken (1-24) |
| Region_id | INT | Yes | Numeric identifier for the region |
| Region_name | VARCHAR(255) | Yes | Name of the region |
| Local_authority_name | VARCHAR(255) | Yes | Name of the local authority area |
| Road_name | VARCHAR(255) | Yes | Name or identifier of the road |
| Road_Category_ID | INT | Yes | Numeric identifier for road category (1-5) |
| Start_junction_road_name | VARCHAR(255) | Yes | Name of the road at the start junction |
| End_junction_road_name | VARCHAR(255) | Yes | Name of the road at the end junction |
| Latitude | DOUBLE | Yes | Geographic latitude coordinate of the count point |
| Longitude | DOUBLE | Yes | Geographic longitude coordinate of the count point |
| Link_length_km | DOUBLE | Yes | Length of the road link in kilometers |
| Pedal_cycles | INT | Yes | Count of pedal cycles (bicycles) |
| Two_wheeled_motor_vehicles | INT | Yes | Count of two-wheeled motor vehicles (motorcycles) |
| Cars_and_taxis | INT | Yes | Count of cars and taxis |
| Buses_and_coaches | INT | Yes | Count of buses and coaches |
| LGV_Type | INT | Yes | Count of Light Goods Vehicles |
| HGV_Type | INT | Yes | Count of Heavy Goods Vehicles |
| EV_Car | INT | Yes | Count of electric cars |
| EV_Bike | INT | Yes | Count of electric bikes |
| Extract_Time | TIMESTAMP | No | Timestamp when the record was extracted/ingested |
| **Electrics_Vehicles_Count** | INT | No | **Calculated:** Sum of EV_Car + EV_Bike |
| **Motor_Vehicles_Count** | INT | No | **Calculated:** Sum of Two_wheeled_motor_vehicles + Cars_and_taxis + Buses_and_coaches + LGV_Type + HGV_Type + Electrics_Vehicles_Count |
| **Transformed_Time** | TIMESTAMP | No | **Calculated:** Timestamp when the record was transformed in the silver layer |

**Data Quality Improvements:**
- No duplicate records
- No null values (replaced with defaults)
- All calculated fields populated

---

### 2.2 silver_roads

**Description:** Cleaned and enriched road data from the bronze layer. Includes road category name mappings and road type classifications.

**Storage Location:** `{silver_url}silver_roads/`  
**Table Format:** Delta Lake  
**Catalog:** `traffic_{environment}_catalog.traffic_db.silver_roads`

**Source:** Transformed from `raw_roads` table

**Transformations Applied:**
- Duplicate records removed
- Null values handled: strings → 'Unknown', numeric → 0
- Road category name mapping added
- Road type classification added

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| Road_ID | INT | No | Unique identifier for each road record |
| Road_Category_Id | INT | Yes | Numeric identifier for road category (1-5) |
| Road_Category | VARCHAR(255) | Yes | Road category code (e.g., 'TM', 'TA', 'PA', 'PM', 'M') |
| Region_ID | INT | Yes | Numeric identifier for the region |
| Region_Name | VARCHAR(255) | Yes | Name of the region |
| Total_Link_Length_Km | DOUBLE | Yes | Total length of road links in kilometers |
| Total_Link_Length_Miles | DOUBLE | Yes | Total length of road links in miles |
| All_Motor_Vehicles | DOUBLE | Yes | Total count of all motor vehicles on the road |
| **Road_Category_Name** | VARCHAR(255) | No | **Calculated:** Full name of road category based on Road_Category code mapping |
| **Road_Type** | VARCHAR(255) | No | **Calculated:** Classification as 'Major' (Class A) or 'Minor' (Class B) |

**Road Category Name Mapping:**
- 'TA' → 'Class A Trunk Road'
- 'TM' → 'Class A Trunk Motor'
- 'PA' → 'Class A Principal Road'
- 'PM' → 'Class A Principal Motorway'
- 'M' → 'Class B Road'
- Other → 'NA'

**Road Type Classification:**
- Contains 'Class A' → 'Major'
- Contains 'Class B' → 'Minor'
- Other → 'NA'

**Data Quality Improvements:**
- No duplicate records
- No null values (replaced with defaults)
- All classification fields populated

---

## Gold Layer Tables

### 3.1 gold_traffic

**Description:** Final analytical-ready traffic data with business metrics calculated. Optimized for reporting and analytics.

**Storage Location:** `{gold_url}gold_traffic/`  
**Table Format:** Delta Lake  
**Catalog:** `traffic_{environment}_catalog.traffic_db.gold_traffic`

**Source:** Transformed from `silver_traffic` table

**Transformations Applied:**
- Vehicle intensity calculation added
- Loading timestamp added

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| Record_ID | INT | No | Unique identifier for each traffic count record |
| Count_point_id | INT | Yes | Identifier for the traffic counting point/location |
| Direction_of_travel | VARCHAR(255) | Yes | Direction of vehicle travel |
| Year | INT | Yes | Year of the traffic count |
| Count_date | VARCHAR(255) | Yes | Date and time of the count in string format |
| hour | INT | Yes | Hour of the day when the count was taken (1-24) |
| Region_id | INT | Yes | Numeric identifier for the region |
| Region_name | VARCHAR(255) | Yes | Name of the region |
| Local_authority_name | VARCHAR(255) | Yes | Name of the local authority area |
| Road_name | VARCHAR(255) | Yes | Name or identifier of the road |
| Road_Category_ID | INT | Yes | Numeric identifier for road category (1-5) |
| Start_junction_road_name | VARCHAR(255) | Yes | Name of the road at the start junction |
| End_junction_road_name | VARCHAR(255) | Yes | Name of the road at the end junction |
| Latitude | DOUBLE | Yes | Geographic latitude coordinate of the count point |
| Longitude | DOUBLE | Yes | Geographic longitude coordinate of the count point |
| Link_length_km | DOUBLE | Yes | Length of the road link in kilometers |
| Pedal_cycles | INT | Yes | Count of pedal cycles (bicycles) |
| Two_wheeled_motor_vehicles | INT | Yes | Count of two-wheeled motor vehicles (motorcycles) |
| Cars_and_taxis | INT | Yes | Count of cars and taxis |
| Buses_and_coaches | INT | Yes | Count of buses and coaches |
| LGV_Type | INT | Yes | Count of Light Goods Vehicles |
| HGV_Type | INT | Yes | Count of Heavy Goods Vehicles |
| EV_Car | INT | Yes | Count of electric cars |
| EV_Bike | INT | Yes | Count of electric bikes |
| Extract_Time | TIMESTAMP | No | Timestamp when the record was extracted/ingested |
| Electrics_Vehicles_Count | INT | No | Sum of EV_Car + EV_Bike |
| Motor_Vehicles_Count | INT | No | Sum of all motor vehicle types |
| Transformed_Time | TIMESTAMP | No | Timestamp when the record was transformed in the silver layer |
| **Vehicle_Indensity** | DOUBLE | No | **Calculated:** Motor_Vehicles_Count / Link_length_km (vehicles per kilometer) |
| **Loading_Time** | TIMESTAMP | No | **Calculated:** Timestamp when the record was loaded into the gold layer |

**Business Metrics:**
- Vehicle_Indensity: Measures traffic density per kilometer of road, useful for congestion analysis

---

### 3.2 gold_roads

**Description:** Final analytical-ready road data. Optimized for reporting and analytics.

**Storage Location:** `{gold_url}gold_roads/`  
**Table Format:** Delta Lake  
**Catalog:** `traffic_{environment}_catalog.traffic_db.gold_roads`

**Source:** Transformed from `silver_roads` table

**Note:** This table contains the same schema as `silver_roads` with no additional transformations in the gold layer.

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| Road_ID | INT | No | Unique identifier for each road record |
| Road_Category_Id | INT | Yes | Numeric identifier for road category (1-5) |
| Road_Category | VARCHAR(255) | Yes | Road category code (e.g., 'TM', 'TA', 'PA', 'PM', 'M') |
| Region_ID | INT | Yes | Numeric identifier for the region |
| Region_Name | VARCHAR(255) | Yes | Name of the region |
| Total_Link_Length_Km | DOUBLE | Yes | Total length of road links in kilometers |
| Total_Link_Length_Miles | DOUBLE | Yes | Total length of road links in miles |
| All_Motor_Vehicles | DOUBLE | Yes | Total count of all motor vehicles on the road |
| Road_Category_Name | VARCHAR(255) | No | Full name of road category |
| Road_Type | VARCHAR(255) | No | Classification as 'Major' or 'Minor' |

---

## Data Quality Tables

### 4.1 data_quality_quarantine

**Description:** Quarantine table for records that fail data quality validation during ingestion. Used for error tracking and data recovery.

**Storage Location:** Unity Catalog Volume  
**Table Format:** Delta Lake  
**Catalog:** `traffic_{environment}_catalog.gx.data_quality_quarantine`

**Purpose:** Stores records that violate Great Expectations validation rules, allowing for review and potential recovery.

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| table_name | VARCHAR(255) | No | Name of the source table where the violation occurred (e.g., 'raw_traffic', 'raw_roads') |
| gx_batch_id | VARCHAR(255) | No | Batch identifier from Great Expectations validation run |
| violated_rules | VARCHAR(255) | No | Description of the validation rules that were violated (e.g., '[Count_date] expect_column_values_to_match_strftime_format') |
| raw_data | STRING | No | JSON string containing the original record data that failed validation |
| ingestion_time | TIMESTAMP | No | Timestamp when the record was quarantined |

**Common Violation Types:**
- Column format mismatches (e.g., date format)
- Null value violations
- Value range violations
- Table-level schema/count errors

**Recovery Process:**
- Records can be reviewed and corrected
- Corrected records can be re-ingested into the target table
- Violated rules are tracked for data quality monitoring

---

## Schema Evolution Notes

- All tables use Delta Lake format, supporting schema evolution
- Schema merging is enabled with `mergeSchema: true` option
- New columns can be added without breaking existing queries
- Data quality validation ensures schema compliance at ingestion

## Environment Configuration

All tables are created in the catalog: `traffic_{environment}_catalog.traffic_db`
- Replace `{environment}` with: `dev`, `uat`, or `prod`
- Storage paths are dynamically configured based on the selected environment

---

**Document Version:** 1.0  


