/********************************************************************
    NYC Green Taxi Trip Records - Data Cleaning & Transformation Script
    Database: CABDATA
    Table   : project.green_taxi_trip_records
********************************************************************/

-- 1. Select the active database to work on
CREATE DATABASE CABDATA;
USE CABDATA;

-- 2. Preview the first 50 records of the original dataset for initial inspection
SELECT * 
FROM green_taxi_trip_records
LIMIT 50;

/********************************************************************
    STEP 1: Remove irrelevant columns
    - VendorID: This column only identifies the data provider and does not contribute to analysis.
    - ehail_fee: Contains only NULL values, offering no useful information.
    - store_and_fwd_flag: This field lacks meaningful data for the current analytical goals.
*********************************************************************/
ALTER TABLE green_taxi_trip_records
DROP COLUMN VendorID,
DROP COLUMN store_and_fwd_flag,
DROP COLUMN ehail_fee;

/*******************************************************************
	Step 2: 						Data Cleaning
********************************************************************/

	/********************************************************************
		STEP 2a: Remove invalid trips
		- Remove records with identical pickup and dropoff timestamps as they indicate no trip.
		- Remove records with zero trip duration but non-zero distance which indicate data inconsistencies.
	*********************************************************************/
		DELETE FROM green_taxi_trip_records 
		WHERE lpep_pickup_datetime = lpep_dropoff_datetime;

		DELETE FROM green_taxi_trip_records 
		WHERE (trip_duration_in_min = 0 AND trip_distance <> 0);


	/***********************************************************************************
		STEP 2b: Explore data anomalies related to identical pickup and dropoff locations
	************************************************************************************/
    
		SELECT * 
		FROM green_taxi_trip_records
		WHERE PULocationID = DOLocationID
		ORDER BY trip_distance DESC;
        
        DELETE FROM 
        green_taxi_trip_records
        WHERE PULocationID = DOLocationID AND trip_distance > 50  ;
        
	/********************************************************************
		STEP 2c: Handle negative in financial fields
		- Identify and correct negative amounts which likely represent data entry errors by converting them to positive.
	*********************************************************************/
		SELECT * FROM green_taxi_trip_records
		WHERE tip_amount < 0 
		   OR fare_amount <= 0 
		   OR mta_tax < 0
           OR extra < 0 
		   OR improvement_surcharge < 0 
		   OR total_amount <= 0 
		   OR congestion_surcharge < 0 
		   OR cbd_congestion_fee < 0;
		
        SELECT COUNT(*) AS negative_rows FROM green_taxi_trip_records
		WHERE tip_amount < 0 
		   OR fare_amount < 0 
		   OR mta_tax < 0
           OR extra < 0 
		   OR improvement_surcharge < 0 
		   OR total_amount < 0 
		   OR congestion_surcharge < 0 
		   OR cbd_congestion_fee < 0;
        
		-- Fix Negative Fare Amounts
		UPDATE green_taxi_trip_records
		SET fare_amount = CASE WHEN fare_amount < 0 THEN -1 * fare_amount ELSE fare_amount END,
			extra = CASE WHEN extra < 0 THEN -1 * extra ELSE extra END,
			mta_tax = CASE WHEN mta_tax  < 0  THEN -1 * mta_tax ELSE mta_tax END,
			tip_amount = CASE WHEN tip_amount  < 0  THEN -1 * tip_amount ELSE tip_amount END,
			tolls_amount = CASE WHEN tolls_amount  < 0  THEN -1 * tolls_amount ELSE tolls_amount END,
            total_amount  = CASE WHEN total_amount < 0 THEN -1 * total_amount ELSE total_amount END,
			improvement_surcharge = CASE WHEN improvement_surcharge  < 0  THEN -1 * improvement_surcharge ELSE improvement_surcharge END,
            congestion_surcharge = CASE WHEN congestion_surcharge < 0 THEN -1 * congestion_surcharge ELSE congestion_surcharge END,
            cbd_congestion_fee = CASE WHEN cbd_congestion_fee < 0 THEN -1 * cbd_congestion_fee ELSE cbd_congestion_fee END;

	/********************************************************************
		STEP 2d: Passenger and Payment Type Validations and Imputations
		- Investigate unrealistic passenger counts and values outside valid range.
		- Impute missing or invalid passenger counts (0, 8, 9, and '\N') with 1, as majority trips are single passenger.
	*********************************************************************/
	-- Check passenger counts for anomalies
		SELECT * 
		FROM green_taxi_trip_records 
		ORDER BY passenger_count DESC;

	
	-- Analyze distributions of key categorical and numeric columns for data understanding
		SELECT passenger_count, COUNT(*) / (SELECT COUNT(*) FROM green_taxi_trip_records) AS proportion
		FROM green_taxi_trip_records
		GROUP BY passenger_count;

		SELECT payment_type, COUNT(*) / (SELECT COUNT(*) FROM green_taxi_trip_records) AS proportion
		FROM green_taxi_trip_records
		GROUP BY payment_type;

		SELECT RatecodeID, COUNT(*) / (SELECT COUNT(*) FROM green_taxi_trip_records) AS proportion
		FROM green_taxi_trip_records
		GROUP BY RatecodeID;

		SELECT congestion_surcharge, COUNT(*) / (SELECT COUNT(*) FROM green_taxi_trip_records) AS proportion
		FROM green_taxi_trip_records
		GROUP BY congestion_surcharge;

		SELECT cbd_congestion_fee, COUNT(*) / (SELECT COUNT(*) FROM green_taxi_trip_records) AS proportion
		FROM green_taxi_trip_records
		GROUP BY cbd_congestion_fee;

		SELECT PULocationID, COUNT(*) / (SELECT COUNT(*) FROM green_taxi_trip_records) AS proportion
		FROM green_taxi_trip_records
		GROUP BY PULocationID;

		SELECT DOLocationID, COUNT(*) / (SELECT COUNT(*) FROM green_taxi_trip_records) AS proportion
		FROM green_taxi_trip_records
		GROUP BY DOLocationID;

		SELECT ROUND(avg(trip_distance),2) AS mean_distance
		FROM green_taxi_trip_records;

		SELECT ROUND(avg(fare_amount),2) AS mean_fare
		FROM green_taxi_trip_records;

		SELECT extra, COUNT(*) / (SELECT COUNT(*) FROM green_taxi_trip_records) AS proportion
		FROM green_taxi_trip_records
		GROUP BY extra;
        
        SELECT ROUND(avg(extra),2) mean_avg FROM 
        green_taxi_trip_records;

		SELECT mta_tax, COUNT(*) / (SELECT COUNT(*) FROM green_taxi_trip_records) AS proportion
		FROM green_taxi_trip_records
		GROUP BY mta_tax;

		SELECT ROUND(avg(tip_amount),2) AS mean_tip_amount
		FROM green_taxi_trip_records ;

		SELECT ROUND(avg(tolls_amount),2) AS mean_toll_amount
		FROM green_taxi_trip_records;

		SELECT improvement_surcharge, COUNT(*) / (SELECT COUNT(*) FROM green_taxi_trip_records) AS proportion
		FROM green_taxi_trip_records
		GROUP BY improvement_surcharge;

		SELECT ROUND(AVG(total_amount),2) AS mean_total_amount
		FROM green_taxi_trip_records ;

		SELECT trip_type, COUNT(*) / (SELECT COUNT(*) FROM green_taxi_trip_records) AS proportion
		FROM green_taxi_trip_records
		GROUP BY trip_type;

		UPDATE green_taxi_trip_records
		SET 
			passenger_count = CASE 
				WHEN passenger_count = 0 OR passenger_count = '\\N' OR passenger_count = 8 OR passenger_count = 9 THEN 1
				ELSE passenger_count 
			END,
			
			payment_type = CASE 
				WHEN payment_type = '\\N' THEN 1
				ELSE payment_type 
			END,
			
			RatecodeID = CASE 
				WHEN RatecodeID = 99 OR RatecodeID = '\\N' THEN 1
				ELSE RatecodeID 
			END,
			
			congestion_surcharge = CASE 
				WHEN congestion_surcharge = '\\N' THEN 0
				ELSE congestion_surcharge 
			END,
			
			cbd_congestion_fee = CASE 
				WHEN cbd_congestion_fee = '\\N' THEN 0
				ELSE cbd_congestion_fee 
			END,
			
			trip_type = CASE 
				WHEN trip_type = '\\N' THEN 1
				ELSE trip_type 
			END;
            
             
	/********************************************************************
		STEP 3a: Feature Engineering
		Add new derived columns to enable richer analytical insights:
		- trip_duration_in_min: Trip duration in minutes for temporal analysis.
		- pickup_day, drop_day: To analyze daily trip patterns.
		- Day_time: Categorizes trip time of day for trend detection.
		- ride_type: Classify rides by duration categories for segmentation.
	*********************************************************************/


		ALTER TABLE green_taxi_trip_records
		-- Add trip duration in minutes after dropoff datetime
		ADD COLUMN trip_duration_in_min INT AFTER lpep_dropoff_datetime,
		-- Add pickup day after trip duration
		ADD COLUMN pickup_day VARCHAR(25) AFTER trip_duration_in_min,
		-- Add drop day after pickup day
		ADD COLUMN drop_day VARCHAR(25) AFTER pickup_day,
		-- Add time of day category after drop day
		ADD COLUMN Day_time VARCHAR(25) AFTER drop_day,
		-- Add ride type classification after time of day
		ADD COLUMN ride_type VARCHAR(25) AFTER Day_time,
		-- Add average speed (distance/duration) after ride type
		ADD COLUMN speed FLOAT AFTER trip_duration_in_min ;

	/********************************************************************
		STEP 3b: Classify ride types based on trip duration_in_min
		Segments rides into meaningful groups to facilitate analysis of trip length patterns.
	*********************************************************************/
		UPDATE green_taxi_trip_records
		SET 
			trip_duration_in_min = ROUND(TIMESTAMPDIFF(SECOND, lpep_pickup_datetime, lpep_dropoff_datetime)/60,2),
			pickup_day = DAYNAME(lpep_pickup_datetime),
			drop_day = DAYNAME(lpep_dropoff_datetime),
			Day_time = CASE
				WHEN HOUR(lpep_pickup_datetime) BETWEEN 0 AND 1   THEN 'Midnight'
				WHEN HOUR(lpep_pickup_datetime) BETWEEN 1 AND 4   THEN 'Late Night'
				WHEN HOUR(lpep_pickup_datetime) BETWEEN 5 AND 8   THEN 'Early Morning'
				WHEN HOUR(lpep_pickup_datetime) BETWEEN 8 AND 10  THEN 'Morning Rush'
				WHEN HOUR(lpep_pickup_datetime) BETWEEN 10 AND 12 THEN 'Late Morning'
				WHEN HOUR(lpep_pickup_datetime) BETWEEN 12 AND 16 THEN 'Early Afternoon'
				WHEN HOUR(lpep_pickup_datetime) BETWEEN 16 AND 20 THEN 'Evening Rush'
				WHEN HOUR(lpep_pickup_datetime) BETWEEN 20 AND 23 THEN 'Early Night'
				ELSE 'Night'
			END,
			ride_type = CASE
				WHEN TIMESTAMPDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) BETWEEN 0 AND 5 THEN 'Micro Trip'
				WHEN TIMESTAMPDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) BETWEEN 6 AND 15 THEN 'Quick Trip'
				WHEN TIMESTAMPDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) BETWEEN 16 AND 30 THEN 'Short Haul'
				WHEN TIMESTAMPDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) BETWEEN 31 AND 60 THEN 'Standard Ride'
				WHEN TIMESTAMPDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) BETWEEN 61 AND 120 THEN 'Long Ride'
				WHEN TIMESTAMPDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) BETWEEN 121 AND 240 THEN 'Extended Ride'
				WHEN TIMESTAMPDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) BETWEEN 241 AND 720 THEN 'Day Trip'
				WHEN TIMESTAMPDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) BETWEEN 721 AND 1440 THEN 'Full-Day Ride'
				ELSE 'Multi-Day Ride'
			END,
            speed= ROUND(COALESCE(trip_distance*60/NULLIF(trip_duration_in_min,0),0),2);
	/***************************************************************************
		Step 4:	  OUTLIER DETECTTTION + TREATMENT
	******************************************************************************/
		SELECT * FROM green_taxi_trip_records;


		-- Checking Values above 99 percentile
		WITH ordered AS (
			SELECT trip_distance,
				   ROW_NUMBER() OVER (ORDER BY trip_distance) AS rn
			FROM green_taxi_trip_records
		),
		stats AS (
			SELECT COUNT(*) AS N FROM ordered
		),
		rank_calc AS (
			SELECT 0.99 * (N - 1) + 1 AS R FROM stats
		),
		bounds AS (
			SELECT o.trip_distance,
				   o.rn,
				   r.R
			FROM ordered o
			JOIN rank_calc r
			  ON o.rn = FLOOR(r.R) OR o.rn = CEIL(r.R)
		)
		SELECT
			CASE
				WHEN MIN(rn) = MAX(rn) THEN MAX(trip_distance)
				ELSE MIN(trip_distance) +
					 ((MAX(R) - FLOOR(MAX(R))) *
					  (MAX(trip_distance) - MIN(trip_distance)))
			END AS p99
		FROM bounds;
        
        
     -- Check If there is any genuiene outlier present in trip_distance
		 select ROUND(count(*)*100/(select count(*) from green_taxi_trip_records),2) pct 
		 from green_taxi_trip_records
		 where trip_distance > 15;
		 
		 select * from green_taxi_trip_records
		 where trip_distance > 15
		 order by trip_distance desc ;
		 
     -- Delete records with trip_distance > 100 
     
		 DELETE FROM green_taxi_trip_records
		 WHERE trip_distance > 100;
     
	-- Checking Values above 99 percentile wrt to trip_duration_in_min
		WITH ordered AS (
			SELECT trip_duration_in_min,
				   ROW_NUMBER() OVER (ORDER BY trip_duration_in_min) AS rn
			FROM green_taxi_trip_records
		),
		stats AS (
			SELECT COUNT(*) AS N FROM ordered
		),
		rank_calc AS (
			SELECT 0.99 * (N - 1) + 1 AS R FROM stats
		),
		bounds AS (
			SELECT o.trip_duration_in_min,
				   o.rn,
				   r.R
			FROM ordered o
			JOIN rank_calc r
			  ON o.rn = FLOOR(r.R) OR o.rn = CEIL(r.R)
		)
		SELECT
			CASE
				WHEN MIN(rn) = MAX(rn) THEN MAX(trip_duration_in_min)
				ELSE MIN(trip_duration_in_min) +
					 ((MAX(R) - FLOOR(MAX(R))) *
					  (MAX(trip_duration_in_min) - MIN(trip_duration_in_min)))
			END AS p99
		FROM bounds;
        
        -- Check if the outlier are geneuine 
        SELECT * FROM green_taxi_trip_records
        WHERE trip_duration_in_min > 62
        order by trip_duration_in_min desc;
        
        DELETE  FROM green_taxi_trip_records
        WHERE trip_duration_in_min > 62; 
        
		-- Checking Values above 99 percentile wrt to trip_duration_in_min
			WITH ordered AS (
				SELECT trip_duration_in_min,
					   ROW_NUMBER() OVER (ORDER BY trip_duration_in_min) AS rn
				FROM green_taxi_trip_records
			),
			stats AS (
				SELECT COUNT(*) AS N FROM ordered
			),
			rank_calc AS (
				SELECT 0.01 * (N - 1) + 1 AS R FROM stats
			),
			bounds AS (
				SELECT o.trip_duration_in_min,
					   o.rn,
					   r.R
				FROM ordered o
				JOIN rank_calc r
				  ON o.rn = FLOOR(r.R) OR o.rn = CEIL(r.R)
			)
			SELECT
				CASE
					WHEN MIN(rn) = MAX(rn) THEN MAX(trip_duration_in_min)
					ELSE MIN(trip_duration_in_min) +
						 ((MAX(R) - FLOOR(MAX(R))) *
						  (MAX(trip_duration_in_min) - MIN(trip_duration_in_min)))
				END AS p99
			FROM bounds;
            
            
            -- Data Entry Error
				SELECT * FROM green_taxi_trip_records
				WHERE trip_duration_in_min = 0 AND trip_distance <> 0;
			
            -- Delete Such Records
				DELETE FROM green_taxi_trip_records
				WHERE  trip_duration_in_min = 0 AND trip_distance <> 0;
			
            -- Records Where trip_duration and distance both are zero 
				SELECT * FROM green_taxi_trip_records
                WHERE trip_duration_in_min = 0 AND trip_distance = 0 AND fare_amount <> 3;
                
                -- Delete these data anamolies
                DELETE FROM green_taxi_trip_records
                WHERE trip_duration_in_min = 0 AND trip_distance = 0 AND fare_amount <> 3;
                
			-- See How speed varies
				SELECT ride_type , min(speed) , avg(speed) , max(speed)  FROM green_taxi_trip_records
				group by ride_type;
				/* Observations on Speed Column:
				   - The maximum speed values seem unrealistic. For example:
					   • Quick Trip (6–15 min) shows speeds up to ~212 mph, which is highly unlikely.
					   • Micro Trip (0–5 min) shows speeds as high as ~360 mph, which is practically impossible.
					   • Short Haul trips also indicate overspeeding anomalies.
				   - These extremely high values are likely due to data quality issues such as incorrect timestamps,
					 wrongly recorded trip distances, or outliers in the dataset.
				   - On the other hand, very low speeds (close to zero) can be justified in cases of heavy traffic
					 or waiting time. However, they will also be examined further to rule out anomalies.
				   - Next step: Perform anomaly detection / filtering on speed values to clean the dataset.
				*/
                
                SELECT * FROM green_taxi_trip_records
                WHERE ride_type = 'Quick Trip'
                ORDER BY  speed DESC ,trip_distance DESC  , trip_duration_in_min DESC  ;
                
                DELETE FROM green_taxi_trip_records
                WHERE ride_type = "Quick Trip" AND speed > 80;
                
				SELECT * FROM green_taxi_trip_records
                WHERE ride_type = 'Micro Trip' AND trip_duration_in_min = 1
                ORDER BY speed DESC ,trip_distance DESC    ;
                
                SELECT ROUND(COUNT(*)*100/(SELECT COUNT(*) FROM green_taxi_trip_records),2)
				FROM green_taxi_trip_records
				WHERE ride_type = 'Micro Trip' AND speed > 25;
				/* About 0.25% of the trips have speeds going well above the city driving limits.
                In a real city scenario, driving at such high speeds in such  short rides is  just impractical(also speed limit is 25 mph)
				   These extremely high values are unrealistic (likely due to data entry or GPS errors).
				   Since they don’t contribute meaningful information for analysis or modeling,
				   it’s better to delete them from the dataset. */
                   
				DELETE FROM green_taxi_trip_records
                WHERE ride_type = 'Micro Trip' AND speed > 25;
                
                
				SELECT * FROM green_taxi_trip_records
                WHERE ride_type = 'Short Haul'
                ORDER BY  trip_distance ASC  ,speed DESC , trip_duration_in_min DESC  ;
  
/*******************************************************
            Pickup and Drop Zone Analysis
                
**********************************************************/

-- Most Busiest Pickup Zone 
-- Most Busiest Drop Off Zone
-- Deas Pickup And Drop off Zone
-- Most Busiest Pickup Zone / Dropoff Zone wrt to day_time 
-- Most Busiest Route 
-- Least Used Route
-- Weekely Route And Pickup zone Analysis
-- Weekdays VS Weekend Route Trend
-- How Trips  With Same Pickup And Drop off Zone 
-- Kya koi aisa pickup zone hai jisme jyadatar routes round trip ho (hotzone kya hai mainly)


/*******************************************************
                Pickup and Drop Zone Analysis
***********************************************************/


/***************** 1. Top Pickup Zones *****************/
-- Insight: Which pickup zones generate the highest trip volume?
SELECT 
    tz.zone AS pickup_zone,
    gt.PULocationID,
    COUNT(*) AS trip_volume
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON gt.PULocationID = tz.LocationID
GROUP BY tz.zone, gt.PULocationID
ORDER BY trip_volume DESC
LIMIT 10;


/***************** 2. Top Drop-off Zones *****************/
-- Insight: Which drop-off zones are the busiest?
SELECT 
    tz.zone AS dropoff_zone,
    gt.DOLocationID,
    COUNT(*) AS trip_volume
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON gt.DOLocationID = tz.LocationID
GROUP BY tz.zone, gt.DOLocationID
ORDER BY trip_volume DESC
LIMIT 10;


/***************** 3. Least Frequented Zones *****************/
-- (a) Rare Pickup Zones
SELECT 
    tz.zone AS pickup_zone,
    gt.PULocationID,
    COUNT(*) AS trip_volume
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON gt.PULocationID = tz.LocationID
GROUP BY tz.zone, gt.PULocationID
HAVING trip_volume = 1;

-- (b) Rare Drop-off Zones
SELECT 
    tz.zone AS dropoff_zone,
    gt.DOLocationID,
    COUNT(*) AS trip_volume
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON gt.DOLocationID = tz.LocationID
GROUP BY tz.zone, gt.DOLocationID
HAVING trip_volume = 1;


/***************** 4. Peak Routes by Time of Day *****************/
-- Insight: Most frequent pickup-dropoff pair for each time-of-day segment.
SELECT 
    Day_time, 
    pickup_zone, 
    dropoff_zone,
    trip_volume
FROM (
    SELECT 
        gt.Day_time,
        tz1.zone AS pickup_zone,
        tz2.zone AS dropoff_zone,
        COUNT(*) AS trip_volume,
        ROW_NUMBER() OVER (PARTITION BY gt.Day_time ORDER BY COUNT(*) DESC) AS rn
    FROM green_taxi_trip_records gt
    JOIN taxi_zone_lookup tz1 ON gt.PULocationID = tz1.LocationID
    JOIN taxi_zone_lookup tz2 ON gt.DOLocationID = tz2.LocationID
    GROUP BY gt.Day_time, tz1.zone, tz2.zone
) t
WHERE rn <= 2;


/***************** 5. Top Origin–Destination Routes *****************/
-- Insight: Most frequent pickup–dropoff pairs overall.
SELECT 
    tz1.zone AS pickup_zone,
    tz2.zone AS dropoff_zone,
    COUNT(*) AS trip_volume
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz1 ON gt.PULocationID = tz1.LocationID
JOIN taxi_zone_lookup tz2 ON gt.DOLocationID = tz2.LocationID
GROUP BY tz1.zone, tz2.zone
ORDER BY trip_volume DESC
LIMIT 10;


/***************** 6. Low-Traffic Routes *****************/
-- Insight: Rarely used or one-off routes.
SELECT 
    tz1.zone AS pickup_zone,
    tz2.zone AS dropoff_zone,
    COUNT(*) AS trip_volume
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz1 ON gt.PULocationID = tz1.LocationID
JOIN taxi_zone_lookup tz2 ON gt.DOLocationID = tz2.LocationID
GROUP BY tz1.zone, tz2.zone
HAVING COUNT(*) <= 1;

-- Count of such rare routes:
SELECT COUNT(*) AS low_frequency_routes
FROM (
    SELECT 
        tz1.zone AS pickup_zone,
        tz2.zone AS dropoff_zone,
        COUNT(*) AS trip_volume
    FROM green_taxi_trip_records gt
    JOIN taxi_zone_lookup tz1 ON gt.PULocationID = tz1.LocationID
    JOIN taxi_zone_lookup tz2 ON gt.DOLocationID = tz2.LocationID
    GROUP BY tz1.zone, tz2.zone
    HAVING COUNT(*) <= 1
) t;


/***************** 7. Weekly Route Trends *****************/
-- (a) Most Frequent Pickup Zone per Week
SELECT pickup_zone, year, week_number, trip_volume
FROM (
    SELECT 
        tz.zone AS pickup_zone,
        YEAR(gt.lpep_pickup_datetime) AS year,
        WEEK(gt.lpep_pickup_datetime, 1) AS week_number,
        COUNT(*) AS trip_volume,
        ROW_NUMBER() OVER (
            PARTITION BY YEAR(gt.lpep_pickup_datetime), WEEK(gt.lpep_pickup_datetime, 1)
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM green_taxi_trip_records gt
    JOIN taxi_zone_lookup tz ON gt.PULocationID = tz.LocationID
    GROUP BY tz.zone, year, week_number
) t
WHERE rn = 1;

-- (b) Most Frequent Route per Week (Year Ignored)
SELECT pickup_zone, dropoff_zone, week_number, trip_volume
FROM (
    SELECT 
        tz1.zone AS pickup_zone,
        tz2.zone AS dropoff_zone,
        WEEK(gt.lpep_pickup_datetime,1) AS week_number,
        COUNT(*) AS trip_volume,
        ROW_NUMBER() OVER (
            PARTITION BY WEEK(gt.lpep_pickup_datetime,1) 
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM green_taxi_trip_records gt
    JOIN taxi_zone_lookup tz1 ON tz1.LocationID = gt.PULocationID
    JOIN taxi_zone_lookup tz2 ON tz2.LocationID = gt.DOLocationID 
    GROUP BY tz1.zone, tz2.zone, week_number
) t
WHERE rn = 1;


/***************** 8. Weekday vs Weekend Patterns *****************/
-- (a) Busiest Days for Trips
SELECT day_type, day_name, trip_volume
FROM (
    SELECT
        CASE WHEN DAYOFWEEK(gt.lpep_pickup_datetime) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        DAYNAME(gt.lpep_pickup_datetime) AS day_name,
        COUNT(*) AS trip_volume,
        ROW_NUMBER() OVER (
            PARTITION BY CASE WHEN DAYOFWEEK(gt.lpep_pickup_datetime) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM green_taxi_trip_records gt
    GROUP BY day_type, day_name
) t
WHERE rn = 1;

-- (b) Most Frequent Route by Day Type
SELECT day_type, pickup_zone, dropoff_zone, trip_volume
FROM (
    SELECT 
        CASE WHEN DAYOFWEEK(gt.lpep_pickup_datetime) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        tz1.zone AS pickup_zone,
        tz2.zone AS dropoff_zone,
        COUNT(*) AS trip_volume,
        ROW_NUMBER() OVER (
            PARTITION BY CASE WHEN DAYOFWEEK(gt.lpep_pickup_datetime) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM green_taxi_trip_records gt
    JOIN taxi_zone_lookup tz1 ON tz1.LocationID = gt.PULocationID
    JOIN taxi_zone_lookup tz2 ON tz2.LocationID = gt.DOLocationID
    GROUP BY day_type, pickup_zone, dropoff_zone
) t
WHERE rn = 1;


/***************** 9. Round-Trip Ride Analysis *****************/
-- How Many Round Trips Are There  
SELECT 
	COUNT(*) trip_volume
FROM green_taxi_trip_records
WHERE PULocationID = DOLocationID;
-- Insight: Rides where pickup and dropoff zones are the same.
SELECT 
    tz.zone AS pickup_zone,
    COUNT(*) AS total_round_trips
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON gt.PULocationID = tz.LocationID 
   AND gt.DOLocationID = tz.LocationID
GROUP BY tz.zone
ORDER BY total_round_trips DESC;


/***************** 10. Daily Tips, Passengers & Free Trips *****************/
SELECT 
    pickup_day,
    SUM(passenger_count) AS total_passengers,
    ROUND(AVG(passenger_count),2) AS avg_passengers_per_trip,
    ROUND(SUM(tip_amount),2) AS total_tips,
    ROUND(SUM(IF(tip_amount > 0, 1, 0)) * 100 / COUNT(*), 2) AS pct_trips_with_tips,
    ROUND(SUM(IF(payment_type = 3, 1, 0)) * 100 / COUNT(*), 2) AS pct_free_trips,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records
GROUP BY pickup_day
ORDER BY FIELD(pickup_day,'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');


/***************** 11. Popular Ride Type by Day Type *****************/
SELECT day_type, ride_type, trip_volume
FROM (
    SELECT 
        CASE WHEN DAYOFWEEK(lpep_pickup_datetime) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        ride_type,
        COUNT(*) AS trip_volume,
        ROW_NUMBER() OVER (
            PARTITION BY CASE WHEN DAYOFWEEK(lpep_pickup_datetime) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM green_taxi_trip_records
    GROUP BY day_type, ride_type
) t
WHERE rn = 1;


/***************** 12. Toll Amounts: Weekday vs Weekend *****************/
SELECT 
    CASE WHEN DAYOFWEEK(lpep_pickup_datetime) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    ROUND(SUM(tolls_amount), 2) AS total_toll_amount,
    ROUND(AVG(tolls_amount), 2) AS avg_toll_per_trip,
    ROUND(SUM(CASE WHEN tolls_amount <> 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_trips_with_tolls
FROM green_taxi_trip_records
GROUP BY day_type;


/***************** 13. Revenue Contribution by Zone *****************/
SELECT 
    tz.zone AS pickup_zone,
    ROUND(SUM(gt.total_amount),2) AS total_revenue,
    ROUND(AVG(gt.total_amount),2) AS avg_revenue_per_trip,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz ON tz.LocationID = gt.PULocationID
WHERE gt.payment_type <> 3
GROUP BY tz.zone
ORDER BY total_revenue DESC;


/***************** 14. Busiest Hour by Day *****************/
SELECT pickup_day, pickup_hour, trip_count
FROM (
    SELECT 
        DAYNAME(lpep_pickup_datetime) AS pickup_day,
        HOUR(lpep_pickup_datetime) AS pickup_hour,
        COUNT(*) AS trip_count,
        ROW_NUMBER() OVER (
            PARTITION BY DAYNAME(lpep_pickup_datetime) 
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM green_taxi_trip_records
    GROUP BY DAYNAME(lpep_pickup_datetime), HOUR(lpep_pickup_datetime)
) ranked
WHERE rn = 1
ORDER BY FIELD(pickup_day,'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');


/***************** 15. Daily Revenue Analysis *****************/
-- Insight: Share of total revenue by day of week.
SELECT 
    pickup_day,
    ROUND(SUM(total_amount),2) AS total_revenue,
    ROUND(AVG(total_amount),2) AS avg_revenue_per_trip,
    ROUND(SUM(total_amount) * 100 / (SELECT SUM(total_amount) FROM green_taxi_trip_records), 2) AS pct_of_total_revenue,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records
GROUP BY pickup_day
ORDER BY FIELD(pickup_day,'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');

/***********************************************
             DROP DAY BASED ANALYSIS
   (Only for trips where pickup_day <> drop_day)
************************************************/
-- 1. Average trip duration for cross-day trips.
	SELECT 
		ROUND(AVG(trip_duration_in_min),2) AS avg_cross_day_duration
	FROM green_taxi_trip_records
	WHERE pickup_day <> drop_day;

-- 2. Common ride types used for cross-day trips.
	SELECT 
		ride_type ,
        COUNT(*) trip_volume 
	FROM green_taxi_trip_records
	WHERE pickup_day <> drop_day
    GROUP BY ride_type 
    ORDER BY trip_volume DESC;

-- 3. Popular pickup–dropoff locations for cross-day trips.
	SELECT 
		PULocationID ,
        COUNT(*) trip_volume
	FROM green_taxi_trip_records
	WHERE pickup_day <> drop_day
    GROUP BY PULocationID
    ORDER BY trip_volume DESC;

SELECT  * FROM green_taxi_trip_records;

/***********************************************
         PASSENGER COUNT BASED ANALYSIS
************************************************/

/***************** 1. Ride Volume by Passenger Count *****************/
-- Insight: Which passenger counts are most common (solo vs shared)?
SELECT 
    passenger_count AS num_passengers,
    COUNT(*) AS trip_volume
FROM green_taxi_trip_records
GROUP BY passenger_count
ORDER BY num_passengers;


/***************** 2. Revenue by Passenger Count *****************/
-- Insight: How does average trip revenue vary with passenger count?
SELECT 
    passenger_count AS num_passengers,
    COUNT(*) AS total_trips,
    ROUND(AVG(total_amount), 2) AS avg_revenue_per_trip
FROM green_taxi_trip_records
WHERE passenger_count > 0 AND payment_type <> 3
GROUP BY passenger_count
ORDER BY num_passengers;


/***************** 3. Preferred Ride Type by Passenger Count *****************/
-- Insight: Which ride type is most popular for each passenger group?
SELECT 
    passenger_count AS num_passengers,
    ride_type,
    trip_volume
FROM (
    SELECT 
        passenger_count,
        ride_type,
        COUNT(*) AS trip_volume,
        ROW_NUMBER() OVER (
            PARTITION BY passenger_count 
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM green_taxi_trip_records
    GROUP BY passenger_count, ride_type
) ranked
WHERE rn = 1
ORDER BY num_passengers;


/***************** 4. Passenger Count vs Pickup Day *****************/
-- Insight: On which days are rides with certain passenger counts more frequent?
SELECT 
    pickup_day,
    passenger_count AS num_passengers,
    COUNT(*) AS trip_volume
FROM green_taxi_trip_records
GROUP BY pickup_day, passenger_count
ORDER BY pickup_day, num_passengers;


/***************** 5. Zone-Specific Demand *****************/
-- Insight: Which zones dominate for each passenger count?
SELECT 
    num_passenger,
    pickup_zone
FROM (
    SELECT 
        passenger_count AS num_passenger,
        tz.zone AS pickup_zone,
        ROW_NUMBER() OVER (
            PARTITION BY passenger_count 
            ORDER BY COUNT(*) DESC
        ) rn
    FROM green_taxi_trip_records gt
    JOIN taxi_zone_lookup tz
        ON tz.LocationID = gt.PULocationID
    GROUP BY passenger_count, tz.zone
) t
WHERE rn = 1
ORDER BY num_passenger;


/***************** 6. Passenger Count vs Distance *****************/
-- Insight: How far do passengers typically travel based on group size?
SELECT 
    passenger_count AS num_passengers,
    ROUND(AVG(trip_distance),2) AS avg_distance,
    ROUND(MAX(trip_distance),2) AS max_distance,
    ROUND(MIN(trip_distance),2) AS min_distance,
    COUNT(*) AS trip_volume
FROM green_taxi_trip_records
GROUP BY passenger_count
ORDER BY num_passengers;
/**************************************************
                 TRIP DISTANCE ANALYSIS
         Green Taxi Trip Records + Zone Lookup
**************************************************/

/***************** 1. Distance vs Tips *****************/
-- Insight: Do longer trips bring higher tips?
SELECT 
    CASE 
        WHEN trip_distance <= 2 THEN "Short Distance" 
        WHEN trip_distance > 2 AND trip_distance <= 10 THEN "Medium Distance"
        ELSE "Long Distance" 
    END AS distance_type,
    ROUND(AVG(tip_amount),2) AS avg_tip,
    ROUND(SUM(tip_amount),2) AS total_tip,
    ROUND(SUM(IF(tip_amount > 0,1,0)) * 100 / COUNT(*),2) AS pct_trips_with_tip,
    COUNT(*) AS trip_volume
FROM green_taxi_trip_records
GROUP BY distance_type
ORDER BY FIELD(distance_type,'Short Distance','Medium Distance','Long Distance');


/***************** 2. Fare vs Distance *****************/
-- Insight: Is fare per mile consistent across trips?
SELECT 
    trip_distance,
    fare_amount,
    ROUND(fare_amount / NULLIF(trip_distance,0),2) AS fare_per_mile
FROM green_taxi_trip_records;


/*************** 3. Extra Charges vs Distance ***************/
-- Insight: Do long-distance trips attract more extra charges?
-- 3.1 Breakdown by extra value
SELECT 
    extra ,
    ROUND(AVG(trip_distance),2)  avg_distance ,
    COUNT(*) trip_volume 
FROM green_taxi_trip_records
GROUP BY extra;

-- 3.2 Summarized by distance category
SELECT 
    CASE 
        WHEN trip_distance <= 2 THEN 'Short Distance'
        WHEN trip_distance > 2 AND trip_distance <= 10 THEN 'Medium Distance'
        ELSE 'Long Distance'
    END AS distance_type,
    COUNT(*) AS num_trips,
    ROUND(AVG(extra),2) AS avg_extra,
    ROUND(SUM(extra),2) AS total_extra,
    ROUND(SUM(IF(extra > 0,1,0)) * 100 / COUNT(*),2) AS pct_trips_with_extra
FROM green_taxi_trip_records
GROUP BY distance_type
ORDER BY FIELD(distance_type,'Short Distance','Medium Distance','Long Distance');


/*************** 4. Hotspot Zones by Distance ***************/
-- Insight: Which pickup zones generate mostly short vs long trips?
-- 4.1 Short Distance Hotspots
SELECT 
    tz.zone As pickup_zone,
    COUNT(*) trip_volume 
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz 
    ON tz.LocationID = gt.PULocationID
WHERE gt.trip_distance <= 2
GROUP BY tz.zone
ORDER BY trip_volume DESC;

-- 4.2 Long Distance Hotspots
SELECT 
    tz.zone As pickup_zone,
    COUNT(*) trip_volume 
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz 
    ON tz.LocationID = gt.PULocationID
WHERE gt.trip_distance > 10
GROUP BY tz.zone
ORDER BY trip_volume DESC;


/***************** 5. Time & Day Patterns *****************/
-- Insight: On which days do long trips happen more often?
-- 5.1 Days with more long-distance trips
SELECT 
    pickup_day,
    COUNT(*) trip_volume
FROM green_taxi_trip_records
WHERE trip_distance > 10
GROUP BY pickup_day 
ORDER BY trip_volume DESC LIMIT 5;

-- 5.2 Average & total distance by day
SELECT 
    pickup_day,
    ROUND(AVG(trip_distance),2) avg_distance,
    ROUND(SUM(trip_distance),2) total_distance 
FROM green_taxi_trip_records
GROUP BY pickup_day;


/*************** 6. Trip Distance Distribution ***************/
-- Insight: Distribution of trips by distance categories.
-- 6.1 Overall Distribution
SELECT 
    CASE 
        WHEN trip_distance <= 2 THEN 'Short (0–2 miles)'
        WHEN trip_distance <= 5 THEN 'Medium (3–5 miles)'
        WHEN trip_distance <= 10 THEN 'Long (6–10 miles)'
        ELSE 'Very Long (10+ miles)'
    END AS distance_category,
    ROUND(COUNT(*)*100/(SELECT COUNT(*) FROM green_taxi_trip_records),2) AS pct_of_trips,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM green_taxi_trip_records
WHERE trip_distance > 0
GROUP BY distance_category
ORDER BY pct_of_trips DESC;

-- 6.2 By Day Time
SELECT 
    day_time,
    CASE 
        WHEN trip_distance <= 2 THEN 'Short (0–2 miles)'
        WHEN trip_distance <= 5 THEN 'Medium (3–5 miles)'
        WHEN trip_distance <= 10 THEN 'Long (6–10 miles)'
        ELSE 'Very Long (10+ miles)'
    END AS distance_category,
    ROUND(COUNT(*)*100/(SELECT COUNT(*) FROM green_taxi_trip_records gt2
                        WHERE gt2.day_time = gt1.day_time ),2) AS pct_of_trips,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM green_taxi_trip_records gt1
WHERE trip_distance > 0
GROUP BY day_time, distance_category
ORDER BY day_time, pct_of_trips DESC;


/***************** 7. Special Cases *****************/
-- Insight: Fare per mile in special conditions
-- 7.1 Average fare per mile (overall)
SELECT ROUND(AVG(fare_amount/trip_distance),2) avg_fare_per_mile   
FROM green_taxi_trip_records;

-- 7.2 Out-of-NYC trips (mta_tax = 0)
SELECT 
    ROUND(AVG(fare_amount/trip_distance),2) avg_fare_per_mile   
FROM green_taxi_trip_records
WHERE mta_tax = 0;

-- 7.3 Round trips (same pickup & dropoff)
SELECT 
    ROUND(AVG(fare_amount/trip_distance),2) avg_fare_per_mile   
FROM green_taxi_trip_records
WHERE PULocationID = DOLocationID;


/*************** 8. Round Trip Distance Distribution ***************/
-- Insight: Distance distribution for round trips
SELECT 
    tz.zone AS zone,
    CASE 
        WHEN trip_distance <= 1 THEN 'Short (0–1 miles)'
        WHEN trip_distance <= 5 THEN 'Medium (2–5 miles)'
        WHEN trip_distance <= 10 THEN 'Long (6–10 miles)'
        ELSE 'Very Long (10+ miles)'
    END AS distance_category,
    COUNT(*) AS trip_count,
    ROUND(COUNT(*)*100.0 / SUM(COUNT(*)) OVER (PARTITION BY tz.zone), 2) AS percentage_share
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON gt.PULocationID = tz.LocationID
WHERE gt.PULocationID = gt.DOLocationID   
GROUP BY tz.zone, distance_category
ORDER BY trip_count DESC;


/*************** 9. Distance vs Toll Amounts ***************/
-- Insight: Do longer trips attract higher tolls?
SELECT 
    CASE 
        WHEN trip_distance <= 1 THEN '0–1 miles'
        WHEN trip_distance <= 5 THEN '2–5 miles'
        WHEN trip_distance <= 10 THEN '6–10 miles'
        ELSE '10+ miles'
    END AS distance_category,
    ROUND(AVG(tolls_amount), 2) AS avg_toll,
    COUNT(*) AS trip_count
FROM green_taxi_trip_records
GROUP BY distance_category
ORDER BY MIN(trip_distance);


/*************** 10. Long Distance Hotspot Zones ***************/
-- Insight: Which pickup zones are the biggest long-distance hotspots?
SELECT 
    tz.zone AS pickup_zone,
    COUNT(*) AS long_distance_trips
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON gt.PULocationID = tz.LocationID
WHERE gt.trip_distance > 10
GROUP BY tz.zone
ORDER BY long_distance_trips DESC
LIMIT 10;
/***********************************************
                TRIP DURATION ANALYSIS
************************************************/

-- 1a. Overall Average Trip Duration
SELECT 
    ROUND(AVG(trip_duration_in_min),2) AS avg_duration
FROM green_taxi_trip_records;

-- 1b. Average Trip Duration by Pickup Zone
SELECT
    tz.zone AS pickup_zone,
    ROUND(AVG(trip_duration_in_min),2) AS avg_duration
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON tz.LocationID = gt.PULocationID
GROUP BY tz.zone;

-- 3. Average & Total Trip Duration by Pickup Day
SELECT
    pickup_day,
    ROUND(AVG(trip_duration_in_min),2) AS avg_duration,
    SUM(trip_duration_in_min) AS total_duration
FROM green_taxi_trip_records
GROUP BY pickup_day
ORDER BY FIELD(pickup_day,'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');

-- 4. Ride Type vs Trip Duration
SELECT 
    ride_type,
    COUNT(*) AS trip_volume,
    ROUND(AVG(trip_duration_in_min),2) AS avg_duration_min,
    ROUND(MIN(trip_duration_in_min),2) AS min_duration_min,
    ROUND(MAX(trip_duration_in_min),2) AS max_duration_min,
    ROUND(SUM(trip_duration_in_min),2) AS total_duration_min
FROM green_taxi_trip_records
GROUP BY ride_type
ORDER BY avg_duration_min DESC;

-- 5. Consistency Check: Distance vs Duration
SELECT 
    CEIL(trip_distance) AS distance_bucket,
    COUNT(*) AS trip_volume,
    ROUND(AVG(trip_duration_in_min),2) AS avg_duration_min,
    ROUND(MIN(trip_duration_in_min),2) AS min_duration_min,
    ROUND(MAX(trip_duration_in_min),2) AS max_duration_min
FROM green_taxi_trip_records
WHERE trip_distance > 0
GROUP BY distance_bucket
ORDER BY distance_bucket;

-- 6. Relationship Between Trip Duration & Fare Amount
SELECT 
    ride_type,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(MIN(fare_amount), 2) AS min_fare,
    ROUND(MAX(fare_amount), 2) AS max_fare,
    COUNT(*) AS trip_volume
FROM green_taxi_trip_records
WHERE fare_amount > 0
GROUP BY ride_type
ORDER BY trip_volume DESC;

-- 7. Average Duration for Trips Going Outside NYC
SELECT 
    ROUND(AVG(trip_duration_in_min),2) AS avg_duration
FROM green_taxi_trip_records
WHERE mta_tax = 0;

-- 8. Avg & Total Fare by Trip Duration Bucket
SELECT 
    CASE
        WHEN trip_duration_in_min BETWEEN 0 AND 5 THEN '0-5 min'
        WHEN trip_duration_in_min BETWEEN 6 AND 15 THEN '6-15 min'
        WHEN trip_duration_in_min BETWEEN 16 AND 30 THEN '16-30 min'
        WHEN trip_duration_in_min BETWEEN 31 AND 60 THEN '31-60 min'
        ELSE '60+ min'
    END AS duration_bucket,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(SUM(fare_amount), 2) AS total_fare,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records
WHERE payment_type <> 3
GROUP BY duration_bucket
ORDER BY MIN(trip_duration_in_min);

-- 9. Tips by Trip Duration
SELECT 
    CASE
        WHEN trip_duration_in_min BETWEEN 0 AND 5 THEN '0-5 min'
        WHEN trip_duration_in_min BETWEEN 6 AND 15 THEN '6-15 min'
        WHEN trip_duration_in_min BETWEEN 16 AND 30 THEN '16-30 min'
        WHEN trip_duration_in_min BETWEEN 31 AND 60 THEN '31-60 min'
        ELSE '60+ min'
    END AS duration_bucket,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    ROUND(SUM(tip_amount), 2) AS total_tip,
    ROUND(SUM(IF(tip_amount > 0,1,0))/COUNT(*),2) AS pct_of_tip_payer,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records
WHERE payment_type <> 3
GROUP BY duration_bucket
ORDER BY MIN(trip_duration_in_min);

-- 10. MTA Tax by Trip Duration
SELECT 
    CASE
        WHEN trip_duration_in_min BETWEEN 0 AND 5 THEN '0-5 min'
        WHEN trip_duration_in_min BETWEEN 6 AND 15 THEN '6-15 min'
        WHEN trip_duration_in_min BETWEEN 16 AND 30 THEN '16-30 min'
        WHEN trip_duration_in_min BETWEEN 31 AND 60 THEN '31-60 min'
        ELSE '60+ min'
    END AS duration_bucket,
    SUM(mta_tax) AS total_mta_tax,
    ROUND(SUM(IF(mta_tax > 0.5 , 1,0 ))*100/COUNT(*),2) AS pct_mta_tax_payer,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records
WHERE payment_type <> 3
GROUP BY duration_bucket
ORDER BY MIN(trip_duration_in_min);

-- 11. Tolls by Trip Duration
SELECT 
    CASE
        WHEN trip_duration_in_min BETWEEN 0 AND 5 THEN '0-5 min'
        WHEN trip_duration_in_min BETWEEN 6 AND 15 THEN '6-15 min'
        WHEN trip_duration_in_min BETWEEN 16 AND 30 THEN '16-30 min'
        WHEN trip_duration_in_min BETWEEN 31 AND 60 THEN '31-60 min'
        ELSE '60+ min'
    END AS duration_bucket,
    ROUND(AVG(tolls_amount),2) AS avg_toll_amount,
    ROUND(SUM(tolls_amount),2) AS total_toll_amount,
    ROUND(SUM(IF(tolls_amount > 0,1,0))/COUNT(*),2) AS pct_of_toll_payer,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records
WHERE payment_type <> 3
GROUP BY duration_bucket
ORDER BY MIN(trip_duration_in_min);

-- 12. Congestion Surcharge by Trip Duration
SELECT 
    CASE
        WHEN trip_duration_in_min BETWEEN 0 AND 5 THEN '0-5 min'
        WHEN trip_duration_in_min BETWEEN 6 AND 15 THEN '6-15 min'
        WHEN trip_duration_in_min BETWEEN 16 AND 30 THEN '16-30 min'
        WHEN trip_duration_in_min BETWEEN 31 AND 60 THEN '31-60 min'
        ELSE '60+ min'
    END AS duration_bucket,
    ROUND(AVG(congestion_surcharge),2) AS avg_congestion_surcharge,
    ROUND(SUM(congestion_surcharge),2) AS total_congestion_surcharge,
    ROUND(SUM(IF(congestion_surcharge > 0,1,0))/COUNT(*),2) AS pct_of_congestion_surcharge_payer,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records
WHERE payment_type <> 3
GROUP BY duration_bucket
ORDER BY MIN(trip_duration_in_min);

/***************** 1. Zone vs Tip Amount *****************/
-- Insight: Which pickup zones generate higher tips?
SELECT 
    tz.zone AS pickup_zone,
    ROUND(AVG(tip_amount),2) AS avg_tip_amount,
    ROUND(SUM(tip_amount),2) AS total_tip_amount,
    ROUND(SUM(IF(tip_amount > 0,1,0))/COUNT(*) * 100,2) AS pct_of_tip_payer,
    COUNT(*) AS trip_count
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON gt.PULocationID = tz.LocationID
GROUP BY tz.zone
ORDER BY trip_count DESC, avg_tip_amount DESC;


/***************** 2. Day-Time vs Tip Amount *****************/
-- Insight: Which time periods yield higher tipping?
SELECT 
    day_time,
    ROUND(AVG(tip_amount),2) AS avg_tip_amount,
    ROUND(SUM(tip_amount),2) AS total_tip_amount,
    ROUND(SUM(IF(tip_amount > 0,1,0))/COUNT(*) * 100,2) AS pct_of_tip_payer,
    COUNT(*) AS trip_count
FROM green_taxi_trip_records
GROUP BY day_time
ORDER BY avg_tip_amount DESC;


/***************** 3. Ride Type vs Tip Amount *****************/
-- Insight: Which ride type gets the highest tips?
SELECT 
    ride_type,
    ROUND(AVG(tip_amount),2) AS avg_tip_amount,
    ROUND(SUM(tip_amount),2) AS total_tip_amount,
    ROUND(SUM(IF(tip_amount > 0,1,0))/COUNT(*) * 100,2) AS pct_of_tip_payer,
    COUNT(*) AS trip_count
FROM green_taxi_trip_records
GROUP BY ride_type
ORDER BY avg_tip_amount DESC;


/***************** 4. Passenger Count vs Tip Amount *****************/
-- Insight: Do larger groups tip more?
SELECT 
    passenger_count AS num_passenger,
    ROUND(AVG(tip_amount),2) AS avg_tip_amount,
    ROUND(SUM(tip_amount),2) AS total_tip_amount,
    ROUND(SUM(IF(tip_amount > 0,1,0))/COUNT(*) * 100,2) AS pct_of_tip_payer,
    COUNT(*) AS trip_count
FROM green_taxi_trip_records
GROUP BY num_passenger
ORDER BY num_passenger;


/***************** 5. Payment Type vs Tip Amount *****************/
-- Insight: Which payment methods are most tip-friendly?
SELECT 
    payment_type,
    ROUND(AVG(tip_amount),2) AS avg_tip_amount,
    ROUND(SUM(tip_amount),2) AS total_tip_amount,
    ROUND(SUM(IF(tip_amount > 0,1,0))/COUNT(*) * 100,2) AS pct_of_tip_payer,
    COUNT(*) AS trip_count
FROM green_taxi_trip_records
WHERE payment_type <> 3
GROUP BY payment_type
ORDER BY avg_tip_amount DESC;


/***************** 6. Fare vs Tip Amount *****************/
-- Insight: How do tips vary across fare ranges?
SELECT 
    CASE 
        WHEN fare_amount <= 5 THEN '0–5$'
        WHEN fare_amount <= 10 THEN '6–10$'
        WHEN fare_amount <= 20 THEN '11–20$'
        WHEN fare_amount <= 50 THEN '21–50$'
        ELSE '50+$'
    END AS fare_range,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    ROUND(AVG(tip_amount / NULLIF(fare_amount,0)) * 100, 2) AS avg_tip_percent,
    COUNT(*) AS num_trips
FROM green_taxi_trip_records
WHERE fare_amount > 0
GROUP BY fare_range
ORDER BY MIN(fare_amount);


/***************** 7. Special Cases *****************/
-- 7a. Avg Tip for Outside NYC trips
SELECT ROUND(AVG(tip_amount),2) AS avg_tip_amount
FROM green_taxi_trip_records
WHERE mta_tax = 0;

-- 7b. Avg Tip for Round Trips (same PU/DO location)
SELECT ROUND(AVG(tip_amount),2) AS avg_tip_amount
FROM green_taxi_trip_records
WHERE PULocationID = DOLocationID;

-- 7c. Overall Avg Tip
SELECT ROUND(AVG(tip_amount),2) AS avg_tip_amount
FROM green_taxi_trip_records;


/***************** 8. Tolls vs Tip Amount *****************/
-- Insight: Are riders who pay tolls more generous?
SELECT 
    CASE 
        WHEN tolls_amount = 0 THEN 'No Toll'
        WHEN tolls_amount <= 5 THEN '0–5$'
        WHEN tolls_amount <= 10 THEN '6–10$'
        WHEN tolls_amount <= 20 THEN '11–20$'
        ELSE '20+$'
    END AS toll_range,
    ROUND(AVG(tip_amount),2) AS avg_tip,
    COUNT(*) AS trip_count
FROM green_taxi_trip_records
GROUP BY toll_range
ORDER BY MIN(tolls_amount);


/***************** 9. Short Distance – Long Duration *****************/
-- Insight: Do “slow short trips” earn more tips?
SELECT
    trip_type,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)) * 100, 2) AS avg_tip_percent,
    COUNT(*) AS trip_count
FROM (
    SELECT
        CASE
            WHEN trip_distance <= 1 AND trip_duration_in_min > 6 THEN 'Short Distance - Long Duration'
            WHEN trip_distance <= 1 THEN 'Short Distance - Normal Duration'
        END AS trip_type,
        tip_amount,
        fare_amount
    FROM green_taxi_trip_records
) categorized_trips
GROUP BY trip_type
ORDER BY trip_type;


/***************** 10. Route-wise Tip Analysis *****************/
-- Insight: Which pickup–dropoff routes earn higher tips?
SELECT 
    tz1.zone AS pickup_zone,
    tz2.zone AS dropoff_zone,
    ROUND(AVG(tip_amount),2) AS avg_tip,
    ROUND(SUM(IF(tip_amount > 0,1,0))/COUNT(*) * 100,2) AS pct_of_tip_payer,
    COUNT(*) AS trip_counts
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz1
    ON tz1.LocationID = gt.PULocationID
JOIN taxi_zone_lookup tz2
    ON tz2.LocationID = gt.DOLocationID
GROUP BY tz1.zone, tz2.zone
ORDER BY trip_counts DESC;

/*********************************************************************
                      RIDE TYPE ANALYSIS
*********************************************************************/

/********** 1. Pickup Day vs Ride Type **********/
SELECT 
    pickup_day, 
    ride_type,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records
GROUP BY pickup_day, ride_type
ORDER BY FIELD(pickup_day , 'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');


/********** 2. Pickup Location vs Avg Trip Duration **********/
SELECT 
    gt.pulocationid,
    tz.zone AS pickup_zone,
    ROUND(AVG(trip_duration_in_min), 2) AS avg_trip_duration,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON tz.locationid = gt.pulocationid
GROUP BY gt.pulocationid, tz.zone
ORDER BY avg_trip_duration DESC;


/********** 3. Ride Type vs Pickup Zone **********/
SELECT 
    ride_type,
    tz.zone AS pickup_zone,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON gt.pulocationid = tz.locationid
GROUP BY ride_type, tz.zone
ORDER BY ride_type, total_trips DESC;


/********** 4. Ride Type vs Dropoff Zone **********/
SELECT 
    ride_type,
    tz.zone AS dropoff_zone,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON gt.dolocationid = tz.locationid
GROUP BY ride_type, tz.zone
ORDER BY ride_type, total_trips DESC;

select * from green_taxi_trip_records
WHERE PULocationID IN (1,264,265) OR DOLocationID IN (1,264,265);


/***********************************************
				Payment Type
***********************************************/
-- 1. IS cash Commonly Used for Credit Has Taken Over
SELECT 
    CASE 
        WHEN payment_type = 1 THEN 'Credit Card'
        WHEN payment_type = 2 THEN 'Cash'
        ELSE 'Other'
    END AS payment_method,
    COUNT(*) AS total_trips,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM green_taxi_trip_records), 2) AS percentage_share
FROM green_taxi_trip_records
GROUP BY payment_method
ORDER BY total_trips DESC;


-- 2. Cashless Ride % by Pickup Location
SELECT 
    tz.zone AS pickup_zone,
    COUNT(*) AS total_trips,
    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS credit_trips,
    SUM(CASE WHEN payment_type NOT IN (2) THEN 1 ELSE 0 END) AS cashless_trips,
    ROUND(SUM(CASE WHEN payment_type NOT IN (2) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cashless_percentage
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz 
    ON gt.PULocationID = tz.LocationID
GROUP BY tz.zone
ORDER BY cashless_percentage DESC;
