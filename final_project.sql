USE Project;
SELECT * FROM green_taxi_trip_records;



-- 11. Popular ride_type distribution by pickup day.
-- 12. Weekend vs weekday analysis: toll amounts comparison.

-- 13. Revenue Contribution by Zone
--     -- Identify zones generating the most revenue.

-- 14. Payment Method Trends by Zone
    -- Compare cash, card, and wallet usage across zones.


-- 15. Check if mta_tax varies with pickup day.
-- 16. Relationship between passenger_count and pickup day.
-- 17. Identify busiest hour on each pickup day.
-- 18. Popular pickup–dropoff locations per pickup day.
-- 19. Number of trips per day and day_time.
-- 20. Relation between "extra" charges and pickup day.
-- 21. Day vs total_amount analysis (fare+charges).
-- 22. Investigate why congestion_surcharge is not included in total_amount.

/***********************************************
             DROP DAY BASED ANALYSIS
   (Only for trips where pickup_day <> drop_day)
************************************************/
-- 1. Average trip duration for cross-day trips.
-- 2. Common ride types used for cross-day trips.
-- 3. Popular pickup–dropoff locations for cross-day trips.
-- 4. Passenger count patterns for cross-day trips.
-- 5. Fare distribution for cross-day trips.
-- 6. Typical trip duration and distance for cross-day trips.
-- 7. Day_time distribution for cross-day trips.

/***********************************************
         PASSENGER COUNT BASED ANALYSIS
************************************************/
-- 1. Relationship between passenger_count and fare_amount.
-- 2. Passenger_count vs day vs ride_type (which ride types are preferred by different passenger counts).

/***********************************************
           LONG-DISTANCE TRIP ANALYSIS
************************************************/
-- 1. Identify unusually long-distance trips.
-- 2. Detect trips with long distance but unusually low fare.
-- 3. Compare tip amounts for long-distance vs short-distance/long-duration trips.
-- 4. Check consistency between trip_distance and fare_amount.
-- 5. Analyze if long-distance trips attract more "extra" charges.
-- 6. Pickup and dropoff locations mostly generating short-distance trips.
-- 7. Pickup locations mostly generating long-distance trips.
-- 8. Days with higher frequency of long-distance trips.
-- 9. Pickup locations never producing long-distance trips.
-- 10. Average and total travel distance by pickup day.


/***********************************************
                TRIP DURATION ANALYSIS
************************************************/
-- 1a. Average Trip Duration by Zone
-- 1. Average trip duration by PULocation.
-- 2. Average trip duration by DOLocation.
-- 3. Average and total trip duration by pickup day.
-- 4. Analyze ride_type vs trip duration.
-- 5. Analyze trip duration vs passenger_count (do passengers prefer short trips?).
-- 6. Consistency check: trip_duration vs trip_distance.
-- 7. Relationship between trip_duration and fare_amount.
-- 8. Average duration for trips going outside NYC.

/***********************************************
                TRIP DISTANCE ANALYSIS
************************************************/
-- 1. Identify hotspot pickup locations for long-distance trips.
-- 2. Identify hotspot pickup locations for short-distance trips.
-- 3. Trip distance distribution by day_time (long vs short routes).
-- 4. Check consistency between trip_distance and fare_amount.
-- 5. Trip distance analysis for out-of-NYC trips.
-- 6. Distance analysis for round trips (pickup = dropoff).
-- 7. Relationship between trip_distance and toll_amount.
-- 8. Relationship between trip_distance and tip_amount.

/***********************************************
                FARE AMOUNT ANALYSIS
************************************************/
-- 1. Relationship between trip_duration and fare_amount.
-- 2. Investigate if overspeeding affects fare_amount.
-- 3. Consistency check between fare_amount and total_amount.
-- 4. Identify routes with unusually high fare_amount.
-- 5. Average fare amount by pickup day.
-- Analysis: Average and Total Fare by Trip Duration
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


-- Analysis: Average, Total Tip and % of Tip Payers by Trip Duration
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


-- Analysis: Total MTA Tax and % Payers by Trip Duration
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


-- Analysis: Average, Total Tolls and % of Toll Payers by Trip Duration
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


-- Analysis: Average, Total Congestion Surcharge and % of Payers by Trip Duration
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

-- 
/***************************************
		TRIP DURATION
*****************************************/

-- Kis PULocation de avergae kitni lambi  ride mitli hai 
-- Usually kitni duration travel karni padti hai kisi  drop point
-- kiss day on an average kitni duration travel ki jati hai , total kitni  ki jati hai 
-- ride_type and duration ko analyze 
-- duration and passenger  ko ek sath sekho kya insight nikal sakata , no of passenger kya perfer karte hai short trip 
-- kya trip_duration and distance aapas mein consistent 
-- trip duration and fare amount ka apas mein kya relation rehta
-- jo trips nyc se bahar gyee hain unka average duration kya hai 

/****************************************************************
             TRIP DISTANCE 
****************************************************************/
-- long distance trips ka koi hotspot point hai kya 
-- short distance ka hotspot pickup point hai kya koi
-- kis day  time pe , loong ya short route nikal te hai
-- kya trip distance and fare amount meinn koi relation and kya woh dono consistent hai ya nahi aapas meinn
-- out of nyc trips ka trip distance ka analysis karo
-- round trips ka distance analysis karo 
-- kya distance and toll amount ka koi relation hai 
-- kya distance and tip_amount ka koi relatio hai

/*********************************************************************
            RIDE TYPE 
**********************************************************************/

-- Relationship between Pickup Day and Ride Type
SELECT 
    pickup_day, 
    ride_type,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records
GROUP BY pickup_day, ride_type
ORDER BY 
    CASE pickup_day
        WHEN 'MONDAY' THEN 1
        WHEN 'TUESDAY' THEN 2
        WHEN 'WEDNESDAY' THEN 3
        WHEN 'THURSDAY' THEN 4
        WHEN 'FRIDAY' THEN 5
        WHEN 'SATURDAY' THEN 6
        ELSE 7
    END,
    total_trips;

-- Relationship between Pickup Location and Average Trip Duration
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

-- Relationship between Ride Type and Pickup Zone
SELECT 
    ride_type,
    tz.zone AS pickup_zone,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON gt.pulocationid = tz.locationid
GROUP BY ride_type, tz.zone
ORDER BY ride_type, total_trips DESC;

-- Relationship between Ride Type and Dropoff Zone
SELECT 
    ride_type,
    tz.zone AS dropoff_zone,
    COUNT(*) AS total_trips
FROM green_taxi_trip_records gt
JOIN taxi_zone_lookup tz
    ON gt.dolocationid = tz.locationid
GROUP BY ride_type, tz.zone
ORDER BY ride_type, total_trips DESC;

                
                
                
                
					   
