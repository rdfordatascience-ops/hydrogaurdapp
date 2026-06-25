-- 1. Create the source table linking directly to the sensor stream
CREATE TABLE water_telemetry (
    sensor_id STRING,
    station_name STRING,
    location_coordinates STRING,
    ecoli_enzyme_fluorescence_rfu FLOAT,
    estimated_mpn_per_100ml FLOAT,
    turbidity_ntu FLOAT,
    `timestamp` TIMESTAMP(3),
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'telemetry.water.sensors',
    'properties.bootstrap.servers' = 'KAFKA_BOOTSTRAP_SERVERS',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'PLAIN',
    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="KAFKA_API_KEY" password="KAFKA_API_SECRET";',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'SQL'
);

-- 2. Define the detection rules over a 5-minute window
SELECT 
    TUMBLE_START(`timestamp`, INTERVAL '5' MINUTE) AS window_start,
    sensor_id,
    station_name,
    AVG(estimated_mpn_per_100ml) AS avg_ecoli_mpn,
    AVG(turbidity_ntu) AS avg_turbidity_ntu,
    'CRITICAL_CONTAMINATION' AS alert_level
FROM water_telemetry
GROUP BY TUMBLE(`timestamp`, INTERVAL '5' MINUTE), sensor_id, station_name
HAVING AVG(estimated_mpn_per_100ml) > 1.0 AND AVG(turbidity_ntu) > 5.0;
