git add .
CREATE     PROCEDURE dbo.sp_load_gold_layer
    @start_date DATE
AS
BEGIN

    WITH cte_silver_date_enriched AS
    (
        SELECT
            e.*,
            c.country_key,
            m.mag_category_key,
            s.sig_category_key,
            d.depth_category_key
        FROM lh_earthquake.dbo.silver_earthquake e
        INNER JOIN wh_earthquake.dbo.dim_country c
            ON e.country_code = c.country_code
        INNER JOIN wh_earthquake.dbo.dim_mag_category m
            ON e.mag >= m.min_value
           AND (e.mag < m.max_value OR m.max_value IS NULL)
        INNER JOIN wh_earthquake.dbo.dim_sig_category s
            ON e.sig >= s.min_value
           AND (e.sig < s.max_value OR s.max_value IS NULL)
        INNER JOIN wh_earthquake.dbo.dim_depth_category d
            ON e.depth >= d.min_value
           AND (e.depth < d.max_value OR d.max_value IS NULL)
        WHERE e.event_date > @start_date
    )

    MERGE wh_earthquake.dbo.fact_earthquake AS tgt
    USING cte_silver_date_enriched AS src
        ON tgt.event_id = src.event_id

    WHEN MATCHED THEN
        UPDATE SET
            tgt.event_date = src.event_date,
            tgt.event_time = src.event_time,
            tgt.country_key = src.country_key,
            tgt.lat = src.latitude,
            tgt.lon = src.longitude,
            tgt.depth = src.depth,
            tgt.depth_category_key = src.depth_category_key,
            tgt.sig = src.sig,
            tgt.sig_category_key = src.sig_category_key,
            tgt.mag = src.mag,
            tgt.mag_category_key = src.mag_category_key,
            tgt.hemisphere = src.hemisphere,
            tgt.is_tsunami = src.is_tsunami,
            tgt.location = src.location , 
            tgt.updated_at = CURRENT_TIMESTAMP

    WHEN NOT MATCHED THEN
        INSERT (
            event_id,
            event_date,
            event_time,
            country_key,
            sig,
            mag,
            mag_category_key,
            sig_category_key,
            depth_category_key,
            is_tsunami,
            lat,
            lon,
            depth,
            hemisphere,
            location,
            ingested_at,
            updated_at
        )
        VALUES (
            src.event_id,
            src.event_date,
            src.event_time,
            src.country_key,
            src.sig,
            src.mag,
            src.mag_category_key,
            src.sig_category_key,
            src.depth_category_key,
            src.is_tsunami,
            src.latitude,
            src.longitude,
            src.depth,
            src.hemisphere,
            location, 
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        );

END;