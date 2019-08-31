class SqlQueries:
    event_table_insert = ("""
        SELECT distinct
                region,
                state,
                city,
                resale,
                product,
                TO_DATE(registration_date,'DD/MM/YYYY') as registration_date,
                sale_price,
                purchase_price,
                seller
        FROM staging_capstone_events   
               
             
    """)
    
    states_table_insert = ("""
        SELECT distinct
               state ,
               state_name,
               CAST(population as NUMERIC),
               CAST(per_capita_income as NUMERIC)
        FROM staging_capstone_states;     
    """)
    
    region_table_insert = ("""
        SELECT distinct
               region,
               region_name,
               TO_NUMBER(area_km, '99999999999' ) as area_km 
        FROM staging_capstone_region       
    """)
    
    time_table_insert = ("""
        SELECT distinct
              registration_date,
              EXTRACT(day from registration_date) as day,
              EXTRACT(month from registration_date) as month,
              EXTRACT(year from registration_date) as year,
              EXTRACT(quarter from registration_date) as quarter,
              EXTRACT(week from registration_date) as week
       FROM capstone_events;
    """)
    