

-- PURPOSE : Date dimension


CREATE TABLE IF NOT EXISTS bl_dm.dim_dates (
    date_key            INTEGER         PRIMARY KEY,    
    full_date           DATE            NOT NULL,
    day_of_month        INTEGER         NOT NULL,       
    month_num           INTEGER         NOT NULL,      
    month_name          VARCHAR(15)     NOT NULL,      
    quarter             INTEGER         NOT NULL,       
    year                INTEGER         NOT NULL,      
    day_of_week_num     INTEGER         NOT NULL,    
    day_of_week_name    VARCHAR(15)     NOT NULL,   
    is_weekend          BOOLEAN         NOT NULL,   
    fiscal_year         INTEGER         NOT NULL,
    fiscal_quarter      INTEGER         NOT NULL    

);
