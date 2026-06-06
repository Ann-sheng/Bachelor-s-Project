-- Helps filter by exact date value
CREATE INDEX IF NOT EXISTS idx_dim_dates_full ON bl_dm.dim_dates (full_date);

-- Helps filter and aggregate by year and month
CREATE INDEX IF NOT EXISTS idx_dim_dates_year_month  ON bl_dm.dim_dates (year, month_num);




