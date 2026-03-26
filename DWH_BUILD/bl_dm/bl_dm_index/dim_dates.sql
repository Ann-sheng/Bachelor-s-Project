
CREATE INDEX IF NOT EXISTS idx_dim_dates_full ON bl_dm.dim_dates (full_date);

CREATE INDEX IF NOT EXISTS idx_dim_dates_year_month  ON bl_dm.dim_dates (year, month_num);




