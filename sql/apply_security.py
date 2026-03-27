# Apply CLS
spark.sql("""
CREATE OR REPLACE FUNCTION mazhara_test.mazhara_silver.email_mask(email STRING)
RETURNS STRING
RETURN CASE
  WHEN current_user() IN (
    'mazhara.kirill@softserve.academy',
    'squ14sssm@softserve.academy'
  ) THEN email
  WHEN is_account_group_member('admins') THEN email
  ELSE regexp_replace(email, '(?<=.).(?=.*@)', '*')
END
""")

# Apply RLS
spark.sql("""
CREATE OR REPLACE FUNCTION mazhara_test.mazhara_silver.row_filter()
RETURNS BOOLEAN
RETURN
  current_user() IN (
    'mazhara.kirill@softserve.academy',
    'squ14sssm@softserve.academy'
  )
  OR is_account_group_member('admins')
""")

# Apply CLS to column
spark.sql("""
ALTER MATERIALIZED VIEW mazhara_test.mazhara_gold.fact_sales_gold
ALTER COLUMN user_email
SET MASK mazhara_test.mazhara_silver.email_mask
""")

# Apply RLS to table
spark.sql("""
ALTER MATERIALIZED VIEW mazhara_test.mazhara_gold.fact_sales_gold
SET ROW FILTER mazhara_test.mazhara_silver.row_filter ON ()
""")