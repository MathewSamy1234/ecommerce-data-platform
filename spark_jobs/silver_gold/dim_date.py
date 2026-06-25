


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("dimdate")
    .getOrCreate()
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# PARAMETERS
# =============================================================================

BEGIN_DATE = "2000-01-01"
END_DATE = "2050-12-31"

# =============================================================================
# GENERATE DATE RANGE
# =============================================================================

date_dim = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{BEGIN_DATE}'),
                to_date('{END_DATE}'),
                interval 1 day
            )
        ) AS calendarDate
    """)
)

# =============================================================================
# BASIC DATE ATTRIBUTES
# =============================================================================

date_dim = (
    date_dim
    .withColumn(
        "DateSK",
        F.date_format("calendarDate", "yyyyMMdd").cast("int")
    )
    .withColumn("Day", F.lpad(F.dayofmonth("calendarDate"), 2, "0"))
    .withColumn("DayNumber", F.dayofmonth("calendarDate"))
    .withColumn("DayOfWeekNumber", F.dayofweek("calendarDate"))
    .withColumn("DayOfWeek", F.date_format("calendarDate", "EEEE"))
    .withColumn("DayOfYear", F.dayofyear("calendarDate"))
    .withColumn("WeekOfYear", F.weekofyear("calendarDate"))
    .withColumn("Month", F.lpad(F.month("calendarDate"), 2, "0"))
    .withColumn("MonthNumber", F.month("calendarDate"))
    .withColumn("MonthName", F.date_format("calendarDate", "MMMM"))
    .withColumn("Quarter", F.quarter("calendarDate"))
    .withColumn(
        "QuarterName",
        F.when(F.col("Quarter") == 1, "First")
         .when(F.col("Quarter") == 2, "Second")
         .when(F.col("Quarter") == 3, "Third")
         .otherwise("Fourth")
    )
    .withColumn("Year", F.year("calendarDate"))
    .withColumn(
        "StandardDate",
        F.date_format("calendarDate", "MM/dd/yyyy")
    )
)

# =============================================================================
# DAY SUFFIX (1st, 2nd, 3rd, etc.)
# =============================================================================

date_dim = (
    date_dim
    .withColumn(
        "DaySuffix",
        F.when(F.col("DayNumber").isin(11, 12, 13),
               F.concat(F.col("DayNumber").cast("string"), F.lit("th")))
        .when((F.col("DayNumber") % 10) == 1,
              F.concat(F.col("DayNumber").cast("string"), F.lit("st")))
        .when((F.col("DayNumber") % 10) == 2,
              F.concat(F.col("DayNumber").cast("string"), F.lit("nd")))
        .when((F.col("DayNumber") % 10) == 3,
              F.concat(F.col("DayNumber").cast("string"), F.lit("rd")))
        .otherwise(
              F.concat(F.col("DayNumber").cast("string"), F.lit("th"))
        )
    )
)

# =============================================================================
# DOW IN MONTH (1st Monday, 2nd Monday, etc.)
# =============================================================================

dow_window = (
    Window.partitionBy(
        F.year("calendarDate"),
        F.month("calendarDate"),
        F.dayofweek("calendarDate")
    )
    .orderBy("calendarDate")
)

date_dim = date_dim.withColumn(
    "DOWInMonth",
    F.row_number().over(dow_window)
)

# =============================================================================
# WEEK OF MONTH
# =============================================================================

month_start = F.trunc("calendarDate", "month")

date_dim = (
    date_dim
    .withColumn(
        "WeekOfMonth",
        (
            F.weekofyear("calendarDate")
            - F.weekofyear(month_start)
            + 1
        )
    )
)

# Handle year boundaries correctly
date_dim = date_dim.withColumn(
    "WeekOfMonth",
    F.when(F.col("WeekOfMonth") <= 0, 1)
     .otherwise(F.col("WeekOfMonth"))
)

# =============================================================================
# WEEKEND FLAG
# =============================================================================

date_dim = date_dim.withColumn(
    "IsWeekend",
    F.col("DayOfWeekNumber").isin(1, 7)
)

# =============================================================================
# HOLIDAYS
# =============================================================================

date_dim = date_dim.withColumn("HolidayText", F.lit(None).cast("string"))

# New Year's Day
date_dim = date_dim.withColumn(
    "HolidayText",
    F.when(
        (F.col("MonthNumber") == 1) &
        (F.col("DayNumber") == 1),
        "New Year's Day"
    ).otherwise(F.col("HolidayText"))
)

# Valentine's Day
date_dim = date_dim.withColumn(
    "HolidayText",
    F.when(
        (F.col("MonthNumber") == 2) &
        (F.col("DayNumber") == 14),
        "Valentine's Day"
    ).otherwise(F.col("HolidayText"))
)

# St Patrick's Day
date_dim = date_dim.withColumn(
    "HolidayText",
    F.when(
        (F.col("MonthNumber") == 3) &
        (F.col("DayNumber") == 17),
        "Saint Patrick's Day"
    ).otherwise(F.col("HolidayText"))
)

# Independence Day
date_dim = date_dim.withColumn(
    "HolidayText",
    F.when(
        (F.col("MonthNumber") == 7) &
        (F.col("DayNumber") == 4),
        "Independence Day"
    ).otherwise(F.col("HolidayText"))
)

# Halloween
date_dim = date_dim.withColumn(
    "HolidayText",
    F.when(
        (F.col("MonthNumber") == 10) &
        (F.col("DayNumber") == 31),
        "Halloween"
    ).otherwise(F.col("HolidayText"))
)

# Christmas
date_dim = date_dim.withColumn(
    "HolidayText",
    F.when(
        (F.col("MonthNumber") == 12) &
        (F.col("DayNumber") == 25),
        "Christmas Day"
    ).otherwise(F.col("HolidayText"))
)

# MLK Day (3rd Monday January)
date_dim = date_dim.withColumn(
    "HolidayText",
    F.when(
        (F.col("MonthNumber") == 1) &
        (F.col("DayOfWeek") == "Monday") &
        (F.col("DOWInMonth") == 3),
        "Martin Luther King Jr Day"
    ).otherwise(F.col("HolidayText"))
)

# Presidents Day (3rd Monday February)
date_dim = date_dim.withColumn(
    "HolidayText",
    F.when(
        (F.col("MonthNumber") == 2) &
        (F.col("DayOfWeek") == "Monday") &
        (F.col("DOWInMonth") == 3),
        "Presidents Day"
    ).otherwise(F.col("HolidayText"))
)

# Mother's Day (2nd Sunday May)
date_dim = date_dim.withColumn(
    "HolidayText",
    F.when(
        (F.col("MonthNumber") == 5) &
        (F.col("DayOfWeek") == "Sunday") &
        (F.col("DOWInMonth") == 2),
        "Mother's Day"
    ).otherwise(F.col("HolidayText"))
)

# Father's Day (3rd Sunday June)
date_dim = date_dim.withColumn(
    "HolidayText",
    F.when(
        (F.col("MonthNumber") == 6) &
        (F.col("DayOfWeek") == "Sunday") &
        (F.col("DOWInMonth") == 3),
        "Father's Day"
    ).otherwise(F.col("HolidayText"))
)

# Labor Day (1st Monday September)
date_dim = date_dim.withColumn(
    "HolidayText",
    F.when(
        (F.col("MonthNumber") == 9) &
        (F.col("DayOfWeek") == "Monday") &
        (F.col("DOWInMonth") == 1),
        "Labor Day"
    ).otherwise(F.col("HolidayText"))
)

# Thanksgiving (4th Thursday November)
date_dim = date_dim.withColumn(
    "HolidayText",
    F.when(
        (F.col("MonthNumber") == 11) &
        (F.col("DayOfWeek") == "Thursday") &
        (F.col("DOWInMonth") == 4),
        "Thanksgiving Day"
    ).otherwise(F.col("HolidayText"))
)

# Memorial Day (last Monday May)
memorial_day = (
    date_dim
    .filter(
        (F.col("MonthNumber") == 5) &
        (F.col("DayOfWeek") == "Monday")
    )
    .groupBy("Year")
    .agg(F.max("calendarDate").alias("MemorialDate"))
    .select("MemorialDate")
)
date_dim = (
    date_dim.alias("d")
    .join(
        memorial_day.alias("m"),
        F.col("d.calendarDate") == F.col("m.MemorialDate"),
        "left"
    )
    .withColumn(
        "HolidayText",
        F.when(
            F.col("m.MemorialDate").isNotNull(),
            "Memorial Day"
        ).otherwise(F.col("HolidayText"))
    )
    .drop("MemorialDate")
)

# =============================================================================
# FINAL COLUMN ORDER
# =============================================================================

date_dim = date_dim.select(
    "DateSK",
    F.col("calendarDate").alias("Date"),
    "Day",
    "DaySuffix",
    "DayOfWeek",
    "DayOfWeekNumber",
    "DOWInMonth",
    "DayOfYear",
    "WeekOfYear",
    "WeekOfMonth",
    "Month",
    "MonthNumber",
    "MonthName",
    "Quarter",
    "QuarterName",
    "Year",
    "StandardDate",
    "IsWeekend",
    "HolidayText"
)

# =============================================================================
# SAVE
# =============================================================================

date_dim.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/ecommerce/gold/dim_date")