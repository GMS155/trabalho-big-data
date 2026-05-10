from pyspark.sql.types import (
    StructType, StructField,
    DoubleType, StringType,
)
from pyspark.sql import DataFrame

# Schema that matches vehicle_data.csv headers exactly.
# All numeric columns are DoubleType to handle nulls and decimals.
RAW_SCHEMA = StructType([
    StructField("DayNum",                                 DoubleType(), True),
    StructField("VehId",                                  DoubleType(), True),
    StructField("Trip",                                   DoubleType(), True),
    StructField("Timestamp(ms)",                          DoubleType(), True),
    StructField("Latitude[deg]",                          DoubleType(), True),
    StructField("Longitude[deg]",                         DoubleType(), True),
    StructField("Vehicle Speed[km/h]",                    DoubleType(), True),
    StructField("MAF[g/sec]",                             DoubleType(), True),
    StructField("Engine RPM[RPM]",                        DoubleType(), True),
    StructField("Absolute Load[%]",                       DoubleType(), True),
    StructField("OAT[DegC]",                              DoubleType(), True),
    StructField("Fuel Rate[L/hr]",                        DoubleType(), True),
    StructField("Air Conditioning Power[kW]",             DoubleType(), True),
    StructField("Air Conditioning Power[Watts]",          DoubleType(), True),
    StructField("Heater Power[Watts]",                    DoubleType(), True),
    StructField("HV Battery Current[A]",                  DoubleType(), True),
    StructField("HV Battery SOC[%]",                      DoubleType(), True),
    StructField("HV Battery Voltage[V]",                  DoubleType(), True),
    StructField("Short Term Fuel Trim Bank 1[%]",         DoubleType(), True),
    StructField("Short Term Fuel Trim Bank 2[%]",         DoubleType(), True),
    StructField("Long Term Fuel Trim Bank 1[%]",          DoubleType(), True),
    StructField("Long Term Fuel Trim Bank 2[%]",          DoubleType(), True),
    StructField("Elevation Raw[m]",                       DoubleType(), True),
    StructField("Elevation Smoothed[m]",                  DoubleType(), True),
    StructField("Gradient",                               DoubleType(), True),
    StructField("Energy_Consumption",                     DoubleType(), True),
    StructField("Matchted Latitude[deg]",                 DoubleType(), True),
    StructField("Matched Longitude[deg]",                 DoubleType(), True),
    StructField("Match Type",                             DoubleType(), True),
    StructField("Class of Speed Limit",                   DoubleType(), True),
    StructField("Speed Limit[km/h]",                      DoubleType(), True),
    StructField("Speed Limit with Direction[km/h]",       DoubleType(), True),
    StructField("Intersection",                           StringType(), True),
    StructField("Bus Stops",                              StringType(), True),
    StructField("Focus Points",                           StringType(), True),
])

# Maps raw CSV header names → clean Python-friendly column names.
COLUMN_RENAMES: dict[str, str] = {
    "Timestamp(ms)":                        "timestamp_ms",
    "Latitude[deg]":                        "latitude",
    "Longitude[deg]":                       "longitude",
    "Vehicle Speed[km/h]":                  "speed_kmh",
    "MAF[g/sec]":                           "maf_g_sec",
    "Engine RPM[RPM]":                      "engine_rpm",
    "Absolute Load[%]":                     "absolute_load_pct",
    "OAT[DegC]":                            "outside_air_temp_c",
    "Fuel Rate[L/hr]":                      "fuel_rate_l_hr",
    "Air Conditioning Power[kW]":           "ac_power_kw",
    "Air Conditioning Power[Watts]":        "ac_power_w",
    "Heater Power[Watts]":                  "heater_power_w",
    "HV Battery Current[A]":               "hv_battery_current_a",
    "HV Battery SOC[%]":                    "hv_battery_soc_pct",
    "HV Battery Voltage[V]":               "hv_battery_voltage_v",
    "Short Term Fuel Trim Bank 1[%]":       "short_term_fuel_trim_b1_pct",
    "Short Term Fuel Trim Bank 2[%]":       "short_term_fuel_trim_b2_pct",
    "Long Term Fuel Trim Bank 1[%]":        "long_term_fuel_trim_b1_pct",
    "Long Term Fuel Trim Bank 2[%]":        "long_term_fuel_trim_b2_pct",
    "Elevation Raw[m]":                     "elevation_raw_m",
    "Elevation Smoothed[m]":               "elevation_smoothed_m",
    "Gradient":                             "gradient",
    "Energy_Consumption":                   "energy_consumption",
    "Matchted Latitude[deg]":               "matched_latitude",
    "Matched Longitude[deg]":              "matched_longitude",
    "Match Type":                           "match_type",
    "Class of Speed Limit":                "speed_limit_class",
    "Speed Limit[km/h]":                    "speed_limit_kmh",
    "Speed Limit with Direction[km/h]":     "speed_limit_dir_kmh",
    "Intersection":                         "intersection",
    "Bus Stops":                            "bus_stops",
    "Focus Points":                         "focus_points",
}


def rename_columns(df: DataFrame) -> DataFrame:
    for old_name, new_name in COLUMN_RENAMES.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df
