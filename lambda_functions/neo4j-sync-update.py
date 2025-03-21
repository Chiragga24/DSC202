
import json
import psycopg2
import datetime
import json
from neo4j import GraphDatabase

# RDS credentials
RDS_HOST = "database-1.cb0gsmcyc0ns.us-west-1.rds.amazonaws.com"
RDS_DB = "fitbit_data"
RDS_USER = "postgres"
RDS_PASSWORD = "Pass1234!"

# # Neo4j credentials
NEO4J_URI = "neo4j+s://9d24ccb0.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "ll0grkN_ME_vUSDlN2JMRd12xZ8m8Qmj25M0e5LZFuA"


def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=RDS_HOST,
        database=RDS_DB,
        user=RDS_USER,
        password=RDS_PASSWORD,
        port=5432
    )


def get_neo4j_session():
    """Establishes a connection to the Neo4j database."""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return driver.session()


def fetch_daily_metrics(user_id):
    """Fetches daily metrics and computes intensity & resting heart rate."""
    query = """
    WITH latest_metrics AS (
        SELECT
            d.user_id,
            d.activity_date,
            d.total_steps,
            d.total_calories,
            d.total_sleep_minutes,
            (
                SELECT ROUND(AVG(h.intensity_level), 2)
                FROM hourly_data h
                WHERE d.user_id = h.user_id
                  AND DATE(h.activity_hour) = d.activity_date
            ) AS avg_intensity,
            (
                SELECT mode() WITHIN GROUP (ORDER BY m.heart_rate)
                FROM minute_data m
                WHERE d.user_id = m.user_id
                  AND DATE(m.activity_minute) = d.activity_date
            ) AS resting_hr
        FROM daily_data d
        WHERE d.user_id = %s
        ORDER BY d.activity_date DESC
        LIMIT 1
    )
    SELECT * FROM latest_metrics;
    """

    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()

    if not result:
        return None

    return {
        "user_id": result[0],
        "activity_date": str(result[1]),
        "total_steps": result[2],
        "total_calories": result[3],
        "total_sleep_minutes": result[4],
        "avg_intensity": result[5],
        "resting_hr": result[6]
    }

# def fetch_anomalies(user_id):
#     """Fetches anomalies for the user's latest data entry."""
#     query = """
#     WITH latest_metrics AS (
#         SELECT d.user_id, d.activity_date, d.total_steps, d.total_calories, d.total_sleep_minutes,
#             (SELECT ROUND(AVG(h.intensity_level), 2)
#              FROM hourly_data h WHERE d.user_id = h.user_id
#              AND DATE(h.activity_hour) = d.activity_date) AS avg_intensity,
#             (SELECT mode() WITHIN GROUP (ORDER BY m.heart_rate)
#              FROM minute_data m WHERE d.user_id = m.user_id
#              AND DATE(m.activity_minute) = d.activity_date) AS resting_hr
#         FROM daily_data d
#         WHERE d.user_id = %s
#         ORDER BY d.activity_date DESC
#         LIMIT 1
#     )
#     SELECT
#         lm.user_id, lm.activity_date,
#         sleep_exp.min_anamoly_desc AS sleep_anomaly,
#         hr_exp.min_anamoly_desc AS hr_anomaly,
#         intensity_exp.min_anamoly_desc AS intensity_anomaly,
#         calories_exp.min_anamoly_desc AS calories_anomaly
#     FROM latest_metrics lm
#     LEFT JOIN metric_explaination sleep_exp
#         ON sleep_exp.metric_name = 'Sleep (Hours)'
#         AND (lm.total_sleep_minutes < (sleep_exp.min_value * 60)
#         OR lm.total_sleep_minutes > (sleep_exp.max_value * 60))
#     LEFT JOIN metric_explaination hr_exp
#         ON hr_exp.metric_name = 'Heart Rate (Resting bpm)'
#         AND (lm.resting_hr < hr_exp.min_value OR lm.resting_hr > hr_exp.max_value)
#     LEFT JOIN metric_explaination intensity_exp
#         ON intensity_exp.metric_name = 'Intensity (HRV in ms)'
#         AND (lm.avg_intensity < intensity_exp.min_value OR lm.avg_intensity > intensity_exp.max_value)
#     LEFT JOIN metric_explaination calories_exp
#         ON calories_exp.metric_name = 'Calories (kcal/day)'
#         AND (lm.total_calories < calories_exp.min_value OR lm.total_calories > calories_exp.max_value);
#     """

#     with get_db_connection() as conn, conn.cursor() as cursor:
#         cursor.execute(query, (user_id,))
#         result = cursor.fetchone()

#     if not result:
#         return None

#     return {
#         "user_id": result[0],
#         "activity_date": str(result[1]),
#         "sleep_anomaly": result[2],
#         "hr_anomaly": result[3],
#         "intensity_anomaly": result[4],
#         "calories_anomaly": result[5]
#     }


def fetch_met_values(user_id):
    """Fetches MET averages split by time zones for the last 7 days."""
    query = """
    WITH latest_user_date AS (
        SELECT DATE(MAX(activity_minute)) AS latest_date
        FROM minute_data
        WHERE user_id = %s
    ),
    met_by_timezone AS (
        SELECT
            m.user_id,
            ROUND(AVG(CASE WHEN EXTRACT(HOUR FROM m.activity_minute) BETWEEN 0 AND 5 THEN m.mets ELSE NULL END), 2) AS early_morning_avg_met,
            ROUND(AVG(CASE WHEN EXTRACT(HOUR FROM m.activity_minute) BETWEEN 6 AND 11 THEN m.mets ELSE NULL END), 2) AS morning_avg_met,
            ROUND(AVG(CASE WHEN EXTRACT(HOUR FROM m.activity_minute) BETWEEN 12 AND 17 THEN m.mets ELSE NULL END), 2) AS afternoon_avg_met,
            ROUND(AVG(CASE WHEN EXTRACT(HOUR FROM m.activity_minute) BETWEEN 18 AND 23 THEN m.mets ELSE NULL END), 2) AS evening_avg_met
        FROM minute_data m
        JOIN latest_user_date l ON DATE(m.activity_minute) BETWEEN l.latest_date - INTERVAL '6 days' AND l.latest_date
        WHERE m.user_id = %s
        GROUP BY m.user_id
    )
    SELECT 
        tz.user_id,
        tz.early_morning_avg_met, em_md.likely_activity AS early_morning_likely_activity,
        tz.morning_avg_met, m_md.likely_activity AS morning_likely_activity,
        tz.afternoon_avg_met, a_md.likely_activity AS afternoon_likely_activity,
        tz.evening_avg_met, e_md.likely_activity AS evening_likely_activity
    FROM met_by_timezone tz
    LEFT JOIN met_data em_md ON tz.early_morning_avg_met BETWEEN em_md.met_min AND em_md.met_max
    LEFT JOIN met_data m_md ON tz.morning_avg_met BETWEEN m_md.met_min AND m_md.met_max
    LEFT JOIN met_data a_md ON tz.afternoon_avg_met BETWEEN a_md.met_min AND a_md.met_max
    LEFT JOIN met_data e_md ON tz.evening_avg_met BETWEEN e_md.met_min AND e_md.met_max;
    """

    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute(query, (user_id, user_id))
        result = cursor.fetchone()

    if not result:
        return None

    return {
        "user_id": result[0],
        "early_morning": {"avg_met": result[1], "activity": result[2]},
        "morning": {"avg_met": result[3], "activity": result[4]},
        "afternoon": {"avg_met": result[5], "activity": result[6]},
        "evening": {"avg_met": result[7], "activity": result[8]},
    }


def lambda_handler(event, context):
    user_id = event.get("user_id")
    if not user_id:
        return {"error": "user_id is required"}

    return {
        "daily_metrics": fetch_daily_metrics(user_id),
        "met_values": fetch_met_values(user_id)
    }
