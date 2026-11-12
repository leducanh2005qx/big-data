import importlib
import os
import unittest
from unittest.mock import patch


features = importlib.import_module("scripts.air_quality_features")
ai = importlib.import_module("scripts.air_quality_ai")


def payload(groups):
    return {"results": [{"series": [{"columns": ["time", "value"], "tags": tags, "values": values} for tags, values in groups]}]}


class AirQualityFeatureTests(unittest.TestCase):
    def test_forecast_returns_bounded_estimate(self):
        groups = [
            ({"city": "Hanoi", "pollutant": "us_aqi"}, [[f"2026-11-11T{hour:02d}:00:00Z", 50 + hour] for hour in range(6)]),
        ]
        with patch.object(features, "influx_query", return_value=payload(groups)):
            result = features.forecast_aqi("Hanoi", horizon=6)
        forecast = result["cities"]["Hanoi"]["forecast"]
        self.assertEqual(len(forecast), 6)
        self.assertTrue(all(0 <= item["aqi"] <= 500 for item in forecast))
        self.assertTrue(result["is_estimate"])

    def test_anomaly_detection_uses_median_mad(self):
        groups = [
            ({"city": "Hanoi", "pollutant": "pm25"}, [[f"2026-11-11T{hour:02d}:00:00Z", 10 if hour < 5 else 80] for hour in range(6)]),
        ]
        with patch.object(features, "influx_query", return_value=payload(groups)):
            result = features.detect_anomalies(hours=24)
        self.assertEqual(len(result["anomalies"]), 1)
        self.assertEqual(result["anomalies"][0]["city"], "Hanoi")
        self.assertEqual(result["anomalies"][0]["direction"], "tăng đột biến")

    def test_recommendation_ranks_lower_pollution_first(self):
        groups = [
            ({"city": "Hanoi", "pollutant": "us_aqi"}, [["2026-11-11T10:00:00Z", 120]]),
            ({"city": "Hanoi", "pollutant": "pm25"}, [["2026-11-11T10:00:00Z", 40]]),
            ({"city": "Da Lat", "pollutant": "us_aqi"}, [["2026-11-11T10:00:00Z", 35]]),
            ({"city": "Da Lat", "pollutant": "pm25"}, [["2026-11-11T10:00:00Z", 8]]),
        ]
        with patch.object(features, "influx_query", return_value=payload(groups)):
            result = features.recommend_cities()
        self.assertEqual(result["recommendations"][0]["city"], "Da Lat")
        self.assertGreater(result["recommendations"][0]["score"], result["recommendations"][1]["score"])

    def test_email_is_disabled_by_default_and_does_not_send(self):
        alerts = [{"message": "test", "time": "2026-11-11T10:00:00Z"}]
        with patch.dict(os.environ, {"ALERT_EMAIL_ENABLED": "false", "ALERT_EMAIL_DRY_RUN": "true"}, clear=False), \
                patch.object(features.smtplib, "SMTP") as smtp:
            result = features.send_alert_email(alerts)
        self.assertFalse(result["sent"])
        self.assertEqual(result["reason"], "email_disabled")
        smtp.assert_not_called()

    def test_email_alerts_only_include_anomaly_or_forecast_crossing(self):
        current_rows = [{"city": "Hanoi", "pollutant": "us_aqi", "time": "2026-11-11T10:00:00Z", "value": 80}]
        anomaly = {"anomalies": [{"city": "Hanoi", "label": "Hà Nội", "pollutant": "pm25", "value": 60, "direction": "tăng đột biến", "time": "2026-11-11T10:00:00Z"}]}
        forecast = {"cities": {"Hanoi": {"label": "Hà Nội", "forecast": [{"aqi": 105, "time": "2026-11-11T18:00:00Z"}]}}}
        with patch.object(features, "query_metric_rows", return_value=current_rows), \
                patch.object(features, "detect_anomalies", return_value=anomaly), \
                patch.object(features, "forecast_aqi", return_value=forecast), \
                patch.dict(os.environ, {"ALERT_AQI_THRESHOLD": "100"}, clear=False):
            alerts = features.collect_alerts()
        self.assertEqual({alert["kind"] for alert in alerts}, {"anomaly", "forecast"})
        self.assertTrue(all(alert["kind"] != "periodic_forecast_report" for alert in alerts))

    def test_demo_alert_is_explicitly_marked_and_has_preview(self):
        alerts = features.build_demo_alerts(151)
        preview = features.build_email_preview(alerts)
        self.assertEqual(alerts[0]["kind"], "demo")
        self.assertIn("DEMO", alerts[0]["message"])
        self.assertIn("không phải dữ liệu quan trắc thật", preview["body"])

    def test_ai_question_router_supports_new_feature_intents(self):
        context = {"city": "all", "hours": 24}
        with patch.object(ai, "recommend_cities", return_value={"recommendations": [{"label": "Đà Lạt", "score": 90, "aqi": 30, "pm25": 5}]}):
            answer = ai.feature_question_answer("Tôi muốn đi du lịch, nên chọn thành phố nào?", context)
        self.assertIn("Đà Lạt", answer)
        with patch.object(ai, "forecast_aqi", return_value={"cities": {"Hanoi": {"label": "Hà Nội", "current_aqi": 60, "current_category": "Trung bình", "forecast": [{"aqi": 65, "category": "Trung bình"}]}}}):
            answer = ai.feature_question_answer("Dự báo AQI 24 giờ tới?", {"city": "Hanoi", "hours": 24})
        self.assertIn("Hà Nội", answer)

    def test_feature_question_prefers_city_named_in_question(self):
        with patch.object(ai, "forecast_aqi", return_value={"cities": {"Hanoi": {"label": "Hà Nội", "current_aqi": 60, "current_category": "Trung bình", "forecast": [{"aqi": 65, "category": "Trung bình"}]}}}) as forecast:
            answer = ai.feature_question_answer("Dự báo AQI Hà Nội trong 24 giờ tới?", {"city": "all", "hours": 24})
        forecast.assert_called_once_with(city="Hanoi", horizon=24, history_hours=24)
        self.assertIn("Hà Nội", answer)


if __name__ == "__main__":
    unittest.main()
