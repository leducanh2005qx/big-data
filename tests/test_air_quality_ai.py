import importlib
import os
import unittest
from unittest.mock import patch


ai = importlib.import_module("scripts.air_quality_ai")


def fake_rows_payload():
    groups = [
        ({"city": "Hanoi", "pollutant": "us_aqi", "metric_type": "aqi", "source": "open_meteo_air_quality", "data_mode": "model_estimate", "aqi_standard": "US-AQI"}, [["2026-11-02T00:00:00Z", 120]]),
        ({"city": "Ho Chi Minh City", "pollutant": "us_aqi", "metric_type": "aqi", "source": "open_meteo_air_quality", "data_mode": "model_estimate", "aqi_standard": "US-AQI"}, [["2026-11-02T00:00:00Z", 80]]),
        ({"city": "Da Nang", "pollutant": "us_aqi", "metric_type": "aqi", "source": "open_meteo_air_quality", "data_mode": "model_estimate", "aqi_standard": "US-AQI"}, [["2026-11-02T00:00:00Z", 40]]),
        ({"city": "Hanoi", "pollutant": "pm25", "metric_type": "concentration", "source": "open_meteo_air_quality", "data_mode": "model_estimate"}, [["2026-11-02T00:00:00Z", 36]]),
        ({"city": "Hanoi", "pollutant": "pm10", "metric_type": "concentration", "source": "open_meteo_air_quality", "data_mode": "model_estimate"}, [["2026-11-02T00:00:00Z", 44]]),
    ]
    return {"results": [{"series": [{"columns": ["time", "value"], "tags": tags, "values": values} for tags, values in groups]}]}


class AirQualityAiTests(unittest.TestCase):
    def setUp(self):
        ai._cache.clear()

    def test_summary_is_structured_and_cached(self):
        with patch.object(ai, "influx_query", return_value=fake_rows_payload()) as query, \
                patch.object(ai, "influx_write_summary"), \
                patch.object(ai, "_ai_text", return_value="Hà Nội có AQI cao nhất trong dữ liệu.") as model, \
                patch.dict(os.environ, {"AI_API_KEY": "configured"}):
            first = ai.summary_for("overview", hours=24)
            second = ai.summary_for("overview", hours=24)

        self.assertEqual(first["mode"], "ai")
        self.assertEqual(first["scope"], "overview")
        self.assertIn("Hà Nội", first["summary"])
        self.assertIn("data_updated_at", first)
        self.assertIn("generated_at", first)
        self.assertIn("response_time_ms", first)
        self.assertEqual(first, second)
        self.assertEqual(query.call_count, 2)
        self.assertEqual(model.call_count, 1)

    def test_all_required_summary_scopes_use_real_rows(self):
        with patch.object(ai, "influx_query", return_value=fake_rows_payload()), \
                patch.object(ai, "influx_write_summary"), \
                patch.object(ai, "_ai_text", return_value=None), \
                patch.dict(os.environ, {"AI_API_KEY": ""}):
            results = [ai.summary_for(scope, hours=24) for scope in ("overview", "aqi-trend", "particulate", "compare")]

        self.assertEqual({result["scope"] for result in results}, {"overview", "aqi-trend", "particulate", "compare"})
        self.assertTrue(all(result["mode"] == "data_summary" for result in results))
        self.assertTrue(all(result["highlights"] for result in results))

    def test_fallback_is_explicit_when_data_is_empty(self):
        empty = {"results": [{}]}
        with patch.object(ai, "influx_query", return_value=empty), patch.object(ai, "influx_write_summary"), patch.dict(os.environ, {"AI_API_KEY": ""}):
            result = ai.summary_for("overview", hours=24)

        self.assertEqual(result["mode"], "data_summary")
        self.assertIn("Chưa có dữ liệu", result["summary"])
        self.assertTrue(result["warnings"])

    def test_validation_and_health_do_not_expose_key(self):
        self.assertEqual(ai.normalize_city("hanoi"), "Hanoi")
        self.assertEqual(ai.normalize_city("TP.HCM"), "Ho Chi Minh City")
        self.assertEqual(ai.normalize_city("Cao Bằng"), "Cao Bang")
        self.assertEqual(ai.normalize_city("Lâm Đồng"), "Lam Dong")
        with self.assertRaises(ValueError):
            ai.parse_hours("0")
        with patch.dict(os.environ, {"AI_API_KEY": "super-secret"}):
            health = ai.health_payload()
        self.assertTrue(health["ai_configured"])
        self.assertNotIn("super-secret", str(health))
        self.assertNotIn("AI_API_KEY", str(health))

    def test_aqi_trend_uses_the_selected_time_series(self):
        rows = [
            {"city": "Hanoi", "pollutant": "us_aqi", "metric_type": "aqi", "aqi_standard": "US-AQI", "source": "open_meteo_air_quality", "data_mode": "model_estimate", "time": "2026-11-02T00:00:00Z", "value": 40},
            {"city": "Hanoi", "pollutant": "us_aqi", "metric_type": "aqi", "aqi_standard": "US-AQI", "source": "open_meteo_air_quality", "data_mode": "model_estimate", "time": "2026-11-02T12:00:00Z", "value": 80},
        ]
        stats = ai._stats_for("aqi-trend", rows, "Hanoi", 24)
        self.assertEqual(stats["aqi_trend"]["direction"], "tăng")
        self.assertEqual(stats["aqi_trend"]["maximum"], 80)
        self.assertEqual(stats["aqi_trend"]["delta"], 40)

    def test_question_router_returns_only_requested_ranked_value(self):
        rows = [
            {"city": "Hanoi", "pollutant": "pm25", "value": 36, "event_time": "2026-11-02T00:00:00Z"},
            {"city": "Da Nang", "pollutant": "pm25", "value": 11, "event_time": "2026-11-02T00:00:00Z"},
            {"city": "Can Tho", "pollutant": "pm25", "value": 22, "event_time": "2026-11-02T00:00:00Z"},
            {"city": "Hanoi", "pollutant": "pm10", "value": 44, "event_time": "2026-11-02T00:00:00Z"},
        ]
        answer = ai.deterministic_answer("Thành phố nào có PM2.5 cao nhất?", rows)
        self.assertIn("Hà Nội", answer)
        self.assertIn("36", answer)
        self.assertNotIn("Đà Nẵng", answer)
        self.assertNotIn("PM10", answer)

    def test_question_router_filters_city_and_pollutant(self):
        answer = ai.deterministic_answer("PM2.5 ở Hà Nội là bao nhiêu?", [
            {"city": "Hanoi", "pollutant": "pm25", "value": 36, "event_time": "2026-11-02T00:00:00Z"},
            {"city": "Hanoi", "pollutant": "pm10", "value": 44, "event_time": "2026-11-02T00:00:00Z"},
        ])
        self.assertEqual(answer, "Hà Nội: PM2.5 = 36 µg/m³; thời điểm bản ghi: 2026-11-02T00:00:00Z.")

    def test_question_router_compares_only_requested_cities(self):
        rows = [
            {"city": "Hanoi", "pollutant": "us_aqi", "value": 90, "aqi_standard": "US-AQI", "event_time": "2026-11-02T00:00:00Z"},
            {"city": "Hanoi", "pollutant": "pm25", "value": 36, "event_time": "2026-11-02T00:00:00Z"},
            {"city": "Hanoi", "pollutant": "pm10", "value": 44, "event_time": "2026-11-02T00:00:00Z"},
            {"city": "Da Lat", "pollutant": "us_aqi", "value": 33, "aqi_standard": "US-AQI", "event_time": "2026-11-02T00:00:00Z"},
            {"city": "Da Lat", "pollutant": "pm25", "value": 8, "event_time": "2026-11-02T00:00:00Z"},
            {"city": "Da Nang", "pollutant": "us_aqi", "value": 40, "aqi_standard": "US-AQI", "event_time": "2026-11-02T00:00:00Z"},
        ]
        answer = ai.deterministic_answer("Tôi muốn đi du lịch, hãy so sánh ô nhiễm ở Hà Nội và Đà Lạt", rows)
        self.assertIn("Hà Nội", answer)
        self.assertIn("Đà Lạt", answer)
        self.assertIn("AQI 90", answer)
        self.assertIn("AQI 33", answer)
        self.assertNotIn("Đà Nẵng", answer)
        self.assertIn("phần trăm", answer)

    def test_question_router_accepts_unaccented_city_names(self):
        answer = ai.deterministic_answer("So sanh Ha Noi va Da Lat", [
            {"city": "Hanoi", "pollutant": "us_aqi", "value": 90, "aqi_standard": "US-AQI", "event_time": "2026-11-02T00:00:00Z"},
            {"city": "Da Lat", "pollutant": "us_aqi", "value": 33, "aqi_standard": "US-AQI", "event_time": "2026-11-02T00:00:00Z"},
        ])
        self.assertIn("Hà Nội", answer)
        self.assertIn("Đà Lạt", answer)

    def test_dashboard_context_answers_scope_question(self):
        context = ai.dashboard_context_payload({"dashboard": "air-quality-overview", "city": "Hanoi"})
        answer = ai.dashboard_scope_answer("Dashboard này nói về chỉ số gì?", context)
        self.assertIn("AQI", answer)
        self.assertIn("PM2.5", answer)
        self.assertIn("PM10", answer)
        self.assertIn("Hanoi", answer)

    def test_single_city_question_returns_compact_metric_summary(self):
        answer = ai.deterministic_answer("Hà Nội hiện thế nào?", [
            {"city": "Hanoi", "pollutant": "us_aqi", "value": 78, "event_time": "2026-11-11T10:00:00Z"},
            {"city": "Hanoi", "pollutant": "pm25", "value": 4.1, "event_time": "2026-11-11T10:00:00Z"},
            {"city": "Hanoi", "pollutant": "pm10", "value": 4.1, "event_time": "2026-11-11T10:00:00Z"},
        ])
        self.assertIn("AQI", answer)
        self.assertIn("PM2.5", answer)
        self.assertIn("PM10", answer)

    def test_chat_page_has_dashboard_navigation_and_quick_actions(self):
        self.assertIn("Quay về Grafana", ai.HTML)
        self.assertIn("Tóm tắt dashboard", ai.HTML)
        self.assertIn("Phân tích bụi mịn", ai.HTML)
        self.assertIn("Xóa cuộc trò chuyện", ai.HTML)
        self.assertIn("dashboard_context", ai.HTML)


if __name__ == "__main__":
    unittest.main()
