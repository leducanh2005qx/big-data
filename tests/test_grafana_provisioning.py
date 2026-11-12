import json
from pathlib import Path
import unittest

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROVISIONING_ROOT = PROJECT_ROOT / "grafana" / "provisioning"


class GrafanaProvisioningTests(unittest.TestCase):
    def test_dashboard_datasource_alert_and_contact_point_references_match(self):
        datasource_config = yaml.safe_load(
            (PROVISIONING_ROOT / "datasources" / "air-quality.yaml").read_text(encoding="utf-8")
        )
        contact_config = yaml.safe_load(
            (PROVISIONING_ROOT / "alerting" / "contact-points.yaml").read_text(encoding="utf-8")
        )
        alert_config = yaml.safe_load(
            (PROVISIONING_ROOT / "alerting" / "alert-rules.yaml").read_text(encoding="utf-8")
        )
        dashboard = json.loads(
            (PROJECT_ROOT / "grafana" / "dashboards" / "air-quality-overview.json").read_text(encoding="utf-8")
        )

        datasource_uid = datasource_config["datasources"][0]["uid"]
        dashboard_ids = {panel["id"] for panel in dashboard["panels"]}
        contact_name = contact_config["contactPoints"][0]["name"]
        alert_rule = alert_config["groups"][0]["rules"][0]
        target = alert_rule["data"][0]

        self.assertEqual(datasource_uid, "air-quality-influx")
        self.assertEqual(alert_rule["dashboardUid"], dashboard["uid"])
        self.assertIn(alert_rule["panelId"], dashboard_ids)
        self.assertEqual(target["datasourceUid"], datasource_uid)
        self.assertIn('"raw_7d"."air_quality_observation"', target["model"]["query"])
        self.assertEqual(alert_rule["notification_settings"]["receiver"], contact_name)
        self.assertEqual(
            contact_config["contactPoints"][0]["receivers"][0]["settings"]["url"],
            "$ALERT_WEBHOOK_URL",
        )

    def test_dashboard_includes_freshness_coverage_and_metric_summary_panels(self):
        dashboard = json.loads(
            (PROJECT_ROOT / "grafana" / "dashboards" / "air-quality-overview.json").read_text(encoding="utf-8")
        )
        panels_by_title = {panel["title"]: panel for panel in dashboard["panels"]}
        pm25_panel = panels_by_title["Diễn biến PM2.5"]
        collection_panel = panels_by_title["Trạng thái bộ thu thập và độ mới dữ liệu"]

        self.assertIn('"aggregates_30d"."air_quality_hourly"', pm25_panel["targets"][0]["query"])
        self.assertIn('"unit" = \'ug/m3\'', pm25_panel["targets"][0]["query"])
        self.assertIn('"air_quality_collection_status"', collection_panel["targets"][0]["query"])
        self.assertIn('last("poll_completed_at")', collection_panel["targets"][0]["query"])

    def test_dashboard_has_ai_summary_panels_and_all_dust_map_layers(self):
        dashboard = json.loads(
            (PROJECT_ROOT / "grafana" / "dashboards" / "air-quality-overview.json").read_text(encoding="utf-8")
        )
        panels_by_title = {panel["title"]: panel for panel in dashboard["panels"]}
        self.assertEqual(panels_by_title["AI phân tích xu hướng AQI"]["type"], "table")
        self.assertEqual(panels_by_title["AI nhận xét bụi mịn"]["type"], "table")
        self.assertIn('"air_quality_ai_summary"', panels_by_title["AI tổng hợp dữ liệu hiện tại"]["targets"][0]["query"])
        self.assertIn('last("response_time_ms")', panels_by_title["AI tổng hợp dữ liệu hiện tại"]["targets"][0]["query"])
        map_panel = panels_by_title["Bản đồ AQI và nồng độ bụi theo thành phố"]
        self.assertEqual({target["refId"] for target in map_panel["targets"]}, {"A", "B", "C"})
        self.assertEqual({layer["name"] for layer in map_panel["options"]["layers"]}, {"AQI theo thành phố", "Nồng độ PM2.5", "Nồng độ PM10"})

    def test_map_layers_bind_numeric_fields_to_colored_markers(self):
        dashboard = json.loads(
            (PROJECT_ROOT / "grafana" / "dashboards" / "air-quality-overview.json").read_text(encoding="utf-8")
        )
        map_panel = next(panel for panel in dashboard["panels"] if panel["title"] == "Bản đồ AQI và nồng độ bụi theo thành phố")
        layers = {layer["name"]: layer for layer in map_panel["options"]["layers"]}
        self.assertEqual(layers["AQI theo thành phố"]["config"]["style"]["color"]["field"], "aqi")
        self.assertEqual(layers["Nồng độ PM2.5"]["config"]["style"]["color"]["field"], "pm25")
        self.assertEqual(layers["Nồng độ PM10"]["config"]["style"]["color"]["field"], "pm10")
        self.assertTrue(all("fixed" in layer["config"]["style"]["color"] for layer in layers.values()))
        self.assertIn("\"pollutant\" = 'us_aqi'", map_panel["targets"][0]["query"])
        self.assertIn("\"aqi_standard\" = 'US-AQI'", map_panel["targets"][0]["query"])

    def test_dashboard_has_air_quality_sections_and_filters(self):
        dashboard = json.loads(
            (PROJECT_ROOT / "grafana" / "dashboards" / "air-quality-overview.json").read_text(encoding="utf-8")
        )
        titles = {panel["title"] for panel in dashboard["panels"]}
        self.assertIn("AQI hiện tại", titles)
        self.assertIn("So sánh PM2.5 giữa các thành phố", titles)
        self.assertIn("CO, NO2, SO2 và O3", titles)
        variables = {variable["name"] for variable in dashboard["templating"]["list"]}
        self.assertEqual(variables, {"country", "city", "source"})

    def test_dashboard_ai_link_carries_current_dashboard_context(self):
        dashboard = json.loads(
            (PROJECT_ROOT / "grafana" / "dashboards" / "air-quality-overview.json").read_text(encoding="utf-8")
        )
        ai_panel = next(panel for panel in dashboard["panels"] if panel["title"] == "Trợ lý AI chất lượng không khí")
        content = ai_panel["options"]["content"]
        self.assertIn("dashboard=air-quality-overview", content)
        self.assertIn("city=${city}", content)
        self.assertIn("from=${__from}", content)
        self.assertIn("to=${__to}", content)

    def test_dashboard_provider_uses_mounted_dashboard_directory(self):
        dashboard_provider = yaml.safe_load(
            (PROVISIONING_ROOT / "dashboards" / "provider.yaml").read_text(encoding="utf-8")
        )

        self.assertEqual(
            dashboard_provider["providers"][0]["options"]["path"],
            "/var/lib/grafana/dashboards",
        )


if __name__ == "__main__":
    unittest.main()
