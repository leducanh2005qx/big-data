import unittest

from scripts.influxdb.retention import ensure_retention_policies


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def get_points(self):
        return iter(self.rows)


class FakeInfluxClient:
    def __init__(self, policy_names):
        self.policy_names = policy_names
        self.statements = []

    def query(self, statement):
        self.statements.append(statement)
        if statement.startswith("SHOW RETENTION POLICIES"):
            return FakeResult([{"name": name} for name in self.policy_names])
        return FakeResult([])


class EnsureRetentionPoliciesTests(unittest.TestCase):
    def test_creates_raw_and_aggregate_policies_and_limits_legacy_autogen(self):
        client = FakeInfluxClient(["autogen"])

        statements = ensure_retention_policies(client, "air_quality")

        self.assertEqual(len(statements), 3)
        self.assertIn('CREATE RETENTION POLICY "raw_7d" ON "air_quality" DURATION 7d REPLICATION 1 DEFAULT', statements)
        self.assertIn('CREATE RETENTION POLICY "aggregates_30d" ON "air_quality" DURATION 30d REPLICATION 1', statements)
        self.assertIn('ALTER RETENTION POLICY "autogen" ON "air_quality" DURATION 7d REPLICATION 1', statements)

    def test_alters_existing_policies_to_configured_durations(self):
        client = FakeInfluxClient(["autogen", "raw_7d", "aggregates_30d"])

        statements = ensure_retention_policies(client, "air_quality", "14d", "90d")

        self.assertTrue(statements[0].startswith('ALTER RETENTION POLICY "raw_7d"'))
        self.assertIn("DURATION 14d", statements[0])
        self.assertTrue(statements[1].startswith('ALTER RETENTION POLICY "aggregates_30d"'))
        self.assertIn("DURATION 90d", statements[1])

    def test_rejects_unsafe_database_names_and_durations(self):
        with self.assertRaises(ValueError):
            ensure_retention_policies(FakeInfluxClient([]), 'air_quality"; DROP DATABASE x')
        with self.assertRaises(ValueError):
            ensure_retention_policies(FakeInfluxClient([]), "air_quality", "INF")


if __name__ == "__main__":
    unittest.main()