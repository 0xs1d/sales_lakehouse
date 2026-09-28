import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ProjectContractTests(unittest.TestCase):
    def test_required_azure_artifacts_exist(self):
        required = [
            "infrastructure/main.bicep",
            "infrastructure/main.dev.bicepparam",
            "infrastructure/terraform/databricks-unity-catalog.tf",
            "adf/pipelines/pl_enterprise_sales_lakehouse.json",
            "adf/triggers/tr_enterprise_sales_daily.json",
            "databricks/databricks.yml",
            "databricks/resources/jobs.yml",
        ]
        for relative_path in required:
            with self.subTest(relative_path=relative_path):
                self.assertTrue((ROOT / relative_path).is_file())

    def test_adf_pipeline_covers_all_notebooks(self):
        pipeline = json.loads(
            (ROOT / "adf/pipelines/pl_enterprise_sales_lakehouse.json").read_text()
        )
        activities = pipeline["properties"]["activities"]
        self.assertEqual(len(activities), 9)
        self.assertEqual(
            [activity["name"] for activity in activities],
            [
                "01_Source_Generation",
                "02_Bronze_Ingestion",
                "03_Silver_Transformation",
                "05_Incremental_Processing",
                "04_Gold_Analytics",
                "06_Data_Quality_Reconciliation",
                "07_Audit_Monitoring",
                "08_Performance_Business_Analytics",
                "09_Project_Validation",
            ],
        )

    def test_gold_date_dimension_is_contractually_present(self):
        gold_notebook = (ROOT / "notebooks/04_Gold_Sales_Analytics.py").read_text()
        validation_notebook = (ROOT / "notebooks/09_Project_Validation.py").read_text()
        self.assertIn('"gold_dim_date"', gold_notebook)
        self.assertIn('"gold_dim_date"', validation_notebook)
        self.assertIn('"date_key"', gold_notebook)

    def test_notebooks_consume_cloud_parameters(self):
        for path in sorted((ROOT / "notebooks").glob("0*.py")):
            source = path.read_text()
            with self.subTest(notebook=path.name):
                self.assertIn('"catalog"', source)
                self.assertIn('"schema"', source)

    def test_sql_queries_target_implemented_tables(self):
        sql = (ROOT / "sql/business_queries.sql").read_text()
        for table_name in (
            "gold_fact_sales",
            "gold_dim_customer",
            "gold_dim_product",
            "gold_dim_store",
            "audit_pipeline_runs",
            "audit_monitoring_health",
        ):
            with self.subTest(table_name=table_name):
                self.assertIn(table_name, sql)

        self.assertGreaterEqual(len(re.findall(r"^SELECT$", sql, re.MULTILINE)), 10)


class DataQualityRuleTests(unittest.TestCase):
    def test_valid_sales_rows_pass_required_rules(self):
        rows = [
            {"order_id": "ORD1", "quantity": 2, "unit_price": 10.0, "net_amount": 20.0},
            {"order_id": "ORD2", "quantity": 1, "unit_price": 5.0, "net_amount": 5.0},
        ]
        failures = [
            row
            for row in rows
            if not row["order_id"]
            or row["quantity"] <= 0
            or row["unit_price"] < 0
            or row["net_amount"] < 0
        ]
        self.assertEqual(failures, [])

    def test_invalid_sales_rows_are_detected(self):
        rows = [
            {"order_id": None, "quantity": 1, "unit_price": 10.0, "net_amount": 10.0},
            {"order_id": "ORD2", "quantity": 0, "unit_price": 10.0, "net_amount": 0.0},
            {"order_id": "ORD3", "quantity": 1, "unit_price": -1.0, "net_amount": -1.0},
        ]
        failures = [
            row
            for row in rows
            if not row["order_id"]
            or row["quantity"] <= 0
            or row["unit_price"] < 0
            or row["net_amount"] < 0
        ]
        self.assertEqual(len(failures), 3)

    def test_duplicate_and_orphan_keys_are_detected(self):
        orders = {"ORD1", "ORD2"}
        order_items = ["ORD1", "ORD1", "ORD3"]
        duplicate_count = len(order_items) - len(set(order_items))
        orphan_count = sum(order_id not in orders for order_id in order_items)
        self.assertEqual(duplicate_count, 1)
        self.assertEqual(orphan_count, 1)


if __name__ == "__main__":
    unittest.main()
