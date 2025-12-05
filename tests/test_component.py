import sys
import os
import unittest
import mock
from pathlib import Path
from freezegun import freeze_time
from component import Component

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    def _run_test_case(self, case_name):
        """Helper to run a test case by name."""
        path = os.path.join(os.path.dirname(__file__), "data", case_name)
        os.environ["KBC_DATADIR"] = path

        comp = Component()
        comp.run()

        return Path(path) / "out"

    @freeze_time("2010-10-10")
    @mock.patch("component.pyairtable.metadata.get_base_schema")
    @mock.patch("component.pyairtable.metadata.get_table_schema")
    @mock.patch("component.ApiTable.iterate")
    def test_case_1(self, mock_iterate, mock_table_schema, mock_base_schema):
        """Test full sync extraction from Airtable."""

        # Mock the base schema response
        mock_base_schema.return_value = {
            "tables": [
                {
                    "id": "tblMockTableId",
                    "name": "test_table",
                    "fields": [
                        {"id": "fld1", "name": "Name"},
                        {"id": "fld2", "name": "Status"},
                        {"id": "fld3", "name": "Priority"},
                    ],
                }
            ]
        }

        # Mock the table schema response
        mock_table_schema.return_value = {
            "fields": [
                {"name": "Name", "type": "singleLineText"},
                {"name": "Status", "type": "singleSelect"},
                {"name": "Priority", "type": "singleLineText"},
            ]
        }

        # Mock the iterate response with test data
        mock_iterate.return_value = [
            [
                {
                    "id": "rec123",
                    "createdTime": "2024-01-15T10:30:00.000Z",
                    "fields": {"Name": "Test Record 1", "Status": "Active", "Priority": "1"},
                },
                {
                    "id": "rec456",
                    "createdTime": "2024-01-15T11:00:00.000Z",
                    "fields": {"Name": "Test Record 2", "Status": "Completed", "Priority": "2"},
                },
                {
                    "id": "rec789",
                    "createdTime": "2024-01-15T12:00:00.000Z",
                    "fields": {"Name": "Test Record 3", "Status": "Pending", "Priority": "3"},
                },
            ]
        ]

        # Run the test case
        out_dir = self._run_test_case("test_case_1")

        # Verify output exists
        self.assertTrue((out_dir / "tables" / "test_table.csv").exists())
        self.assertTrue((out_dir / "tables" / "test_table.csv.manifest").exists())

    @freeze_time("2010-10-10")
    @mock.patch("component.pyairtable.metadata.get_base_schema")
    @mock.patch("component.pyairtable.metadata.get_table_schema")
    @mock.patch("component.ApiTable.iterate")
    def test_case_2(self, mock_iterate, mock_table_schema, mock_base_schema):
        """Test full sync extraction of contracts table from Airtable."""

        # Mock the base schema response with all contract fields
        mock_base_schema.return_value = {
            "tables": [
                {
                    "id": "tblTWvW5ImNDQm99d",
                    "name": "airtable_contracts",
                    "fields": [
                        {"id": "fldB1KxDHgnCDiBIn", "name": "Ci____ty"},
                        {"id": "fldkZhH0mMvhHzrQG", "name": "Customer_Id"},
                        {"id": "fldbuTq3l1oLoAXcI", "name": "First_Name"},
                        {"id": "fldjW7jUgsiTCndCo", "name": "Index"},
                        {"id": "fldvPqu8c6Uc6zVK9", "name": "Phone_1"},
                        {"id": "fld1yVd7kHvUZOakc", "name": "Email"},
                        {"id": "fld_country", "name": "Country"},
                        {"id": "fld_website", "name": "Website"},
                        {"id": "fld_subdate", "name": "Subscription_Date"},
                        {"id": "fld_company", "name": "_Company_"},
                        {"id": "fld_phone2", "name": "Phone_2_"},
                        {"id": "fld_lastname", "name": "Last_Name___"},
                    ],
                }
            ]
        }

        # Mock the table schema response
        mock_table_schema.return_value = {
            "fields": [
                {"name": "Ci____ty", "type": "singleLineText"},
                {"name": "Customer_Id", "type": "singleLineText"},
                {"name": "First_Name", "type": "singleLineText"},
                {"name": "Index", "type": "number"},
                {"name": "Phone_1", "type": "phoneNumber"},
                {"name": "Email", "type": "email"},
                {"name": "Country", "type": "singleLineText"},
                {"name": "Website", "type": "url"},
                {"name": "Subscription_Date", "type": "date"},
                {"name": "_Company_", "type": "singleLineText"},
                {"name": "Phone_2_", "type": "phoneNumber"},
                {"name": "Last_Name___", "type": "singleLineText"},
            ]
        }

        # Mock the iterate response with test data
        mock_iterate.return_value = [
            [
                {
                    "id": "rec00838jsiDO8Hau",
                    "createdTime": "2025-11-19T16:47:09.000Z",
                    "fields": {
                        "Ci____ty": "New Russell",
                        "Customer_Id": "Dc3Eb2268524A69",
                        "First_Name": "James",
                        "Index": 7749,
                        "Phone_1": "989-616-0860",
                        "Email": "raymond98@oconnell.info",
                        "Country": "Indonesia",
                        "Website": "http://gross.biz/",
                        "Subscription_Date": "2020-05-19",
                        "_Company_": "Ramsey, Barr and Dawson",
                        "Phone_2_": "719-624-0683x1496",
                        "Last_Name___": "Hines",
                    },
                },
                {
                    "id": "rec027WtYu7rZJycP",
                    "createdTime": "2025-11-19T16:47:09.000Z",
                    "fields": {
                        "Ci____ty": "Baldwinfort",
                        "Customer_Id": "a1A7c5d6B086dBF",
                        "First_Name": "Holly",
                        "Index": 2145,
                        "Phone_1": "218.645.5150x16428",
                        "Email": "shelleyleonard@carlson.net",
                        "Country": "Honduras",
                        "Website": "https://grimes-fry.com/",
                        "Subscription_Date": "2021-06-02",
                        "_Company_": "Garrett, Fuentes and Olson",
                        "Phone_2_": "860-912-6407",
                        "Last_Name___": "Lin",
                    },
                },
                {
                    "id": "rec02Gs4OYtZ72dtw",
                    "createdTime": "2025-11-19T16:47:09.000Z",
                    "fields": {
                        "Ci____ty": "New Jefftown",
                        "Customer_Id": "a23B9b1B24455Bd",
                        "First_Name": "Julia",
                        "Index": 4394,
                        "Phone_1": "001-458-732-7019x0126",
                        "Email": "conneraaron@villegas-herring.com",
                        "Country": "Niue",
                        "Website": "https://kaufman.com/",
                        "Subscription_Date": "2021-01-16",
                        "_Company_": "Hodges, Whitehead and Hobbs",
                        "Phone_2_": "652.880.5694x7294",
                        "Last_Name___": "Bradford",
                    },
                },
            ]
        ]

        # Run the test case
        out_dir = self._run_test_case("test_case_2")

        # Verify output exists
        self.assertTrue((out_dir / "tables" / "airtable_contracts.csv").exists())
        self.assertTrue((out_dir / "tables" / "airtable_contracts.csv.manifest").exists())

    @mock.patch("component.pyairtable.metadata.get_api_bases")
    def test_connection_without_base_and_table(self, mock_get_api_bases):
        """Test that testConnection works with only API key, no base_id or table_name."""
        # Mock the API bases response
        mock_get_api_bases.return_value = {
            "bases": [
                {"id": "appTestBase1", "name": "Test Base 1"},
                {"id": "appTestBase2", "name": "Test Base 2"},
            ]
        }

        # Set up test environment with only API key
        path = os.path.join(os.path.dirname(__file__), "data", "test_connection_only")
        os.environ["KBC_DATADIR"] = path

        # Create component and run testConnection
        comp = Component()
        comp.test_connection()

        # If we get here without exception, the test passed
        self.assertTrue(True)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
