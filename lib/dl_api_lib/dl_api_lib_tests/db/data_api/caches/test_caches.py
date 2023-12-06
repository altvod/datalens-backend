from __future__ import annotations

from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestDataCaches(DefaultApiTestBase):
    data_caches_enabled = True

    def test_cache_by_deleting_table(self, saved_dataset, control_api, data_api, sample_table):
        ds = saved_dataset
        ds = add_formulas_to_dataset(dataset=ds, api_v1=control_api, formulas={"sales sum": "SUM([sales])"})

        def get_data():
            result_resp = data_api.get_result(
                dataset=ds,
                fields=[
                    ds.find_field(title="city"),
                    ds.find_field(title="sales sum"),
                ],
            )
            assert result_resp.status_code == 200, result_resp.response_errors
            data = get_data_rows(response=result_resp)
            return data

        data_rows = get_data()

        # Now delete the table.
        # This will make real DB queries impossible,
        # however the cache should still return the same data
        sample_table.db.drop_table(sample_table.table)

        data_rows_after_deletion = get_data()
        assert data_rows_after_deletion == data_rows
