import unittest
from unittest.mock import Mock

from pipeline.fec.openfec import OpenFECClient, OpenFECError


class OpenFECPaginationTests(unittest.TestCase):
    def client(self, responses):
        client = OpenFECClient(api_key="test-only")
        client.get = Mock(side_effect=responses)
        return client

    def test_schedule_a_uses_all_returned_cursor_fields_and_ignores_approximate_pages(self):
        client = self.client([
            {"results": [{"sub_id": "1"}], "pagination": {"pages": 1, "last_indexes": {"last_index": "1", "last_contribution_receipt_date": "2026-01-01"}}},
            {"results": [{"sub_id": "2"}], "pagination": {"pages": 1, "last_indexes": {"last_index": "2", "sort_null_only": True}}},
            {"results": []},
        ])
        self.assertEqual(list(client.iter_results("schedules/schedule_a/", committee_id="C123")), [{"sub_id": "1"}, {"sub_id": "2"}])
        calls = client.get.call_args_list
        self.assertNotIn("page", calls[0].kwargs)
        self.assertEqual(calls[1].kwargs["last_contribution_receipt_date"], "2026-01-01")
        self.assertTrue(calls[2].kwargs["sort_null_only"])
        self.assertNotIn("last_contribution_receipt_date", calls[2].kwargs)
        self.assertEqual(calls[2].kwargs["committee_id"], "C123")

    def test_repeated_cursor_raises_before_yielding_duplicate_page(self):
        payload = {"results": [{"sub_id": "1"}], "pagination": {"last_indexes": {"last_index": "1"}}}
        client = self.client([payload, payload])
        stream = client.iter_results("schedules/schedule_a/")
        self.assertEqual(next(stream), {"sub_id": "1"})
        with self.assertRaisesRegex(OpenFECError, "repeated"):
            next(stream)

    def test_missing_cursor_fails_instead_of_looping_first_page(self):
        client = self.client([{"results": [{"sub_id": "1"}], "pagination": {"pages": 50}}])
        with self.assertRaisesRegex(OpenFECError, "without a pagination cursor"):
            list(client.iter_results("schedules/schedule_a/"))

    def test_page_number_endpoint_retains_original_behavior(self):
        client = self.client([
            {"results": [{"id": 1}], "pagination": {"pages": 2}},
            {"results": [{"id": 2}], "pagination": {"pages": 2}},
        ])
        self.assertEqual(list(client.iter_results("committees/")), [{"id": 1}, {"id": 2}])
        self.assertEqual([call.kwargs["page"] for call in client.get.call_args_list], [1, 2])

    def test_explicit_bound_stops_cursor_iteration(self):
        client = self.client([{"results": [{"id": 1}], "pagination": {"last_indexes": {"last_index": "1"}}}])
        self.assertEqual(list(client.iter_results("schedules/schedule_a/", max_pages=1)), [{"id": 1}])
        self.assertEqual(client.get.call_count, 1)

    def test_resuming_at_date_cursor_drops_it_when_entering_null_dates(self):
        client = self.client([
            {"results": [{"id": 2}], "pagination": {"last_indexes": {"last_index": "2", "sort_null_only": True}}},
            {"results": []},
        ])
        list(client.iter_results("schedules/schedule_a/", last_index="1", last_contribution_receipt_date="2026-01-01"))
        self.assertNotIn("last_contribution_receipt_date", client.get.call_args_list[1].kwargs)

    def test_schedule_b_uses_seek_cursor_from_first_call(self):
        client = self.client([{"results": []}])
        self.assertEqual(list(client.iter_results("schedules/schedule_b/")), [])
        self.assertNotIn("page", client.get.call_args.kwargs)


if __name__ == "__main__":
    unittest.main()
