"""Contracts and publication-order regression tests; unittest or pytest.

Storage/Spark writers are fakes: these tests never contact S3 or SQL Server.
"""

import io
import json
import os
import unittest
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

from src.common.data_quality import (
    DataQualityError, date_range, max_rejection_rate, record_failure,
    validate_payload, validate_rows, validate_summary,
)
from src.transform.api_daily import (
    fear_greed_rows, market_chart_rows, reject_reason, split_valid_rejected,
    validate_api_batch,
)

DAY = date(2025, 11, 14)
MS = 1763078400000


def market(symbol="BTCUSDT"):
    return dict(symbol=symbol, coin_id="bitcoin", snapshot_date=DAY,
                source_ts_ms=MS, price_usd=10.0, market_cap_usd=None,
                total_volume_usd=None)


class ContractTests(unittest.TestCase):
    def test_partial_historical_api_batch_without_coverage_contract_passes(self):
        validate_api_batch("market", [market()], [], symbols=("BTCUSDT",),
                           through=DAY + timedelta(days=5))

    def test_delayed_ingestion_logical_date_is_not_payload_date(self):
        validate_api_batch("market", [market()], [], symbols=("BTCUSDT",),
                           through=DAY - timedelta(days=5))

    def setUp(self):
        self.env = patch.dict(os.environ, {"DQ_MAX_REJECTION_RATE": "0.01"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_valid_and_nullable_aggregates_pass(self):
        rows = [market()]
        self.assertIsNone(reject_reason(rows[0]))
        validate_api_batch("market", rows, [], symbols=("BTCUSDT",), through=DAY)

    def test_empty_fails(self):
        with self.assertRaisesRegex(DataQualityError, "empty input"):
            validate_api_batch("market", [], [], symbols=("BTCUSDT",), through=DAY)

    def test_invalid_numbers_reject_and_fail_batch(self):
        for field in ("price_usd", "market_cap_usd", "total_volume_usd"):
            for value in (float("nan"), float("inf"), -float("inf"), -1, "bad", True):
                with self.subTest(field=field, value=value):
                    valid, rejected = split_valid_rejected([{**market(), field: value}])
                    self.assertEqual(len(rejected), 1)
                    with self.assertRaises(DataQualityError):
                        validate_api_batch("market", valid, rejected,
                                           symbols=("BTCUSDT",), through=DAY)

    def test_zero_price_fails_but_zero_optional_aggregates_pass(self):
        self.assertEqual(reject_reason({**market(), "price_usd": 0}), "non_positive_price")
        self.assertIsNone(reject_reason({**market(), "market_cap_usd": 0,
                                         "total_volume_usd": 0}))

    def test_missing_symbols(self):
        with self.assertRaisesRegex(DataQualityError, "missing required symbols"):
            validate_api_batch("market", [market()], [],
                               symbols=("BTCUSDT", "ETHUSDT"), through=DAY)

    def test_required_fields(self):
        for field in ("symbol", "coin_id", "price_usd", "snapshot_date"):
            with self.subTest(field=field), self.assertRaises(DataQualityError):
                validate_rows("market", [{**market(), field: None}], [],
                              key_fields=("symbol", "snapshot_date"),
                              required_fields=(field,))

    def test_fng_duplicate_keys_fail(self):
        rows = fear_greed_rows({"data": [
            {"timestamp": str(MS // 1000), "value": "25", "value_classification": "Fear"}
        ] * 2})
        with self.assertRaisesRegex(DataQualityError, "duplicate"):
            validate_api_batch("fng", rows, [], through=DAY)

    def test_old_partial_day_is_not_exempt_from_rejection_budget(self):
        valid, rejected = split_valid_rejected([
            market(), {**market(), "snapshot_date": DAY - timedelta(days=1),
                       "source_ts_ms": MS - 86400000 + 1000}])
        with self.assertRaisesRegex(DataQualityError, "rejection rate"):
            validate_api_batch("market", valid, rejected, symbols=("BTCUSDT",), through=DAY)

    def test_identical_and_conflicting_business_keys(self):
        for price in (10.0, 20.0):
            with self.subTest(price=price), self.assertRaisesRegex(
                    DataQualityError, "duplicate or conflicting"):
                validate_api_batch("market", [market(), {**market(), "price_usd": price}],
                                   [], symbols=("BTCUSDT",), through=DAY)

    def test_duplicate_prices_not_erased_by_parser(self):
        rows = market_chart_rows({"prices": [[MS, 1.0], [MS, 2.0]]}, "BTCUSDT", "bitcoin")
        self.assertEqual(len(rows), 2)
        with self.assertRaisesRegex(DataQualityError, "duplicate"):
            validate_api_batch("market", rows, [], symbols=("BTCUSDT",), through=DAY)

    def test_duplicate_optional_points_are_rejected(self):
        rows = market_chart_rows({"prices": [[MS, 1.0]],
                                  "market_caps": [[MS, 2.0], [MS, 3.0]]},
                                 "BTCUSDT", "bitcoin")
        self.assertIn("duplicate", reject_reason(rows[0]))

    def test_invalid_timestamps_survive_for_quarantine(self):
        for value in ("bad", None, 10 ** 50):
            rows = market_chart_rows({"prices": [[value, 1.0]]}, "BTCUSDT", "bitcoin")
            self.assertEqual(reject_reason(rows[0]), "null_or_unparseable_date")
            fng = fear_greed_rows({"data": [{"timestamp": value, "value": "25"}]})
            self.assertEqual(reject_reason(fng[0]), "null_or_unparseable_date")

    def test_malformed_points_not_silently_dropped(self):
        for point in (None, [], [MS], "bad"):
            rows = market_chart_rows({"prices": [point]}, "BTCUSDT", "bitcoin")
            self.assertEqual(len(rows), 1)
            self.assertIsNotNone(reject_reason(rows[0]))

    def test_partial_day_is_quarantined_without_using_reject_budget(self):
        valid, rejected = split_valid_rejected([market(), {**market(), "source_ts_ms": MS + 1000}])
        self.assertEqual(len(rejected), 1)
        validate_api_batch("market", valid, rejected, symbols=("BTCUSDT",), through=DAY)

    def test_invalid_numeric_partial_point_is_not_exempt(self):
        row = {**market(), "source_ts_ms": MS + 1000, "price_usd": float("nan")}
        self.assertEqual(reject_reason(row), "non_finite_price_usd")

    def test_excessive_rejection_rate_and_boundary(self):
        validate_summary("x", rows_in=100, rows_valid=99, rows_rejected=1)
        with self.assertRaisesRegex(DataQualityError, "rejection rate"):
            validate_summary("x", rows_in=100, rows_valid=98, rows_rejected=2)
        with patch.dict(os.environ, {"DQ_MAX_REJECTION_RATE": "0.02"}):
            validate_summary("x", rows_in=100, rows_valid=98, rows_rejected=2)

    def test_invalid_configuration_fails_closed(self):
        for value in ("nan", "inf", "-1", "1.1", "bad"):
            with patch.dict(os.environ, {"DQ_MAX_REJECTION_RATE": value}):
                with self.assertRaises(ValueError):
                    max_rejection_rate()

    def test_missing_day_and_stale_end_date_fail(self):
        with self.assertRaisesRegex(DataQualityError, "missing expected dates"):
            validate_api_batch("market", [market()], [], symbols=("BTCUSDT",),
                               through=DAY + timedelta(days=1),
                               expected_dates=date_range(DAY, DAY + timedelta(days=1)))
        later = {**market(), "snapshot_date": DAY + timedelta(days=2),
                 "source_ts_ms": MS + 2 * 86400000}
        with self.assertRaisesRegex(DataQualityError, "missing expected dates"):
            validate_api_batch("market", [market(), later], [], symbols=("BTCUSDT",),
                               through=DAY + timedelta(days=2),
                               expected_dates=date_range(DAY, DAY + timedelta(days=2)))

    def test_delayed_ingestion_partial_point_still_quarantined(self):
        valid, rejected = split_valid_rejected([
            market(), {**market(), "source_ts_ms": MS + 1000}])
        validate_api_batch("market", valid, rejected, symbols=("BTCUSDT",),
                           through=DAY - timedelta(days=10))

    def test_api_envelopes(self):
        for payload in ({}, [], {"prices": []}, {"prices": "bad"}):
            with self.assertRaises(DataQualityError):
                validate_payload(payload, "prices", "market")
        validate_payload({"prices": [[MS, 1.0]]}, "prices", "market")

    def test_diagnostic_failure_does_not_mask_quality_error(self):
        writer = MagicMock(side_effect=OSError("offline"))
        error = DataQualityError("market", ["empty input"])
        record_failure(error, writer)
        writer.assert_called_once()

    def test_diagnostics_use_unique_keys(self):
        writer = MagicMock()
        error = DataQualityError("market", ["empty input"])
        record_failure(error, writer)
        record_failure(error, writer)
        self.assertNotEqual(writer.call_args_list[0].args[0], writer.call_args_list[1].args[0])


class PublicationTests(unittest.TestCase):
    """Exercise real job control flow with fake external boundaries."""

    def setUp(self):
        from src.transform import api_job
        self.job = api_job
        self.payloads = {
            "coingecko/market_chart/coin_id=bitcoin/2025-11-14.json": {
                "prices": [[MS, 1.0]], "market_caps": [], "total_volumes": []},
            "alternative_me/fear_greed/2025-11-14.json": {"data": [
                {"timestamp": str(MS // 1000), "value": "25", "value_classification": "Fear"}]},
        }
        self.s3 = MagicMock()
        self.s3.get_object.side_effect = lambda **kw: {
            "Body": io.BytesIO(json.dumps(self.payloads[kw["Key"]]).encode())}
        self.stored = {"silver": "previous good data", "staging": "previous good data"}
        self.spark = MagicMock()
        self.lake = MagicMock(side_effect=lambda *a, **k: self.stored.update(silver="new"))
        self.staging = MagicMock(side_effect=lambda *a, **k: self.stored.update(staging="new"))
        for p in (
            patch.dict(os.environ, {"DQ_MAX_REJECTION_RATE": "0.01"}),
            patch.object(api_job, "COINGECKO_IDS", {"BTCUSDT": "bitcoin"}),
            patch.object(api_job, "_s3", return_value=self.s3),
            patch.object(api_job, "get_spark", self.spark),
            patch.object(api_job, "write_lake", self.lake),
            patch.object(api_job, "_write_staging", self.staging),
            patch.object(api_job, "write_run_metrics"),
        ):
            p.start()
            self.addCleanup(p.stop)

    def assert_blocked(self):
        with self.assertRaises(DataQualityError):
            self.job.run("2025-11-14")
        self.lake.assert_not_called()
        self.staging.assert_not_called()
        self.spark.assert_not_called()
        self.assertEqual(self.stored, {"silver": "previous good data", "staging": "previous good data"})
        self.s3.put_object.assert_called_once()

    def test_empty_market_never_overwrites(self):
        self.payloads[next(iter(self.payloads))] = {}
        self.assert_blocked()

    def test_second_api_fails_before_first_api_overwrite(self):
        self.payloads["alternative_me/fear_greed/2025-11-14.json"] = {"data": []}
        self.assert_blocked()

    def test_nan_never_overwrites(self):
        self.payloads[next(iter(self.payloads))]["prices"] = [[MS, float("nan")]]
        self.assert_blocked()

    def test_missing_configured_symbol_never_overwrites(self):
        self.payloads["coingecko/market_chart/coin_id=ethereum/2025-11-14.json"] = {}
        with patch.object(self.job, "COINGECKO_IDS", {
                "BTCUSDT": "bitcoin", "ETHUSDT": "ethereum"}):
            self.assert_blocked()

    def test_bad_second_frame_schema_never_overwrites(self):
        # Schema verification is completed for both frames before publishing.
        self.spark.return_value.createDataFrame.side_effect = [MagicMock(), TypeError("schema")]
        with self.assertRaisesRegex(TypeError, "schema"):
            self.job.run("2025-11-14")
        self.lake.assert_not_called()
        self.staging.assert_not_called()

    def test_duplicates_never_overwrite(self):
        self.payloads[next(iter(self.payloads))]["prices"] *= 2
        self.assert_blocked()

    def test_bad_timestamp_never_overwrites(self):
        self.payloads[next(iter(self.payloads))]["prices"] = [["bad", 1.0]]
        self.assert_blocked()

    def test_valid_batch_publishes_both_datasets(self):
        self.job.run("2025-11-14")
        self.assertEqual(self.lake.call_count, 2)
        self.assertEqual(self.staging.call_count, 2)
        self.assertEqual(self.stored, {"silver": "new", "staging": "new"})

    def test_quarantine_write_failure_blocks_accepted_writes(self):
        self.payloads[next(iter(self.payloads))]["prices"].append([MS + 1000, 1.0])
        self.lake.side_effect = OSError("quarantine unavailable")
        with self.assertRaisesRegex(OSError, "quarantine unavailable"):
            self.job.run("2025-11-14")
        self.assertEqual(self.lake.call_args.args[1], self.job.QUARANTINE_COIN_MARKET)
        self.staging.assert_not_called()
        self.assertEqual(self.stored["silver"], "previous good data")

    def test_jdbc_failure_after_silver_write_is_not_transactional(self):
        # Documents a remaining infrastructure risk, not a quality-gate failure.
        self.staging.side_effect = OSError("JDBC unavailable")
        with self.assertRaisesRegex(OSError, "JDBC unavailable"):
            self.job.run("2025-11-14")
        self.assertEqual(self.stored["silver"], "new")
        self.assertEqual(self.stored["staging"], "previous good data")
        self.assertEqual(self.lake.call_count, 1)

    def test_nullable_api_aggregates_survive_frame_preparation(self):
        self.job.run("2025-11-14")
        row = self.spark.return_value.createDataFrame.call_args_list[0].args[0][0]
        self.assertIsNone(row["market_cap_usd"])
        self.assertIsNone(row["total_volume_usd"])

    def test_partial_historical_api_job_still_publishes(self):
        key = next(iter(self.payloads))
        self.payloads[key]["prices"] = [[MS - 86400000 * 10, 1.0]]
        self.job.run("2025-11-14")
        self.assertEqual(self.staging.call_count, 2)

    def test_failure_then_corrected_payload_recovers(self):
        key = next(iter(self.payloads))
        original = self.payloads[key]
        self.payloads[key] = {}
        self.assert_blocked()
        self.payloads[key] = original
        self.job.run("2025-11-14")
        self.assertEqual(self.stored, {"silver": "new", "staging": "new"})


class SilverPublicationTests(unittest.TestCase):
    def test_partial_historical_silver_without_coverage_contract_passes(self):
        from src.transform import silver_job
        frame = MagicMock()
        frame.select.return_value.distinct.return_value.collect.return_value = [
            {"trade_date": date(2026, 6, 15)}]
        with patch.object(silver_job, "write_lake") as writer, patch.object(silver_job.F, "col"):
            silver_job.publish_valid_trades(frame, "BTCUSDT", "2026-06", 1, 0, 1)
            writer.assert_called_once()

    def test_duplicate_in_rejected_rows_still_blocks_publication(self):
        from src.transform import silver_job
        frame = MagicMock()
        frame.select.return_value.distinct.return_value.collect.return_value = [
            {"trade_date": d} for d in date_range(date(2026, 6, 1), date(2026, 6, 30))]
        with patch.object(silver_job, "write_lake") as writer:
            with self.assertRaisesRegex(DataQualityError, "duplicate"):
                silver_job.publish_valid_trades(frame, "BTCUSDT", "2026-06",
                                                1000, 1, 999, n_duplicates=1)
            writer.assert_not_called()

    def test_failed_trade_contract_never_calls_lake_writer(self):
        from src.transform import silver_job
        full_month = date_range(date(2026, 6, 1), date(2026, 6, 30))
        cases = [(0, 0, 0, set()), (31, 0, 30, full_month),
                 (100, 2, 98, full_month), (29, 0, 29, full_month - {date(2026, 6, 1)})]
        for total, rejects, valid, dates in cases:
            frame = MagicMock()
            frame.select.return_value.distinct.return_value.collect.return_value = [
                {"trade_date": d} for d in dates]
            with self.subTest(total=total), patch.object(silver_job, "write_lake") as writer:
                with self.assertRaises(DataQualityError):
                    silver_job.publish_valid_trades(frame, "BTCUSDT", "2026-06",
                                                    total, rejects, valid,
                                                    expected_dates=full_month)
                writer.assert_not_called()

    def test_valid_month_publishes(self):
        from src.transform import silver_job
        frame = MagicMock()
        frame.select.return_value.distinct.return_value.collect.return_value = [
            {"trade_date": d} for d in date_range(date(2026, 6, 1), date(2026, 6, 30))]
        with patch.object(silver_job, "write_lake") as writer, patch.object(silver_job.F, "col"):
            silver_job.publish_valid_trades(frame, "BTCUSDT", "2026-06", 30, 0, 30)
            writer.assert_called_once()


if __name__ == "__main__":
    unittest.main()
