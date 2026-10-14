"""Acceptance checks for comparable-price scope and neighborhood sample size."""
from test_upstream_windows import spark
from portfolio_quality import classify_listings
import importlib
import pytest

gold = importlib.import_module('02_Silver_to_Gold')


def test_scope_and_invalid_measurements_are_quarantined(spark):
    raw = spark.sql("""SELECT * FROM VALUES
      ('good','https://example.org/ro/oferta/a',100000,50.0,2000.0),
      ('ad','https://example.org/ro/ansamblu/a',1800,50.0,36.0),
      ('bad','https://example.org/ro/oferta/b',100000,0.0,CAST(NULL AS DOUBLE)),
      ('missing',CAST(NULL AS STRING),100000,50.0,2000.0)
      AS t(listing_id,url,Price_EUR,Area_sqm,Price_per_sqm)""")
    reasons = {r['listing_id']:r['rejection_reason'] for r in classify_listings(raw).collect()}
    assert reasons['good'] == ''
    assert reasons['ad'] == 'outside_individual_offer_scope'
    assert 'invalid_area' in reasons['bad']
    assert 'invalid_price_per_sqm' in reasons['bad']
    assert reasons['missing'] == 'outside_individual_offer_scope'


def test_duplicate_ids_are_all_rejected(spark):
    raw = spark.sql("""SELECT * FROM VALUES
      ('duplicate','https://example.org/ro/oferta/a',100000,50.0,2000.0),
      ('duplicate','https://example.org/ro/oferta/b',120000,60.0,2000.0)
      AS t(listing_id,url,Price_EUR,Area_sqm,Price_per_sqm)""")
    assert [r['rejection_reason'] for r in classify_listings(raw).collect()] == ['duplicate_listing_id']*2


def test_minimum_sample_size_changes_report_coverage(spark):
    raw = spark.sql("""SELECT * FROM VALUES
      ('a1','Sector 1','A',100000,2000.0),
      ('a2','Sector 1','A',110000,2200.0),
      ('b1','Sector 2','B',120000,2400.0)
      AS t(listing_id,City_Sector,Neighborhood,Price_EUR,Price_per_sqm)""")
    assert gold.build_by_neighborhood(raw).count() == 1
    assert gold.build_by_neighborhood(raw, min_listings=1).count() == 2
    assert gold.build_by_neighborhood(raw, min_listings=3).count() == 0


def test_invalid_minimum_sample_size_is_rejected():
    for value in [0, -1, 1.5, True]:
        with pytest.raises(ValueError):
            gold.build_by_neighborhood(None, min_listings=value)
