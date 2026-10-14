"""Run the upstream transformation functions; keep upstream ownership explicit."""
import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys

UPSTREAM = 'https://github.com/Bahna-Darius/real-estate-data-pipeline.git'
COMMIT = 'a7ab704445b3da4218b4fd9371c1359430cda433'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--upstream', type=Path, required=True)
    parser.add_argument('--results', type=Path, default=Path(__file__).parent / 'results')
    parser.add_argument('--scrape-pages', type=int, default=0)
    args = parser.parse_args()
    repo = args.upstream.resolve()
    actual = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip()
    if actual != COMMIT:
        raise RuntimeError(f'Expected pinned upstream commit {COMMIT}, got {actual}')
    sys.path[:0] = [str(repo / 'src'), str(repo / 'src/pipeline'), str(repo / 'src/ingestion')]
    if args.scrape_pages:
        if not 1 <= args.scrape_pages <= 2:
            raise ValueError('Reproduction restricts scraping to 1-2 pages')
        scraper = importlib.import_module('imobiliare_scraper')
        scraper.NUM_PAGES_TO_SCRAPE = args.scrape_pages
        scraper.main()
    raw = repo / 'data/raw/storia_raw_data.json'
    if not raw.exists():
        raise RuntimeError('No raw data available. Run with --scrape-pages 2 first.')
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    import pandas as pd
    silver_module = importlib.import_module('01_Bronze_to_Silver')
    gold_module = importlib.import_module('02_Silver_to_Gold')
    output = args.results.resolve()
    output.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.master('local[2]').appName('HMG-upstream-reproduction')
             .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
             .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        # Read exactly the JSON file: upstream directory also contains CSV files.
        bronze = spark.read.json(str(raw), multiLine=True)
        silver = silver_module.transform_bronze_to_silver(bronze).cache()
        count = silver.count()
        if count == 0:
            raise RuntimeError('Empty input is not a successful reproduction')
        silver.createOrReplaceTempView('silver_listings')
        # Independent SQL checks, supplementing the upstream's null report.
        quality = spark.sql('''SELECT count(*) AS total_rows,
          count(DISTINCT listing_id) AS unique_ids,
          sum(CASE WHEN Price_EUR IS NULL THEN 1 ELSE 0 END) AS missing_price,
          sum(CASE WHEN Area_sqm IS NULL OR Area_sqm <= 0 THEN 1 ELSE 0 END) AS invalid_area,
          sum(CASE WHEN Price_EUR <= 0 THEN 1 ELSE 0 END) AS nonpositive_price,
          sum(CASE WHEN rooms IS NULL THEN 1 ELSE 0 END) AS missing_rooms,
          sum(CASE WHEN Neighborhood IS NULL OR Neighborhood = 'N/A' THEN 1 ELSE 0 END) AS missing_neighborhood,
          sum(CASE WHEN Price_EUR > 0 AND Area_sqm > 0 AND
              abs(Price_per_sqm - round(Price_EUR / Area_sqm, 2)) > 0.01 THEN 1 ELSE 0 END) AS ratio_mismatches
          FROM silver_listings''').first().asDict()
        # Development advertisements quote EUR/m2, not a unit's total asking price.
        # Do not mix /ansamblu/ advertisements with individual /oferta/ listings.
        upstream_summary = gold_module.build_market_summary(silver).first().asDict()
        rejected = silver.filter(~F.col('url').contains('/ro/oferta/'))
        pd.DataFrame([r.asDict() for r in rejected.collect()]).to_csv(output / 'quarantine.csv', index=False)
        silver = silver.filter(F.col('url').contains('/ro/oferta/')).cache()
        curated_count = silver.count()
        silver.createOrReplaceTempView('silver_listings')
        # Gateway collect avoids Hadoop's Windows native Parquet writer requirements.
        def save(frame, name):
            records = [r.asDict() for r in frame.collect()]
            pdf = pd.DataFrame(records, columns=frame.columns)
            target = output / name
            target.mkdir(exist_ok=True)
            pdf.to_parquet(target / 'part-local.parquet', index=False)
            csv_target = repo / 'data/gold' / (name + '_csv')
            csv_target.mkdir(parents=True, exist_ok=True)
            pdf.to_csv(csv_target / 'part-local.csv', index=False)
            pdf.to_csv(target / 'part-local.csv', index=False)
            return records
        save(silver, 'silver')
        summary = save(gold_module.build_market_summary(silver), 'market_summary')
        neighborhoods = save(gold_module.build_by_neighborhood(silver), 'by_neighborhood')
        rooms = save(gold_module.build_rooms_distribution(silver), 'rooms_distribution')
        # Cross-check author aggregation against independent SQL.
        independent = spark.sql('SELECT count(listing_id) AS n, round(avg(Price_EUR), 0) AS avg_price FROM silver_listings').first()
        assert summary[0]['Total_Anunturi'] == independent['n'] == curated_count
        assert summary[0]['Pret_Mediu_EUR'] == independent['avg_price']
        assert quality['ratio_mismatches'] == 0
        report = {'upstream_url': UPSTREAM.removesuffix('.git'), 'upstream_commit': actual,
                  'input_sha256': hashlib.sha256(raw.read_bytes()).hexdigest(),
                  'data_origin': 'Live upstream scraper output from storia.ro; asking prices, not completed transactions',
                  'runtime': {'python': sys.version.split()[0], 'spark': spark.version},
                  'bronze_rows': bronze.count(), 'silver_rows': count, 'quality': quality,
                  'curated_rows': curated_count, 'quarantined_rows': count - curated_count,
                  'quarantine_rule': 'Only /ro/oferta/ individual listings; exclude /ro/ansamblu/ development advertisements',
                  'upstream_unfiltered_market_summary': upstream_summary,
                  'market_summary': summary, 'neighborhood_groups': len(neighborhoods),
                  'room_groups': len(rooms), 'independent_sql_reconciliation': 'passed',
                  'storage_adapter': 'Original PySpark transform/aggregation functions; pandas/pyarrow writes local CSV/Parquet',
                  'not_executed': ['Fabric', 'OneLake', 'Azure', 'Databricks', 'Airflow', 'Docker', 'Power BI']}
        (output / 'run_report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print(json.dumps(report, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
