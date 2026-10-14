"""Execute real-estate analytics with configurable scope and quality requirements."""
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
    parser.add_argument('--min-listings', type=int, default=2, help='Minimum comparable offers per neighborhood')
    parser.add_argument('--individual-path', default='/ro/oferta/', help='URL path identifying individual-property offers')
    args = parser.parse_args()
    if args.min_listings < 1:
        parser.error('--min-listings must be at least 1')
    if not args.individual_path.startswith('/') or len(args.individual_path.strip('/')) < 3:
        parser.error('--individual-path must be a specific URL path')
    repo = args.upstream.resolve()
    actual = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip()
    if actual != COMMIT and subprocess.run(['git', '-C', str(repo), 'merge-base', '--is-ancestor', COMMIT, actual]).returncode:
        raise RuntimeError(f'Checkout must descend from pinned upstream {COMMIT}')
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
    quality_module = importlib.import_module('portfolio_quality')
    output = args.results.resolve()
    output.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.master('local[2]').appName('RealEstate-Quality-Analytics')
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
        classified = quality_module.classify_listings(silver, args.individual_path).cache()
        rejected = classified.filter(F.col('rejection_reason') != '')
        pd.DataFrame([r.asDict() for r in rejected.collect()], columns=classified.columns).to_csv(output / 'quarantine.csv', index=False)
        issue_counts = rejected.select(F.explode(F.split('rejection_reason', ';')).alias('reason')).groupBy('reason').count()
        pd.DataFrame([r.asDict() for r in issue_counts.collect()], columns=['reason', 'count']).to_csv(output / 'quality_summary.csv', index=False)
        silver = classified.filter(F.col('rejection_reason') == '').drop('rejection_reason').cache()
        curated_count = silver.count()
        if curated_count == 0:
            raise RuntimeError('No comparable offers remain after quality checks; see quarantine.csv')
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
        neighborhoods = save(gold_module.build_by_neighborhood(silver, min_listings=args.min_listings), 'by_neighborhood')
        rooms = save(gold_module.build_rooms_distribution(silver), 'rooms_distribution')
        # Cross-check author aggregation against independent SQL.
        independent = spark.sql('SELECT count(listing_id) AS n, round(avg(Price_EUR), 0) AS avg_price FROM silver_listings').first()
        assert summary[0]['Total_Anunturi'] == independent['n'] == curated_count
        assert summary[0]['Pret_Mediu_EUR'] == independent['avg_price']
        assert quality['ratio_mismatches'] == 0
        report = {'upstream_url': UPSTREAM.removesuffix('.git'), 'upstream_commit': actual,
                  'code_sha256': {p: hashlib.sha256((repo / p).read_bytes()).hexdigest() for p in
                                  ['src/pipeline/01_Bronze_to_Silver.py', 'src/pipeline/02_Silver_to_Gold.py', 'src/pipeline/portfolio_quality.py']},
                  'source_files_modified_before_commit': bool(subprocess.check_output(['git', '-C', str(repo), 'status', '--porcelain', '--', 'src/pipeline'], text=True).strip()),
                  'input_sha256': hashlib.sha256(raw.read_bytes()).hexdigest(),
                  'data_origin': 'Live upstream scraper output from storia.ro; asking prices, not completed transactions',
                  'runtime': {'python': sys.version.split()[0], 'spark': spark.version},
                  'bronze_rows': bronze.count(), 'silver_rows': count, 'quality': quality,
                  'curated_rows': curated_count, 'quarantined_rows': count - curated_count,
                  'requirements': {'min_listings': args.min_listings, 'individual_path': args.individual_path},
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
