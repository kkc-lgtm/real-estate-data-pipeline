"""Business quality rules for comparable individual-property asking prices."""
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def classify_listings(frame, individual_path='/ro/oferta/'):
    """Annotate all reasons; quarantine every duplicate key rather than pick a winner."""
    duplicates = F.count('*').over(Window.partitionBy('listing_id'))
    reasons = [
        F.when(F.col('url').isNull() | ~F.col('url').contains(individual_path), 'outside_individual_offer_scope'),
        F.when(F.col('listing_id').isNull() | (F.trim('listing_id') == '') | (F.col('listing_id') == 'N/A'), 'missing_listing_id'),
        F.when(duplicates > 1, 'duplicate_listing_id'),
        F.when(F.col('Price_EUR').isNull() | (F.col('Price_EUR') <= 0), 'invalid_total_price'),
        F.when(F.col('Area_sqm').isNull() | (F.col('Area_sqm') <= 0) | F.isnan('Area_sqm'), 'invalid_area'),
        F.when(F.col('Price_per_sqm').isNull() | F.isnan('Price_per_sqm') |
               (F.abs(F.col('Price_per_sqm') - F.round(F.col('Price_EUR') / F.col('Area_sqm'), 2)) > 0.01), 'invalid_price_per_sqm'),
    ]
    return frame.withColumn('rejection_reason', F.concat_ws(';', *reasons))
