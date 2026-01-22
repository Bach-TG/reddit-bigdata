"""
MAIN PYSPARK PIPELINE - Reddit Big Data Processing
Orchestrate toàn bộ Spark pipeline: Raw → Processed → Analytics
Production-ready, scalable to millions of records
"""

import sys
import time
from datetime import datetime
from pathlib import Path


class SparkPipeline:
    def __init__(self, input_file):
        self.input_file = input_file
        self.start_time = datetime.now()
        
    def run(self):
        """Execute full Spark pipeline"""
        print("="*80)
        print("🚀 REDDIT BIG DATA PROCESSING - PYSPARK PIPELINE")
        print("="*80)
        print(f"📅 Started at: {self.start_time}")
        print(f"📁 Input file: {self.input_file}")
        print(f"⚡ Framework: Apache Spark (Distributed Processing)")
        print("="*80)
        
        results = {}
        
        try:
            # ================================================================
            # LAYER 1: RAW DATA INGESTION
            # ================================================================
            print("\n\n")
            print("█" * 80)
            print("█  LAYER 1: RAW DATA INGESTION (BRONZE) - PYSPARK                              █")
            print("█" * 80)
            
            from bai_tap_lon.phongtd11.spark_scripts.spark_01_raw_layer import RawLayerProcessorSpark
            
            raw_processor = RawLayerProcessorSpark(
                input_file=self.input_file,
                output_dir="data_spark/raw"
            )
            
            df_raw, manifest_raw = raw_processor.process()
            raw_processor.stop()
            
            print(f"\n✅ RAW LAYER COMPLETE (PySpark)")
            print(f"   📊 Records: {manifest_raw['total_records']}")
            print(f"   📦 Output: data_spark/raw/")
            
            results['raw_count'] = manifest_raw['total_records']
            results['raw_manifest'] = manifest_raw
            
            # ================================================================
            # LAYER 2: PROCESSED DATA
            # ================================================================
            print("\n\n")
            print("█" * 80)
            print("█  LAYER 2: PROCESSED DATA (SILVER) - PYSPARK                                  █")
            print("█" * 80)
            
            from bai_tap_lon.phongtd11.spark_scripts.spark_02_processed_layer import ProcessedLayerProcessorSpark
            
            processed_processor = ProcessedLayerProcessorSpark(
                raw_data_path="data_spark/raw",
                output_dir="data_spark/processed"
            )
            
            df_processed = processed_processor.process()
            processed_count = df_processed.count()
            processed_processor.stop()
            
            print(f"\n✅ PROCESSED LAYER COMPLETE (PySpark)")
            print(f"   📊 Records: {processed_count}")
            print(f"   📦 Output: data_spark/processed/")
            
            results['processed_count'] = processed_count
            
            # ================================================================
            # LAYER 3: ANALYTICS
            # ================================================================
            print("\n\n")
            print("█" * 80)
            print("█  LAYER 3: ANALYTICS (GOLD) - PYSPARK SQL                                     █")
            print("█" * 80)
            
            from bai_tap_lon.phongtd11.spark_scripts.spark_03_analytics_layer import AnalyticsLayerProcessorSpark
            
            analytics_processor = AnalyticsLayerProcessorSpark(
                processed_data_path="data_spark/processed",
                output_dir="data_spark/analytics"
            )
            
            tables, insights = analytics_processor.process()
            analytics_processor.stop()
            
            print(f"\n✅ ANALYTICS LAYER COMPLETE (PySpark SQL)")
            print(f"   📊 Tables: {len(tables)}")
            print(f"   💡 Insights: {len(insights['key_insights'])}")
            print(f"   📦 Output: data_spark/analytics/")
            
            results['tables_count'] = len(tables)
            results['insights'] = insights
            
            # ================================================================
            # FINAL SUMMARY
            # ================================================================
            end_time = datetime.now()
            duration = (end_time - self.start_time).total_seconds()
            
            print("\n\n")
            print("="*80)
            print("🎉 PYSPARK PIPELINE EXECUTION COMPLETE!")
            print("="*80)
            print(f"⏱️  Total Duration: {duration:.2f} seconds")
            print(f"📊 Total Records Processed: {results['processed_count']:,}")
            print(f"⚡ Processing Speed: {results['processed_count']/duration:.0f} records/second")
            print(f"\n🗂️  Data Layers Created:")
            print(f"   ✓ Raw Layer (Bronze) - Partitioned Parquet")
            print(f"   ✓ Processed Layer (Silver) - NLP Enriched")
            print(f"   ✓ Analytics Layer (Gold) - 7 Analytical Tables")
            
            print(f"\n📁 PySpark Output Structure:")
            print(f"   data_spark/")
            print(f"   ├── raw/                         # Partitioned by subreddit/year/month/day")
            print(f"   ├── processed/                   # Partitioned by subreddit")
            print(f"   └── analytics/                   # 7 analytical tables")
            print(f"       ├── daily_subreddit_stats/")
            print(f"       ├── trending_topics/")
            print(f"       ├── user_engagement/")
            print(f"       ├── cross_subreddit_comparison/")
            print(f"       ├── hourly_patterns/")
            print(f"       ├── entity_network/")
            print(f"       ├── sentiment_timeseries/")
            print(f"       └── insights.json")
            
            print(f"\n🚀 Scalability:")
            print(f"   • Current: {results['processed_count']} posts in {duration:.1f}s")
            print(f"   • Spark can scale to: Millions of posts")
            print(f"   • Distributed: Across multiple nodes/cores")
            print(f"   • Production: Deploy on EMR, Databricks, Dataproc")
            
            print(f"\n💡 Key Insights Generated:")
            for i, insight in enumerate(insights['key_insights'], 1):
                print(f"   {i}. {insight['title']}")
            
            print("="*80)
            
            results['duration'] = duration
            results['throughput'] = results['processed_count'] / duration
            
            return results
            
        except Exception as e:
            print(f"\n❌ ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

# ============================================================================
# EXECUTION
# ============================================================================
if __name__ == "__main__":

    print("\n" + "="*80)
    print("Starting PySpark Pipeline...")
    print("="*80 + "\n")
    
    # Run pipeline
    pipeline = SparkPipeline(
        input_file="/mnt/user-data/uploads/reddit_posts.jsonl"
    )
    
    results = pipeline.run()
    
    if results:
        print("\n\n💾 RESULTS SUMMARY:")
        print(f"   • Raw records: {results['raw_count']:,}")
        print(f"   • Processed records: {results['processed_count']:,}")
        print(f"   • Analytical tables: {results['tables_count']}")
        print(f"   • Processing time: {results['duration']:.2f}s")
        print(f"   • Throughput: {results['throughput']:.0f} records/sec")
        print(f"\n✅ All outputs saved to: data_spark/")
