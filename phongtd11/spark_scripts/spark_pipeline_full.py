"""
FULL PIPELINE ORCHESTRATOR - PySpark Version
Mục đích: Orchestrate the complete ETL pipeline from Raw -> Processed -> Analytics layers
Executes all three Spark layers in sequence with proper data flow
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# Add current directory to path to import modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the three layer processors
from spark_01_raw_layer import RawLayerProcessorSpark
from spark_02_processed_layer import ProcessedLayerProcessorSpark
from spark_03_analytics_layer import AnalyticsLayerProcessorSpark


def run_full_pipeline(kafka_topic, raw_output_dir="data_spark/raw", 
                     processed_output_dir="data_spark/processed", 
                     analytics_output_dir="data_spark/analytics"):
    """
    Execute the complete Spark ETL pipeline:
    1. Raw Layer: Ingest and store raw data
    2. Processed Layer: Clean, normalize, and enrich data
    3. Analytics Layer: Create aggregated tables for visualization
    """
    
    print("="*80)
    print("🚀 STARTING FULL SPARK PIPELINE EXECUTION")
    print("="*80)
    print(f"Kafka topic: {kafka_topic}")
    print(f"Raw output: {raw_output_dir}")
    print(f"Processed output: {processed_output_dir}")
    print(f"Analytics output: {analytics_output_dir}")
    print("="*80)
    
    start_time = datetime.now()
    
    # Track results from each layer
    raw_result = None
    processed_result = None
    analytics_result = None
    
    try:
        # ========================================
        # LAYER 1: RAW LAYER PROCESSING
        # ========================================
        print("\n" + "="*60)
        print("📦 LAYER 1: RAW LAYER PROCESSING (BRONZE)")
        print("="*60)
        
        raw_processor = RawLayerProcessorSpark(
            kafka_topic=kafka_topic,
            output_dir=raw_output_dir
        )
        
        raw_result, raw_manifest = raw_processor.process()
        raw_processor.stop()
        
        print(f"✅ Raw Layer Complete - {raw_manifest['total_records']} records processed")
        
        # ========================================
        # LAYER 2: PROCESSED LAYER PROCESSING  
        # ========================================
        print("\n" + "="*60)
        print("🧹 LAYER 2: PROCESSED LAYER PROCESSING (SILVER)")
        print("="*60)
        
        processed_processor = ProcessedLayerProcessorSpark(
            raw_data_path=raw_output_dir,
            output_dir=processed_output_dir
        )
        
        processed_result = processed_processor.process()
        processed_processor.stop()
        
        processed_count = processed_result.count()
        print(f"✅ Processed Layer Complete - {processed_count} records processed")
        
        # ========================================
        # LAYER 3: ANALYTICS LAYER PROCESSING
        # ========================================
        print("\n" + "="*60)
        print("📊 LAYER 3: ANALYTICS LAYER PROCESSING (GOLD)")
        print("="*60)
        
        analytics_processor = AnalyticsLayerProcessorSpark(
            processed_data_path=processed_output_dir,
            output_dir=analytics_output_dir
        )
        
        # Execute all analytics tables
        print("Generating analytics tables...")

        # Create all analytics tables
        daily_stats = analytics_processor.create_daily_subreddit_stats()
        trending_topics = analytics_processor.create_trending_topics()
        user_engagement = analytics_processor.create_user_engagement()
        cross_subreddit = analytics_processor.create_cross_subreddit_comparison()
        hourly_patterns = analytics_processor.create_hourly_patterns()
        entity_network = analytics_processor.create_entity_network()
        sentiment_timeseries = analytics_processor.create_sentiment_timeseries()

        # Generate insights
        # Load the processed data again for insights generation
        processed_df = analytics_processor.load_processed_data()
        processed_df.cache()  # Cache for performance
        insights = analytics_processor.generate_insights(processed_df)
        processed_df.unpersist()
        
        analytics_processor.stop()
        
        print("✅ Analytics Layer Complete - All tables generated")
        
        # ========================================
        # PIPELINE SUMMARY
        # ========================================
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "="*80)
        print("🏁 FULL PIPELINE EXECUTION COMPLETE")
        print("="*80)
        print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {duration}")
        print("-"*80)
        print(f"📊 Raw Layer: {raw_manifest['total_records']} input records")
        print(f"🧹 Processed Layer: {processed_count} processed records")
        print(f"📈 Analytics Layer: Multiple aggregated tables created")
        print("-"*80)
        print(f"📁 Raw data saved to: {raw_output_dir}")
        print(f"📁 Processed data saved to: {processed_output_dir}")
        print(f"📁 Analytics data saved to: {analytics_output_dir}/[table_names]")
        print("="*80)
        
        # Return results for potential further processing
        return {
            'raw_result': raw_result,
            'processed_result': processed_result,
            'raw_manifest': raw_manifest
        }
        
    except Exception as e:
        print(f"❌ Pipeline failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e


def main():
    """Main entry point for the pipeline"""
    
    # # Default input file - this should be adjusted based on your actual data
    # default_input_file = "/mnt/user-data/uploads/reddit_posts.jsonl"
    
    # # Check if input file exists, otherwise prompt user
    # if len(sys.argv) > 1:
    #     input_file = sys.argv[1]
    # else:
    #     print(f"⚠️  No input file specified as argument.")
    #     print(f"ℹ️   Using default: {default_input_file}")
    #     print("💡 To specify input file: python spark_pipeline_full.py /path/to/input.jsonl")
    #     input_file = default_input_file
    
    # # Validate input file exists
    # if not Path(input_file).exists():
    #     print(f"❌ Input file does not exist: {input_file}")
    #     print("💡 Please provide a valid input JSONL file path")
    #     sys.exit(1)
    
    # Create output directories if they don't exist
    Path("data_spark/raw").mkdir(parents=True, exist_ok=True)
    Path("data_spark/processed").mkdir(parents=True, exist_ok=True)
    Path("data_spark/analytics").mkdir(parents=True, exist_ok=True)
    
    kafka_topic = "reddit_posts"
    # Run the full pipeline
    try:
        results = run_full_pipeline(kafka_topic=kafka_topic)
        print("\n🎉 Pipeline completed successfully!")
        return results
    except Exception as e:
        print(f"\n💥 Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()