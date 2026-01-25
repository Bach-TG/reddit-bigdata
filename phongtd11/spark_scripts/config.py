# Database configuration for PostgreSQL
DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'reddit_db',
    'username': 'reddit_user',
    'password': 'reddit_pass'
}

KAFKA_CONFIG = {
    'bootstrap_servers': 'kafka:9092',
    'security_protocol': 'PLAINTEXT',
    'sasl_mechanism': 'PLAIN',
    'username': 'kafka_local',
    'password': 'kafka_local_password',
    'topic': 'reddit_posts'
}