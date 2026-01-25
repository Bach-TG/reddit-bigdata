# Step 1:

docker build -f Dockerfile.airflow -t airflow_dp:test .

# Step 2:

docker build -f Dockerfile.spark -t phongtd11-spark-image .

# Step 3:

chmod +x entrypoint.sh
chmod +x start-with-topics.sh

# Step 4:

docker-compose up -d