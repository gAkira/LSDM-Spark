echo "---------------- AVAILABLE DATA -----------------------"
gsutil ls gs://clusterdata-2011-2/

echo "---------------- DOWNLOADING SAMPLE -------------------"
mkdir -p ./data/job_events
mkdir -p ./data/machine_attributes
mkdir -p ./data/machine_events
mkdir -p ./data/task_constraints
mkdir -p ./data/task_events
mkdir -p ./data/task_usage

gsutil cp gs://clusterdata-2011-2/README ./data/

gsutil cp gs://clusterdata-2011-2/schema.csv ./data/

gsutil cp gs://clusterdata-2011-2/job_events/part-00000-of-00500.csv.gz ./data/job_events/
gsutil cp gs://clusterdata-2011-2/machine_attributes/part-00000-of-00001.csv.gz ./data/machine_attributes/
gsutil cp gs://clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz ./data/machine_events/
gsutil cp gs://clusterdata-2011-2/task_constraints/part-00000-of-00500.csv.gz ./data/task_constraints/
gsutil cp gs://clusterdata-2011-2/task_events/part-00000-of-00500.csv.gz ./data/task_events/
gsutil cp gs://clusterdata-2011-2/task_usage/part-00000-of-00500.csv.gz ./data/task_usage/

echo "---------------- UNPACKING DATA -----------------------"
gunzip ./data/job_events/part-00000-of-00500.csv.gz
gunzip ./data/machine_attributes/part-00000-of-00001.csv.gz
gunzip ./data/machine_events/part-00000-of-00001.csv.gz
gunzip ./data/task_constraints/part-00000-of-00500.csv.gz
gunzip ./data/task_events/part-00000-of-00500.csv.gz
gunzip ./data/task_usage/part-00000-of-00500.csv.gz

echo "Done."
