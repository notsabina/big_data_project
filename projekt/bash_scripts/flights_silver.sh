file_py="/home/vagrant/projekt/transformations/flights_silver.py"
export PATH=$PATH:/usr/local/spark/bin
spark-submit --master local[2] "$file_py"
