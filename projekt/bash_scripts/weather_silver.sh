file_py="/home/vagrant/projekt/transformations/weather_silver.py"
export PATH=$PATH:/usr/local/spark/bin
spark-submit --master local[2] "$file_py"
