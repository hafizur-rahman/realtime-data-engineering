## Miniconda
```
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm ~/miniconda3/miniconda.sh
```

```
source ~/miniconda3/bin/activate
```

```
conda init --all
```

## Virtualenv
```
conda create -n data
```

```
conda activate data
```

```
conda install simplejson pyspark
```

```
pip install confluent-kafka
```

## Generate streaming data
```
python jobs/main.py
```

## Submit spark job
```
sudo docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:aws-sdk-java:2.32.30,software.amazon.awssdk:auth:2.32.30 jobs/spark-city.py
```