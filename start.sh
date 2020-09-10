sbt package
#/home/hadoop/spark/bin/spark-submit --class "test.Test" --master spark://192.168.1.74:7077 --executor-memory 10g /home/hadoop/code/scala/target/scala-2.11/test-project_2.11-2.4.5.jar
sudo /home/hadoop/spark/bin/spark-submit --class "src.main.scala.method.BaseZorder" --master local[*] /home/hadoop/code/scala/target/scala-2.11/test-project_2.11-2.4.5.jar
