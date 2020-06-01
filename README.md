#Поднимаем zookeeper и kafka
Инструкция - https://towardsdatascience.com/running-zookeeper-kafka-on-windows-10-14fc70dcc771
1) zkserver
2) kafka-server-start.bat C:\Apache\kafka_2.12–2.3.1\config\server.properties

#создание топика
 kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test
#список топиков
 kafka-topics.bat --list --bootstrap-server localhost:9092

#producer и consumer
3) kafka-console-producer.bat --topic test --broker-list localhost:9092
4) kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test_out --from-beginning

#приложение
5) .jar