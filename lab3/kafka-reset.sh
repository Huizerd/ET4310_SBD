rm -f ./GDELTProducer/segment/*
kafka-streams-application-reset.sh --application-id "lab3-gdelt-stream"
kafka-topics.sh --zookeeper localhost:2181 --delete --topic "gdelt.*"
