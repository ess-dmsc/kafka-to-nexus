import json
import argparse
from kafka import KafkaProducer

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Send commands to NeXus file writer via Kafka")
    parser.add_argument("-b","--broker",
                        action="store",
                        nargs='?',
                        type=str,
                        default="localhost:9092",
                        help="Kafka broker:port")
    parser.add_argument("-t","--topic", 
                        action="store",
                        nargs='?',
                        default="command",
                        type=str,
                        help="topic where commands are given")
    parser.add_argument("-f","--file", 
                        action="store",
                        nargs='?',
                        default="command.txt",
                        type=str,
                        help="json file containing commands")

    args = parser.parse_args()

    with open(args.file) as json_data:
        cmd = json.load(json_data)

        producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers=args.broker)
        producer.send(args.topic, cmd)
