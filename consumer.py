from confluent_kafka import Consumer, KafkaException
import serverconfig

# conf = {'bootstrap.servers': 'host1:9092',
#         'client.id': 'test prod'}

def set_consumer_configs():
    serverconfig.conf['group.id'] = 'test-consumer-group1'
    # config['auto.offset.reset'] = 'earliest'
    # config['enable.auto.commit'] = False


def assignment_callback(consumer, partitions):
    for p in partitions:
        print(f'Assigned to {p.topic}, parition {p.partition}')


set_consumer_configs()
consumer = Consumer(serverconfig.conf)
consumer.subscribe(['quickstart-events'])
# consumer.subscribe(['quickstart-events'], on_assign=assignment_callback)

try:
    while True:
        event = consumer.poll(1.0)
        if event is None:
            continue
        if event.error():
            raise KafkaException(event.error())
        else:
            val = event.value().decode('utf8')
            partition = event.partition()
            print(f'Received: {val} from partition {partition}')
            consumer.commit(event)
except KeyboardInterrupt:
    print('Canceled by user.')
finally:
    consumer.close()
