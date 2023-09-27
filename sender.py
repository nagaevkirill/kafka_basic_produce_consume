from confluent_kafka import Producer
import serverconfig


def callback(err, event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        val = event.value().decode('utf8')
        print(f'{val} sent to partition {event.partition()}.')


def say_hello(producer, key):
    value = f'Hello {key}!'
    producer.produce('quickstart-events', value, key, on_delivery=callback)


producer = Producer(serverconfig.conf)
keys = ['Amy', 'Brenda', 'Cindy', 'Derrick', 'Elaine', 'Fred']
[say_hello(producer, key) for key in keys]
producer.flush()
