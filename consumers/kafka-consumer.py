import threading
import random
from datetime import datetime
import time
import signal

from confluent_kafka import Consumer, KafkaException, KafkaError

# An threading.Event object manages an internal flag that can be set to true with the set() method
# and reset to false with the clear() method
terminating = threading.Event()

consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'auto.offset.reset': 'earliest'
    }

def interrupt_signal_handler(sig, frame):
    terminating.set()
    print("\nStopping consumers...")
    for thread in threading.enumerate():
        if thread is not threading.main_thread():
            thread.join()
    print("\n========\nAll worker threads stopped.")

def create_consumer(group):
    consumer_config['group.id'] = group
    consumer = Consumer(consumer_config)
    if group == 'atc_group':
        consumer.subscribe(['flight-atc'])
    elif group == 'engine_log_group':
        consumer.subscribe(['engine-logs'])
    elif group == 'hydraulic_log_group':
        consumer.subscribe(['hydraulic-logs'])
    return consumer

def consume_messages(consumer, consumer_id, group, interrupt_e):
    current_thread = threading.current_thread().name
    try:
        while not interrupt_e.is_set():
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                """ if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f"End of partition reached at offset {msg.offset}.")
                else:
                    raise KafkaException(msg.error()) """
                pass
            else:
                # if msg.key() != b'important':
                #    logging.info("Consumer 3 - Skipped message without key 'important'.")
                #    continue
                base_delay = 0
                random_offset = 0
                if group == 'atc_group':
                    base_delay = 20 # 20 seconds
                    random_offset = random.uniform(-0.01, 0.01)
                elif group == 'engine_log_group':
                    base_delay = 60
                    random_offset = random.uniform(-0.05, 0.05)
                elif group == 'hydraulic_log_group':
                    base_delay = 120
                    random_offset = random.uniform(-0.1, 0.1)
                # logging.info(f"{group} Consumer {consumer_id} - Received a message at {datetime.utcnow()}")
                print(f"{group} Consumer {consumer_id} - Received a message at {datetime.utcnow()}")
                time.sleep(base_delay + random_offset)
    finally:
        consumer.close()
        print(f"{current_thread} stopped for group {group}")

def consumer_startup(groups):
    threads = []
    for group, num_consumers in consumer_groups.items():
        for i in range(num_consumers):
            consumer = create_consumer(group)
            thread = threading.Thread(target=consume_messages, args=(consumer, i, group, terminating))
            thread.daemon = True
            threads.append(thread)
    return threads

def main():
    consumer_groups = {
        'atc_group': 4,
        'engine_log_group': 3,
        'hydraulic_log_group': 2
    }

    signal.signal(signal.SIGINT, interrupt_signal_handler) # keyboardinterrupt Exception
    signal.signal(signal.SIGTERM, interrupt_signal_handler)

    print("consumer threads starting up. Press ctrl+c in case for termination.")
    threads = consumer_startup(consumer_groups)
    for thread in threads:
        thread.start()

    while not terminating.is_set():
        time.sleep(2)

if __name__ == '__main__':
    print("Hello, World!")
    main()

