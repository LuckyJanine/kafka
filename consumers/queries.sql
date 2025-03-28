check_table_exists = """
SELECT EXISTS (
    SELECT FROM information_schema.tables
    WHERE table_name = %s
);
"""

create_msg_consumption_table = """
CREATE TABLE IF NOT EXISTS message_consumption (
    id SERIAL PRIMARY KEY,
    topic_name VARCHAR(50) NOT NULL,
    partition INT NOT NULL,
    offset INT,
    log_append_time TIMESTAMP,
    receiving_time TIMESTAMP,
    processed_time TIMESTAMP,
    consumer_group VARCHAR(100),
    consumer_group_offset INT,
    offset_lag INT,
    payload_size INT,
    status VARCHAR(50) DEFAULT 'pending'
);
"""