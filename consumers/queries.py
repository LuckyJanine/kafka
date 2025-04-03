check_table_exists = """
SELECT EXISTS (
    SELECT FROM information_schema.tables
    WHERE table_name = %s
);
"""

create_msg_consumption_table = """
CREATE TABLE IF NOT EXISTS message_consumption (
    message_id UUID PRIMARY KEY,
    topic_name VARCHAR(50) NOT NULL,
    partition INT NOT NULL,
    "offset" INT,
    log_append_time TIMESTAMP,
    receiving_time TIMESTAMP,
    processed_time TIMESTAMP,
    consumer_id INT,
    consumer_group VARCHAR(100),
    group_latest_offset INT,
    group_committed_offset INT,
    offset_lag INT,
    payload_size INT,
    status VARCHAR(50) DEFAULT 'pending'
);
"""

insert_msg_consumption_table = """
INSERT INTO message_consumption (
    message_id,
    topic_name,
    partition,
    "offset",
    log_append_time,
    receiving_time,
    processed_time,
    consumer_id,
    consumer_group,
    group_latest_offset,
    group_committed_offset,
    offset_lag,
    payload_size
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING message_id;
"""

update_msg_consumption_table = """
UPDATE message_consumption
SET processed_time = NOW(), status = %s
WHERE message_id = %s
"""

data_load_query = """
SELECT topic_name, partition, log_append_time, receiving_time, processed_time, consumer_id, offset_lag
FROM attempt3_0403
WHERE processed_time IS NOT NULL
AND status = 'processed'
"""