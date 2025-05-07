# const
# key: topic
# value: consumer group
PROJECT_SETUP = {
    "flight-atc": "atc_group",
    "engine-logs": "engine_log_group",
    "hydraulic-logs": "hydraulic_log_group"
}

# initialize variable
# key: consumer group
# value: tuple(baseline, max_batch)
max_lifetime_by_consumer_group = {
    "atc_group": (0, 0),
    "engine_log_group": (0, 0),
    "hydraulic_log_group": (0, 0)
}


def main():
    try:

    except Exception as e:
        print(f"error: {e}")
    finally:
        print("message lifetime prediction done.")

if __name__ == '__main__':
    print("Hello, from Load-shedding Engine!")
    main()
