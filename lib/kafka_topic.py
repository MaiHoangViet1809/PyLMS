from kafka.admin import KafkaAdminClient, NewTopic


def create_admin_client(config: dict = None):
    if config is None:
        config = {"bootstrap_servers":"localhost:9092"
                ,"client_id": "test_id"
                  }
    admin_client = KafkaAdminClient(**config)
    return admin_client


def create_topic(topic_name: str, num_partitions=1, replication_factor=1, admin_client: KafkaAdminClient = None):
    if admin_client is None:
        admin_client = create_admin_client()

    # noinspection PyBroadException
    try:
        admin_client.create_topics(new_topics=[NewTopic(name=topic_name, num_partitions=num_partitions
                                                      , replication_factor=replication_factor)]
                                   , validate_only=False)
        return True
    except:
        print("create_topic --{}-- is failed !".format(topic_name))
        return False


def drop_topic(topic_name: str, admin_client: KafkaAdminClient = None):
    if admin_client is None:
        admin_client = create_admin_client()
    # noinspection PyBroadException
    try:
        admin_client.delete_topics(topics=[topic_name], timeout_ms=1000)
        return True
    except:
        print("drop_topic --{}-- is failed !".format(topic_name))
        return False
