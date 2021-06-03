from factiva.news.stream import Stream

# CREATE by Query Test
stream_query_test = Stream(
    api_user='abcd1234abcd1234abcd1234abcd1234',
    request_info=True,
    query="publication_datetime >= '2021-04-01 00:00:00' AND LOWER(language_code)='en' AND UPPER(source_code) = 'DJDN'",
)

print(stream_query_test.create())


# CREATE by Snapshot Id Test
stream_snapshot_test = Stream(
    api_user='abcd1234abcd1234abcd1234abcd1234',
    request_info=True,
    snapshot_id='<snapshot-id>',
)

print(stream_snapshot_test.create())


# INFO Test
stream_info_test = Stream(
    stream_id='<stream_id>',
    api_user='abcd1234abcd1234abcd1234abcd1234',
    request_info=True,
)

print(stream_info_test.get_info())


# DELETE Test
stream_delete_test = Stream(
    stream_id='<stream_id>',
    api_user='abcd1234abcd1234abcd1234abcd1234',
    request_info=True,
)

print(stream_delete_test.delete())


# SUBSCRIPTIONS Test
stream_subscriptions_test = Stream(
    stream_id='<stream_id>',
    api_user='abcd1234abcd1234abcd1234abcd1234',
    request_info=True,
)

print(stream_subscriptions_test.all_subscriptions)
print(stream_subscriptions_test.create_subscription())
print(stream_subscriptions_test.all_subscriptions)
print(stream_subscriptions_test.delete_subscription('<subscription_id>'))
print(stream_subscriptions_test.all_subscriptions)


# LISTENER Test (uncomment/comment async or sync)
stream_listener_test = Stream(
    stream_id='<stream_id>',
    api_user='abcd1234abcd1234abcd1234abcd1234',
    request_info=True,
)

def callback(message, subscription_id):
    print('Subscription ID: {}: Message: {}'.format(subscription_id, message))
    return True  # If desired return False to stop the message flow. This will unblock the process as well.

stream_listener_test.consume_messages(
    callback=callback,
    subscription_id='<subscription-id>',
    maximum_messages=10,
    ack_enabled=True
    )

# stream_listener_test.consume_async_messages(
#   callback=callback,
#   subscription_id='<subscription-id>',
#   ack_enabled=True
# )
