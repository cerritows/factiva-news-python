import json
import time

from threading import Thread
from factiva import helper
from factiva.core import const
from google.api_core.exceptions import GoogleAPICallError, NotFound


def default_callback(message, subscription_id):
    print('Subscription ID: {}: Message: {}'.format(subscription_id, message))


class Listener:
    """
    Class that represents a Listener for Google Pubsub.

    Parameters
    ----------
    stream_user : Stream User
        constructor will asign a stream user which has the access
        to the proper url and headers which are going to be used for:

        - Checking the exceeded documents
        - Consuming messages (articles) in sync
        - Consuming messages (articles) in async

    subscription_id :  str
        is used by Pubsub
        to consume messages in async/sync

    Examples
    --------
    Creating a new Listener directly:
        >>> listener = Listener(
                stream_user=StreamUser(
                    api_key='****************************1234',
                    request_info=False,
                    user_id='******-svcaccount@dowjones.com',
                    client_id='****************************5678',
                    password='*******'
                )
            )
        >>> def callback(message, subscription_id):
        >>>     print('Subscription ID: {}: Message: {}'.format(
                    subscription_id, message
                ))
        >>> print(listener.listen(
                callback,
                subscription_id='<subscription-id>',
                maximum_messages=10
                ))
        Received news message with ID: DJDN******************
        Subscription ID: dj-synhub-stream-********************-
        km******-filtered-******:
        Message: {'an': 'DJDN0*********************',
        'document_type': 'article', 'action': 'rep',
        'source_code': 'DJDN', 'source_name': 'Dow Jones ---- -----',
        'publication_date': '2021-05-20T08:00:10.255Z',
        'publication_datetime': '2021-05-20T08:00:10.255Z',
        'modification_date': '2021-05-20T08:04:56.175Z',
        'modification_datetime': '2021-05-20T08:02:54.000Z',
        'ingestion_datetime': '2021-05-20T08:00:13.000Z',
        'title': "----- ------ ------ ------ 2020",
        'snippet': '', 'body': "\nOn Thursday -- --- ----,
        --- Plc. announced its ----- ---- --- \n
        ......
        }
    """

    _check_exceeds_thread = None

    def __init__(self, subscription_id=None, stream_user=None):
        if not subscription_id:
            raise const.UNDEFINED_SUBSCRIPTION_ERROR
        self.stream_user = stream_user
        self.subscription_id = subscription_id
        self.is_consuming = True

    def _check_exceeded(self):
        '''
        _check exceeded function
        checks if the documents have been exceeded
        (max allowed extractions exceeded)

        Parameters
        ----------

        Raises
        -------
        RuntimeError: When HTTP API Response is unexpected
        '''
        host = self.stream_user.get_uri_context()
        headers = self.stream_user.get_authentication_headers()
        limit_msg = None
        stream_id = '-'.join(self.subscription_id.split("-")[:-2])
        stream_id_uri = f'{host}/streams/{stream_id}'
        while self.is_consuming:
            print('Checking if extractions limit is reached')
            response = helper.api_send_request(
                method='GET',
                endpoint_url=stream_id_uri,
                headers=headers
                )
            if response.status_code == 200:
                response = response.json()
                job_status = response['data']['attributes']['job_status']
                if job_status == const.DOC_COUNT_EXCEEDED:
                    if "Authorization" in headers:
                        limits_uri = f'{host}/accounts/{self.stream_user.client_id}'
                    else:
                        limits_uri = f'{host}/accounts/{self.stream_user.api_key}'

                    limit_response = helper.api_send_request(
                        method='GET',
                        endpoint_url=limits_uri,
                        headers=headers
                        )
                    if limit_response.status_code == 200:
                        limit_response = limit_response.json()
                        limit_msg = limit_response['data']['attributes']['max_allowed_extracts']
                    else:
                        raise RuntimeError(
                            '''
                            Unexpected HTTP Response from API
                            while checking for limits
                            '''
                            )
            else:
                raise RuntimeError('HTTP API Response unexpected')
            time.sleep(const.CHECK_EXCEEDED_WAIT_SPACING)

        if not limit_msg:
            print('Job finished')
        else:
            print(
                '''
                OOPS! Looks like you\'ve exceeded the maximum number of
                documents received for your account ({}). As such, no
                new documents will be added to your stream\'s queue.
                However, you won\'t lose access to any documents that
                have already been added to the queue. These will continue
                to be streamed to you. Contact your account administrator
                with any questions or to upgrade your account limits.
                '''.format(limit_msg)
            )

    def check_exceeded_thread(self):
        '''
        check exceeded thread function
        creates threads for checking
        if the doc count has been exceeded
        '''
        self._check_exceeds_thread = Thread(target=self._check_exceeded)
        self._check_exceeds_thread.start()

    def listen(
        self,
        callback=default_callback,
        maximum_messages=None,
        batch_size=10
    ):
        '''
        listen function
        listens the current messages (News)
        from a pubsub subscription in sync

        Parameters
        ----------
        callback :  function
            is used for processing a message
        maximum_messages: int
            is used for consuming a specific
            number of messages
        batch_size: int
            the limit of the batch expected

        Raises
        -------
        ValueError:
            When maximum_messages is undefined
        GoogleAPICallError:
            When there is no valid instance to pull from
            When something unexpected happened with Pubsub client
        '''
        if not maximum_messages:
            raise ValueError('undefined maximum messages to proceed')

        pubsub_client = self.stream_user.get_client_subscription()
        self.check_exceeded_thread()

        streaming_credentials = self.stream_user.fetch_credentials()
        subscription_path = pubsub_client.subscription_path(
            streaming_credentials['project_id'],
            self.subscription_id
            )
        print(
            '''
            Listeners for subscriptions have been set up
            and await message arrival.
            '''
            )

        messages_count = 0
        pubsub_request = {
            "subscription": subscription_path,
            "max_messages": batch_size,
            "return_immediately": True
            }
        while maximum_messages is None or messages_count < maximum_messages:
            try:
                if maximum_messages is not None:
                    batch_size = min(
                        batch_size,
                        maximum_messages - messages_count
                        )
                results = pubsub_client.pull(request=pubsub_request)
                if results and results.received_messages:
                    for message in results.received_messages:
                        pubsub_message = json.loads(message.message.data)
                        print("Received news message with ID: {}".format(
                            pubsub_message['data'][0]['id'])
                        )
                        news_message = pubsub_message['data'][0]['attributes']
                        callback_result = callback(
                            news_message,
                            self.subscription_id
                            )
                        pubsub_client.acknowledge(
                            subscription=subscription_path,
                            ack_ids=[message.ack_id]
                            )
                        messages_count += 1
                        if not callback_result:
                            return
                        else:
                            print(callback_result)

            except GoogleAPICallError as e:
                if isinstance(e, NotFound):
                    raise e
                print(
                    '''
                    Encountered a problem while trying to pull a message
                    from a stream. Error is as follows: {}
                    '''.format(str(e))
                    )
                print(
                    '''
                    Due to the previous error, system will pause 10 seconds.
                    System will then attempt to pull the message from
                    the stream again.
                    '''
                    )
                time.sleep(10)
                pubsub_client = self.stream_user.get_client_subscription()

        self.is_consuming = False

    def listen_async(self, callback=default_callback):
        '''
        listen async function
        listens the current messages (News)
        from a pubsub subscription in async

        Parameters
        ----------
        callback :  function
            is used for processing a message
        '''
        def ack_message_and_callback(message):
            pubsub_message = json.loads(message.data)
            print("Received news message with ID: {}".format(
                pubsub_message['data'][0]['id']
                )
            )
            news_message = pubsub_message['data'][0]['attributes']
            callback(news_message, self.subscription_id)
            message.ack()

        pubsub_client = self.stream_user.get_client_subscription()
        self.check_exceeded_thread()

        streaming_credentials = self.stream_user.fetch_credentials()
        subscription_path = pubsub_client.subscription_path(
            streaming_credentials['project_id'],
            self.subscription_id
            )
        pubsub_client.subscribe(
            subscription_path,
            callback=ack_message_and_callback
            )
        print(
            '''
            Listeners for subscriptions have been set up
            and await message arrival.
            '''
            )