import pandas as pd

from .Subscription import Subscription

from typing import List

from factiva.core import StreamUser, const
from factiva import helper
from factiva.news.bulknews import BulkNewsQuery


class Stream:
    """
    Class that represents a Stream workflow for Factiva API.

    Parameters
    ----------
    stream_id: str
        represents a given stream by its id
        if exists there is no need to have
        a query or a given snapshot id
    snapshot_id: str
        represents a snapshot by its id
        if exists it will be used
        to create a new stream
    query: str
        represents a query
        if exists it will be used
        to create a new stream
    stream_user: Stream User
        constructor will asign a stream user which has the access
        to Pubsub client, authentication headers and urls

    Auth method 1 (All required)
    api_user: str
        if the stream_user is not passed
        it can be created based on the api key param
    request_info: bool
        if the stream_user is not passed
        it can be created based on the request info param

    Auth method 2 (All required)
    user_id: str
        if the stream_user is not passed
        it can be created based on the user id param
    client_id: str
        if the stream_user is not passed
        it can be created based on the client id param
    password: str
        if the stream_user is not passed
        it can be created based on the password param

    Examples
    --------
    Creating a new Stream directly:
        >>> stream_query_test = Stream(
            api_user='abcd1234abcd1234abcd1234abcd1234',
            request_info=True,
            snapshot_id='<snapshot-id>',
            )
        >>> print(stream_query_test.create())
        >>>                       attributes                          id      relationships                                     type
        job_status     JOB_STATE_PENDING  dj-synhub-extraction-...                NaN                                          stream
        subscriptions                NaN  dj-synhub-extraction-...          {'data': [{'id': 'dj-synhub-extraction-...         stream
    """

    stream_id = None
    stream_user = None
    snapshot_id = None
    listener = None
    subscriptions = dict()

    def __init__(
        self,
        stream_id=None,
        snapshot_id=None,
        query='',
        stream_user=None,
        api_user=None,
        request_info=False
    ):
        self.stream_id = stream_id
        self.snapshot_id = snapshot_id
        self.query = BulkNewsQuery(query)
        if isinstance(stream_user, StreamUser):
            self.stream_user = stream_user
        else:
            self.stream_user = StreamUser(
                api_key=api_user,
                request_info=request_info,
            )
        if not self.stream_user:
            raise RuntimeError('Undefined Stream User')

        if stream_id:
            self.set_all_subscriptions()

    @property
    def stream_url(self) -> str:
        return f'{const.API_HOST}{const.API_STREAMS_BASEPATH}'

    @property
    def all_subscriptions(self) -> List[str]:
        return [sub.__repr__() for sub in self.subscriptions.values()]

    def get_info(self) -> pd.DataFrame:
        '''
        get_info allows a user
        consult a stream by its id

        Returns
        -----
        Dataframe which contains the state about the current stream

        Raises
        -------
        ValueError: when stream id is undefined
        RuntimeError: when the stream does not exists
        RuntimeError: when exists an unexpected HTTP error
        '''
        if not self.stream_id:
            raise const.UNDEFINED_STREAM_ID_ERROR
        uri = '{}/{}'.format(self.stream_url, self.stream_id)
        response = helper.api_send_request(
            method='GET',
            endpoint_url=uri,
            headers=self.stream_user.get_authentication_headers()
        )
        if response.status_code == 200:
            response = response.json()
            return pd.DataFrame.from_records([helper.flatten_dict(response['data'])])
        else:
            raise RuntimeError(response.text)

    def delete(self) -> pd.DataFrame:
        '''
        Delete allows
        to delete a stream

        Returns
        -----
        The current state which is expected to be DELETED

        Raises
        -------
        ValueError: when stream id is undefined
        RuntimeError: when the stream does not exists
        RuntimeError: when exists an unexpected HTTP error
        '''
        if not self.stream_id:
            raise const.UNDEFINED_STREAM_ID_ERROR

        uri = f'{self.stream_url}/{self.stream_id}'
        headers = self.stream_user.get_authentication_headers()
        headers['Content-Type'] = 'application/json'
        response = helper.api_send_request(
            method='DELETE',
            endpoint_url=uri,
            headers=headers,
        )
        if response.status_code == 200:
            response = response.json()
            return pd.DataFrame.from_records([helper.flatten_dict(response['data'])])
        elif response == 404:
            raise RuntimeError('The Stream does not exist')
        else:
            raise const.UNEXPECTED_HTTP_ERROR

    def create(self) -> pd.DataFrame:
        '''
        Create function which allows a user to create a stream subscription
        There are two available options:
        Create a stream using a query
        Create a stream using a snapshot id

        Returns
        -----
        Dataframe which contains the state about the current stream

        Raises
        -------
        ValueError: snapshot_id and query are undefned
        '''
        if not self.snapshot_id and not self.query:
            raise ValueError('Snapshot id and query not found')
        if self.snapshot_id:
            return self._create_by_snapshot_id()
        else:
            return self._create_by_query()

    def create_subscription(self):
        '''
        Create subcription allows a user
        to create a another subscription
        for an existing stream

        Returns
        -----
        Dataframe which contains the state about the current stream

        Raises
        -------
        RuntimeError: when unable to create a subscription
        '''
        try:
            new_subscription = Subscription(stream_id=self.stream_id)
            new_subscription.create(
                headers=self.stream_user.get_authentication_headers()
            )
            new_subscription.create_listener(self.stream_user)
            self.subscriptions[new_subscription.id] = new_subscription
            return new_subscription.id
        except Exception as e:
            raise RuntimeError(
                f'''
                Unexpected error happened while
                creating the subscription: {e}
                '''
            )

    def delete_subscription(self, id) -> bool:
        '''
        Delete subcription allows a user
        to delete a subscription
        for an existing stream

        Parameters
        ----------
        id :  str
            is the representation of a given
            subscription planned to be deleted

        Returns
        -----
        Dataframe which contains the state about the current stream

        Raises
        -------
        ValueError: when there is invalid subscription id
        RuntimeError: when unable to delete a subscription
        '''
        if id not in self.subscriptions:
            raise const.INVALID_SUBSCRIPTION_ID_ERROR
        try:
            if self.subscriptions[id].delete(
                headers=self.stream_user.get_authentication_headers()
            ):
                del self.subscriptions[id]
                return True
        except Exception:
            raise RuntimeError('Unable to delete subscription')

    def create_default_subscription(self, response):
        '''
        Creates the default subscriptions
        which comes together with the stream at
        initilization
        Adds the subscriptions to subscriptions dict

        Parameters
        ----------
        response :  dict
            is used for setting every subscription
            which exists inside the stream
        '''
        for subscription in response['data']['relationships']['subscriptions']['data']:
            subscription_obj = Subscription(
                id=subscription['id'],
                stream_id=self.stream_id,
                subscription_type=subscription['type'],
                )
            subscription_obj.create_listener(self.stream_user)
            self.subscriptions[subscription_obj.id] = subscription_obj

    def set_all_subscriptions(self):
        '''
        set_all_subscriptions allows a user
        to set all subscriptions
        from a stream to local storage

        Returns
        -----
        Dataframe which contains the state about the current stream
        Raises
        -------
        ValueError: when stream id is undefined
        '''
        if not self.stream_id:
            raise const.UNDEFINED_STREAM_ID_ERROR
        uri = '{}/{}'.format(self.stream_url, self.stream_id)
        response = helper.api_send_request(
            method='GET',
            endpoint_url=uri,
            headers=self.stream_user.get_authentication_headers()
        )
        if response.status_code == 200:
            response = response.json()
            self.create_default_subscription(response)
        else:
            raise const.UNEXPECTED_HTTP_ERROR

    def consume_messages(
        self,
        callback=None,
        subscription_id=None,
        maximum_messages=None,
        batch_size=None
    ):
        '''
        Consume messages is a listener function
        which consumes the current messages (News)
        from a pubsub subscription in sync

        Parameters
        ----------
        callback :  function
            is used for processing a message
        subscription_id :  str
            is used for connecting to pubsub
        maximum_messages: int
            is used for consuming a specific
            number of messages
        batch_size: int
            the limit of the batch expected

        Raises
        -------
        ValueError: when subscription id is invalid
        '''
        if subscription_id not in self.subscriptions:
            raise const.INVALID_SUBSCRIPTION_ID_ERROR
        self.subscriptions[subscription_id].consume_messages(
            callback=callback,
            maximum_messages=maximum_messages,
            batch_size=batch_size
            )

    def consume_async_messages(self, callback=None, subscription_id=None):
        '''
        Consume messages is a listener function
        which consumes the current messages (News)
        from a pubsub subscription in async

        Parameters
        ----------
        callback :  function
            is used for processing a message
        subscription_id :  str
            is used for connecting to pubsub

        Raises
        -------
        ValueError: when subscription id is invalid
        '''
        if subscription_id not in self.subscriptions:
            raise const.INVALID_SUBSCRIPTION_ID_ERROR
        self.subscriptions[subscription_id].consume_async_messages(
            callback=callback,
            )

    def _create_by_snapshot_id(self):
        '''
        Create by snapshot id
        allows a user to create a stream subscription
        using a snapshot id

        Returns
        -----
        Dataframe which contains the state about the current stream

        Raises
        -------
        ValueError: When query is undefined
        RuntimeError: When API request returns unexpected error
        '''
        if not self.snapshot_id:
            raise ValueError('create fails: snaphot_id undefined')

        headers = self.stream_user.get_authentication_headers()
        headers['Content-Type'] = 'application/json'
        uri = f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}/{self.snapshot_id}/streams'
        response = helper.api_send_request(
            method='POST',
            endpoint_url=uri,
            headers=headers,
        )
        if response.status_code == 201:
            response = response.json()
            self.stream_id = response['data']['id']
            self.create_default_subscription(response)

            return pd.DataFrame.from_records([helper.flatten_dict(response['data'])])
        else:
            raise const.UNEXPECTED_HTTP_ERROR

    def _create_by_query(self):
        '''
        Create by query
        allows a user to create a stream subscription
        using a query

        Returns
        -----
        Dataframe which contains the state about the current stream

        Raises
        -------
        ValueError: When query is undefined
        RuntimeError: When API request returns unexpected error
        '''
        if not self.query:
            raise ValueError('Streams query undefined in Create by query')

        base_query = self.query.get_base_query()
        streams_query = {
            "data": {
                "attributes": base_query['query'],
                "type": "stream"
                }
            }

        headers = self.stream_user.get_authentication_headers()
        headers['Content-Type'] = 'application/json'
        response = helper.api_send_request(
                method='POST',
                endpoint_url=self.stream_url,
                headers=headers,
                payload=streams_query,
            )

        if response.status_code == 201:
            response = response.json()
            self.stream_id = response['data']['id']
            self.create_default_subscription(response)

            return pd.DataFrame.from_records([helper.flatten_dict(response['data'])])
        else:
            raise const.UNEXPECTED_HTTP_ERROR
