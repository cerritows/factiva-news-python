from factiva.core import const
from factiva import helper

from .listener import Listener

from factiva.core import StreamUser

class Subscription:
    """
    Class that represents a Subscription inside a stream.
    There a two possible operations for a Subscription:
        Create new one based on an existing Stream
        Delete an existing subscription from a Stream

    Parameters
    ----------
    url: str
        url used to create/delete a subscription
    stream_id: str
        represents a given stream by its id

    Raises
    -------
    ValueError: when a stream_id is undefined

    Examples
    --------
    Creating a new Subscription directly:
        >>> subscription = Subscription(<stream_id>)
        >>> created_subs = subscription.create(
                headers={'authorization': 'user-key'}
            )
        >>> print(created_subs)
        >>> { "id": "dj-synhub-extraction-*HH**", "type": "subscription" }
    """

    SUBSCRIPTION_IDX = 0
    id = None
    stream_id = None
    subscription_type = None
    listener = None

    def __init__(self, stream_id=None, id=None, subscription_type=None):
        if not stream_id:
            raise const.UNDEFINED_STREAM_ID_ERROR
        self.url = f'{const.API_HOST}{const.API_STREAMS_BASEPATH}'
        self.stream_id = stream_id
        self.id = id
        self.subscription_type = subscription_type

    def __repr__(self):
        return f'Subscription(id={self.id}, type={self.subscription_type})'
    
    def create_listener(self, user):
        '''
        Create listener allows to create a listener
        in a separate step for avoiding undefined
        subscription id
        Parameters
        ----------
        user: StreamUser
            user which possess access to any
            credentials/client needed
            for listener

        Raises
        -------
        RuntimeError: when user is not a StreamUser
        '''
        if not isinstance(user, StreamUser):
            raise RuntimeError('user is not a StreamUser instance')

        self.listener = Listener(
            subscription_id=self.id,
            stream_user=user
            )

    def create(self, headers=None):
        '''
        Create subscription allows a user to create
        another subscription to a given stream

        Parameters
        ----------
        stream_id: str
            to use for creating a subscription
        headers: dict
            which contains the token/acces key for authorization

        Returns
        ----------
        Data which contains:
        subscription's id and type created

        Raises
        -------
        ValueError: when a stream_id is undefined
        RuntimeError: when Unexpected API response happens
        '''
        if not self.stream_id:
            raise ValueError(
                '''
                stream_id is not defined,
                it must be defined for creating a subscription
                '''
            )

        uri = '{}/{}/subscriptions'.format(self.url, self.stream_id)
        response = helper.api_send_request(
            method='POST',
            endpoint_url=uri,
            headers=headers
            )
        if response.status_code == 201:
            response = response.json()
            data = response['data']
            self.id = data[self.SUBSCRIPTION_IDX]['id']
            self.subscription_type = data[self.SUBSCRIPTION_IDX]['type']
            return data
        else:
            raise RuntimeError('Unexpected API response')

    def delete(self, headers=None) -> bool:
        '''
        Delete subscription allows a user to delete
        a subscription to a given stream

        Parameters
        ----------
        headers: dict
            which contains the token/acces key for authorization

        Returns
        -----
        bool value which shows if the subscription is complete deleted

        Raises
        -------
        RuntimeError: when Unexpected API response happens
        '''
        uri = '{}/{}/subscriptions/{}'.format(
            self.url,
            self.stream_id,
            self.id
            )
        response = helper.api_send_request(
            method='DELETE',
            endpoint_url=uri,
            headers=headers
            )
        if response.status_code == 200:
            return True
        else:
            raise RuntimeError('Unexpected API response')

    def consume_messages(
        self,
        callback=None,
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
        maximum_messages: int
            is used for consuming a specific
            number of messages
        batch_size: int
            the limit of the batch expected

        Raises
        -------
        RuntimeError: when listener is not yet init
        '''
        if not self.listener:
            raise RuntimeError('uninitialized listener')
        if batch_size:
            self.listener.listen(
                callback=callback,
                maximum_messages=maximum_messages,
                batch_size=batch_size
                )
        else:
            self.listener.listen(
                callback=callback,
                maximum_messages=maximum_messages
                )

    def consume_async_messages(self, callback=None):
        '''
        Consume async messages is a listener function
        which consumes the current messages (News)
        from a pubsub subscription in async

        Parameters
        ----------
        callback :  function
            is used for processing a message

        Raises
        -------
        RuntimeError: when listener is not yet init
        '''
        if not self.listener:
            raise RuntimeError('uninitialized listener')
        self.listener.listen_async(
            callback=callback
        )
