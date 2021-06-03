
class StreamResponse:
    '''
    Class that represents a Stream Response
    in string format.

    Parameters
    ----------
    response: dict
        represents a given stream response
        by its json representation

    Raises
    -------
    ValueError: When there is an empty response

    Examples
    --------
    Create a StreamResponse
        >>> result = StreamResponse(response)
        >>> print(result)

    type: stream
    id: dj-synhub-extraction-*********************************
    attributes:
        job_status: JOB_STATE_RUNNING

    relationships:
        subscriptions:
                data:
                        id: dj-synhub-extraction-*********************************-filtered-*****
                        type: Subscription

                data:
                        id: dj-synhub-extraction-*********************************-filtered-*****
                        type: Subscription

                data:
                        id: dj-synhub-extraction-*********************************-filtered-*****
                        type: Subscription

    links: 
        self: https://api.dowjones.com/alpha/streams/dj-synhub-extraction-*********************************
    '''


    stype = ''
    id = ''
    attributes = ''
    relationships = ''
    links = ''

    def __init__(self, response):
        if not response:
            raise ValueError('Empty value for reponse')
        self.parse_data(response)
        self.parse_links(response)

    def parse_data(self, response):
        '''
        parses data
        if it exists

        Parameters
        ----------
        data: dict
            dict which must contain data

        Raises
        -------
        ValueError: When there is empty data
        '''
        if not response['data']:
            raise ValueError('Empty data attribute')

        data = response['data']
        self.stype = data['type']
        self.id = data['id']
        self.attributes = self.parse_object(data['attributes'])
        self.relationships = self.parse_object(data['relationships'])

    def parse_links(self, response):
        '''
        parses links
        if they exists

        Parameters
        ----------
        data: dict
            dict which may contains links 
        '''
        if 'links' in response:
            self.links = self.parse_object(response['links'])

    def parse_object(self, data, level=2):
        '''
        parses object representation
        per level of identation

        Parameters
        ----------
        data: dict
            representation of every key value pair
        level: int
            level of identation needed

        Returns
        -----
        String which contains all attributes and values
        with identation and spaces included
        '''
        object_repr = ''
        idents = "\t" * level
        for k, v in data.items():
            if isinstance(v, dict):
                object_repr += f'{idents}{k}: \n{self.parse_object(v, level + 1)}\n'
            elif isinstance(v, list):
                for att in v:
                    object_repr += f'{idents}{k}: \n{self.parse_object(att, level + 1)}\n'
            else:
                object_repr += f'{idents}{k}: {v}\n'

        return object_repr

    def __repr__(self):
        return '''
            type: {}
            id: {}
            attributes: \n{}
            relationships: \n{}
            links: \n{}
            '''.format(
            self.stype,
            self.id,
            self.attributes,
            self.relationships,
            self.links,
        )
