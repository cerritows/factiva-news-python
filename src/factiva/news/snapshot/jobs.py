import pandas as pd


from factiva.core import const
from factiva.news.bulknews import BulkNewsJob

class ExplainJob(BulkNewsJob):
    """
    Class that represents the operation of creating an explain from Factiva Snapshots API.
    """
    
    document_volume = 0

    def __init__(self, api_user):
        super().__init__(api_user=api_user)
        self.document_volume = 0

    def get_endpoint_url(self):
        return f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}{const.API_EXPLAIN_SUFFIX}'

    def get_job_id(self, source):
        return source['data']['id']

    def set_job_data(self, source):
        self.document_volume = source['data']['attributes']['counts']

class AnalyticsJob(BulkNewsJob):
    """
    Class that represents the operation of creating Analtyics from Factiva Snapshots API.
    """
    data = []

    def __init__(self, api_user):
        super().__init__(api_user=api_user)
        self.data = []

    def get_endpoint_url(self):
        return f'{const.API_HOST}{const.API_ANALYTICS_BASEPATH}'

    def get_job_id(self, source):
        return source['data']['id']

    def set_job_data(self, source):
        self.data = pd.DataFrame(source['data']['attributes']['results'])

class ExtractionJob(BulkNewsJob):
    """
    Class that represents the operation of creating a Snapshot from Factiva Snapshots API.
    """
    files = []
    file_format = ''

    def __init__(self, snapshot_id=None, api_user=None):
        super().__init__(api_user=api_user)
        self.files = []
        self.file_format = ''

        if snapshot_id and api_user:
            self.job_id = snapshot_id
            self.link = f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}/dj-synhub-extraction-{self.api_user.api_key}-{snapshot_id}'

    def get_endpoint_url(self):
        return f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}'
    
    def get_job_id(self, source):
        return source['data']['id'].split('-')[-1]

    def set_job_data(self, source):
        self.file_format = source['data']['attributes']['format']
        file_list = source['data']['attributes']['files']
        self.files = [ file_item['uri'] for file_item in file_list ]
    
    def process_job(self, payload=None, path = None):
        """
        Overrides method from parent class to call the method for downloading the files once the snapshot has been completed.

        Parameters
        ----------
        path: str, Optional
            String containg the path where to store the snapshots files that are downloaded from the snapshot. If no path is given, the files will be stored in a folder named after the snapshot_id in the current working directory.
        """
        super().process_job(payload)
        self.download_job_files(path)

class UpdateJob(ExtractionJob):
    """
    Class that represents the Snapshot Updates. There can be three types of updates: additions, replacements and deletes.

    Parameters
    ----------
    update_type: str, Optional
        String describing the type of update that this job represents. Requires snapshot_id to be provided as well. Not compatible with update_id
    snapshot_id: str, Optional
        String containing the id of the snapshot that is being updated. Requires update_type to be provided as well. Not compatible with update_id
    update_id: str, Optional
        String containing the id of an update job that has been created previously. Both update_type and snapshot_id can be obtained from this value. Not compatible with update_type nor snapshot_id

    Raises
    ------
    - Exception when fields that are not compatible are provided or when not enough parameters are provided to create the job.

    """
    update_type = None
    snapshot_id = None

    def __init__(self, update_type=None, snapshot_id=None, update_id=None, api_user=None):
        super().__init__(api_user=api_user)

        if update_id and ( update_type or snapshot_id ):
            raise Exception('update_id parameter is not compatible with update_type and snapshot id')

        if update_id:
            self.job_id = update_id
            self.update_type = update_id.split('-')[1]
            self.snapshot_id = update_id.split('-')[0]
            self.link = f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}/dj-synhub-extraction-{self.api_user.api_key}-{update_id}'
            self.get_job_results()

        elif update_type and snapshot_id:
            self.update_type = update_type
            self.snapshot_id = snapshot_id         
        else:
            raise Exception('Not enough parameters to create an update job')

    def get_endpoint_url(self):
        return f'{const.API_HOST}{const.API_EXTRACTIONS_BASEPATH}/dj-synhub-extraction-{self.api_user.api_key}-{self.snapshot_id}/{self.update_type}'
    
    def get_job_id(self, source):
        # UPDATE_ID FORMAT: {API_URL}/dj-synhub-extraction-{USER-KEY}-{SNAPSHOT_ID}-{UPDATE_TYPE}-{DATETIME}
        return '-'.join(source['data']['id'].split('-')[-3:])
    