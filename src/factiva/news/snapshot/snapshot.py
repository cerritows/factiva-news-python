from factiva.news.bulknews import BulkNewsBase
from .query import SnapshotQuery
from .jobs import ExplainJob, ExtractionJob, AnalyticsJob, UpdateJob

class Snapshot(BulkNewsBase):
    """
    Class that represents a Factiva Snapshot.

    Parameters
    ----------
    api_user : str or APIKeyUser
        String containing the 32-character long APi Key. If not provided, the
        constructor will try to obtain its value from the FACTIVA_APIKEY
        environment variable.
    request_userinfo : boolean, optional (Default: False)
        Indicates if user data has to be pulled from the server. This operation
        fills account detail properties along with maximum, used and remaining
        values. It may take several seconds to complete.
    query : str or SnapshotQuery, optional
        Query used to run any of the Snapshot-related operations. If a str is
        provided, a simple query with a `where` clause is created. If other
        query fields are required, either provide the SnapshotQuery object at
        creation, or set the appropriate object values after creation. This
        parameter is not compatible with snapshot_id.
    snapshot_id: str, optional
        String containing the 10-character long Snapshot ID. This parameter is
        not compatible with query.

    See Also
    --------
    Stream: Class that represents the continuous Factiva News document stream.

    Examples
    --------
    Creating a new Snapshot with an key string and a Where statement. Then,
    running a full Explain process.
        >>> from factiva.news.snapshot import Snapshot
        >>> my_key = "abcd1234abcd1234abcd1234abcd1234"
        >>> my_query = "publication_datetime >= '2020-01-01 00:00:00' AND LOWER(language_code) = 'en'"
        >>> my_snapshot = Snapshot(api_key=my_key, query=my_query)
        >>> my_snapshot.process_explain()
        106535

    Creating a new Snapshot from an APIKeyUser and a SnapshotQuery instances.
    Then, running a full Analytics process.
        >>> my_user = APIKeyUser()
        >>> my_query = SnapshotQuery("publication_datetime >= '2020-01-01 00:00:00' AND LOWER(language_code) = 'en'")
        >>> my_query.frequency = 'YEAR'
        >>> my_query.group_by_source_code = True
        >>> my_query.top = 20
        >>> my_snapshot = Snapshot(api_user=my_user, query=my_query)
        >>> analytics_df = my_snapshot.process_analytics()
        >>> analytics_df.head()
              count  publication_datetime  source_code
            0	20921	1995	NGCIOS
            1	20371	1995	LATAM
            2	18303	1995	REUTES
            3	10593	1995	EXPNSI
            4	4212	1995	MUNDO

    """
    query = None
    folder_path = ''
    file_format = ''
    file_list = []
    news_data = None
    last_explain_job = None
    last_analytics_job = None
    last_extraction_job = None
    last_update_job = None

    def __init__(
        self,
        api_user=None,
        request_userinfo=False,
        query=None,
        snapshot_id=None
    ):
        super().__init__(api_user=api_user, request_userinfo=request_userinfo)

        self.last_explain_job = ExplainJob(api_user=self.api_user)
        self.last_analytics_job = AnalyticsJob(api_user=self.api_user)

        if query and snapshot_id:
            raise Exception("The query and snapshot_id parameters cannot be set simultaneously")

        if query:
            if isinstance(query, SnapshotQuery):
                self.query = query
            elif isinstance(query, str):
                self.query = SnapshotQuery(query)
            else:
                raise ValueError("Unexpected value for the query-where clause")
            self.last_extraction_job = ExtractionJob(api_user=self.api_user)

        if snapshot_id:
            self.query = SnapshotQuery('')
            self.last_extraction_job = ExtractionJob(snapshot_id=snapshot_id, api_user=self.api_user)
            self.get_extraction_job_results()

    def submit_explain_job(self):
        """
        Submits an Explain job to the Factiva Snapshots API, using the
        assigned user in the `api_user`, and SnapshotQuery in the `query`
        properties.

        Returns
        -------
        Boolean : True if the submission was successful. An Exception otherwise.
        """
        return self.last_explain_job.submit_job(self.query.get_explain_query())

    def get_explain_job_results(self):
        """
        Obtains the Explain job results from the Factiva Snapshots API. Results
        are stored in the `last_explain_job` class property.

        Returns
        -------
        Boolean : True if the data was retrieved successfully. An Exception
            otherwise.
        """
        return self.last_explain_job.get_job_results()

    def process_explain(self):
        """
        Submits an Explain job to the Factiva Snapshots API, using the same
        parameters used by `submit_explain_job`. Then, monitors the job until
        its status change to `JOB_STATE_DONE`. Finally, retrieves and stores
        the results in the property `last_explain_job`.

        Returns
        -------
        Boolean : True if the explain processing was successful. An Exception
            otherwise.
        """
        return self.last_explain_job.process_job(self.query.get_explain_query())

    def submit_analytics_job(self):
        """
        Submits an Analytics job to the Factiva Snapshots API, using the
        assigned user in the `api_user`, and SnapshotQuery in the `query`
        properties.

        Returns
        -------
        Boolean : True if the submission was successful. An Exception otherwise.
        """
        return self.last_analytics_job.submit_job(self.query.get_analytics_query())

    def get_analytics_job_results(self):
        """
        Obtains the Analytics job results from the Factiva Snapshots API.
        Results are stored in the `last_analytics_job` class property.

        Returns
        -------
        Boolean : True if the data was retrieved successfully. An Exception
            otherwise.
        """
        return self.last_analytics_job.get_job_results()

    def process_analytics(self):
        """
        Submits an Analytics job to the Factiva Snapshots API, using the same
        parameters used by `submit_analytics_job`. Then, monitors the job until
        its status change to `JOB_STATE_DONE`. Finally, retrieves and stores
        the results in the property `last_analytics_job`.

        Returns
        -------
        Boolean : True if the analytics processing was successful. An Exception
            otherwise.
        """
        return self.last_analytics_job.process_job(self.query.get_analytics_query())

    def submit_extraction_job(self):
        """
        Submits an Extraction job to the Factiva Snapshots API, using the
        assigned user in the `api_user`, and SnapshotQuery in the `query`
        properties.

        Returns
        -------
        Boolean : True if the submission was successful. An Exception otherwise.
        """
        is_successful = self.last_extraction_job.submit_job(self.query.get_extraction_query())

        if is_successful:
            self.snapshot_id = self.last_extraction_job.job_id
        return is_successful

    def get_extraction_job_results(self):
        """
        Obtains the Extraction job results from the Factiva Snapshots API.
        Results are stored in the `last_extraction_job` class property.

        Returns
        -------
        Boolean : True if the data was retrieved successfully. An Exception
            otherwise.
        """
        return self.last_extraction_job.get_job_results()

    def download_extraction_files(self, download_path):
        """
        Downloads the list of files listed in the Snapshot.last_extraction_job.files
        property, and store them in a folder with the same name as the snapshot ID.

        Returns
        -------
        Boolean : True if the files were correctly downloaded, False otherwise.
        """
        return self.last_extraction_job.download_job_files(download_path)

    def process_extraction(self, download_path):
        """
        Submits an Extraction job to the Factiva Snapshots API, using the same
        parameters used by `submit_extraction_job`. Then, monitors the job until
        its status change to `JOB_STATE_DONE`. The final status is retrieved
        and stored in the property `last_extraction_job`, which among other
        properties, contains the list of files to download. The process then
        downloads all files to a subfolder named equal to the `short_id`
        property. Finally, the process ends after all files are downloaded.

        Because the whole processing takes places in a single call, it's
        expected that the execution of this operation takes several
        minutes, or even hours.

        Returns
        -------
        Boolean : True if the extraction processing was successful. An Exception
            otherwise.
        """
        return self.last_extraction_job.process_job(self.query.get_extraction_query(), download_path)

    def submit_update_job(self, update_type):
        self.last_update_job = UpdateJob(update_type=update_type, snapshot_id=self.last_extraction_job.job_id)
        return self.last_update_job.submit_job()
        
    def get_update_job_results(self):
        if self.last_update_job is None:
            raise RuntimeError('Update job has not been set')
        return self.last_update_job.get_job_results()

    def download_update_files(self, download_path):
        if self.last_update_job is None:
            raise RuntimeError('Update job has not been set')
        return self.last_update_job.download_job_files(download_path)

    def process_update(self, update_type, download_path=None):
        self.last_update_job = UpdateJob(update_type=update_type, snapshot_id=self.last_extraction_job.job_id)
        return self.last_update_job.process_job(path=download_path)

    def __repr__(self):
        return self.__str__()

    def __str__(self, detailed=True, prefix='  |-', root_prefix=''):
        pprop = self.__dict__.copy()
        child_prefix = '  |    |-'
        ret_val = str(self.__class__) + '\n'

        ret_val += f'{prefix}api_user: '
        ret_val += self.api_user.__str__()
        del pprop['api_user']
        ret_val += '\n'

        ret_val += f'{prefix}query: '
        ret_val += self.query.__str__(detailed=False, prefix=child_prefix)
        del pprop['query']
        ret_val += '\n'

        ret_val += f'{prefix}last_explain_job: '
        ret_val += self.last_explain_job.__str__(detailed=False, prefix=child_prefix)
        del pprop['last_explain_job']
        ret_val += '\n'

        ret_val += f'{prefix}last_analytics_job: '
        ret_val += self.last_analytics_job.__str__(detailed=False, prefix=child_prefix)
        del pprop['last_analytics_job']
        ret_val += '\n'

        ret_val += f'{prefix}last_extraction_job: '
        ret_val += self.last_extraction_job.__str__(detailed=False, prefix=child_prefix)
        del pprop['last_extraction_job']
        ret_val += '\n'

        ret_val += '\n'.join(('{}{} = {}'.format(prefix, item, pprop[item]) for item in pprop))
        return ret_val
