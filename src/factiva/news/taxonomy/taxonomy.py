import pandas as pd

from io import StringIO

from factiva.core import APIKeyUser
from factiva.core import const
from factiva import helper

class Taxonomy():
    '''
    Class that represents the taxonomy available within the Snapshots API

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

    '''

    categories = []

    def __init__(self, api_user = None, request_userinfo = False):
        self.api_user = APIKeyUser.create_api_user(api_user, request_userinfo)
        self.categories = self.get_categories()

    def get_categories(self):
        '''
        Requests a list of available taxonomy categories

        Returns
        -------
        List of available taxonomy categories.
        
        Raises
        -------
        RuntimeError: When API request returns unexpected error

        '''
        headers_dict = {
            'user-key': self.api_user.api_key
        }

        endpoint = f'{const.API_HOST}{const.API_SNAPSHOTS_TAXONOMY_BASEPATH}'

        response = helper.api_send_request(method='GET', endpoint_url=endpoint, headers=headers_dict)

        if response.status_code == 200:
            return [ entry['attributes']['name'] for entry in response.json()['data'] ]
        else:
            raise RuntimeError('API Request returned an unexpected HTTP status')

    def get_codes(self, category):
        '''
        Requests the codes available in the taxonomy for the specified category
        
        Parameters
        ----------
        category : str
            String with the name of the taxonomy category to request the codes from
        
        Returns
        -------
        Dataframe containing the codes for the specified category

        Raises
        -------
        ValueError: When category is not of a valid type
        RuntimeError: When API request returns unexpected error
        '''
        helper.validate_type(category, str, 'Unexpected value: category value must be string')
        
        response_format = 'csv'
        
        headers_dict = {
            'user-key' : self.api_user.api_key
        }

        endpoint = f'{const.API_HOST}{const.API_SNAPSHOTS_TAXONOMY_BASEPATH}/{category}/{response_format}'

        response = helper.api_send_request(method='GET', endpoint_url=endpoint, headers=headers_dict)
        
        if response.status_code == 200:
            return pd.read_csv(StringIO(response.content.decode()))
        else:
            raise RuntimeError('API Request returned an unexpected HTTP Status')

  
    def get_single_company(self, code_type, company_code):
        '''
        Requests information about a single company

        Parameters
        ----------
        code_type : str
            String describing the code type used to request the information about the company. E.g. isin, ticker.
        company_code : str
            String containing the company code
        
        Returns
        -------
        DataFrame containing the company information

        Raises
        -------
        RuntimeError: When API request returns unexpected error
        '''
        helper.validate_type(code_type, str, 'Unexpected value: code_type must be str')
        helper.validate_type(company_code, str, 'Unexpected value: company must be str')
        
        headers_dict = {
            'user-key': self.api_user.api_key
        }

        endpoint = f'{const.API_HOST}{const.API_SNAPSHOTS_COMPANIES_BASEPATH}/{code_type}/{company_code}'

        response = helper.api_send_request(method='GET', endpoint_url=endpoint, headers=headers_dict)

        if response.status_code == 200:
            response_data = response.json()
            return pd.DataFrame.from_records( [ response_data['data']['attributes'] ] )
        else:
            raise RuntimeError('API Request returned an unexpected HTTP status')
    
    def get_multiple_companies(self, code_type, companies_codes):
        '''
        Requests information about a list of companies

        Parameters
        ----------
        code_type : str
            String describing the code type used to request the information about the company. E.g. isin, ticker.
        companies_codes : list
            List containing the company codes to request information about
        
        Returns
        -------
        DataFrame containing the company information

        Raises
        -------
        RuntimeError: When API request returns unexpected error
        '''
        helper.validate_type(code_type, str, 'Unexpected value: code_type must be str')
        helper.validate_type(companies_codes, list, 'Unexpected value: companies must be list')
        for single_company_code in companies_codes:
            helper.validate_type(single_company_code, str, 'Unexpected value: each company in companies must be str')

        headers_dict = {
            'user-key': self.api_user.api_key
        }

        payload_dict = {
            "data": {
                "attributes": {
                    "ids": companies_codes
                }
            }
        }

        endpoint = f'{const.API_HOST}{const.API_SNAPSHOTS_COMPANIES_BASEPATH}/{code_type}'

        response = helper.api_send_request(method='POST', endpoint_url=endpoint, headers=headers_dict, payload=payload_dict)

        if response.status_code == 207:
            response_data = response.json()
            return pd.DataFrame.from_records(response_data['data']['attributes']['successes'])
        else:
            raise RuntimeError('API Request returned an unexpected HTTP status')


    def get_company(self, code_type, company_code = None, companies_codes = None):
        '''
        Requests information about either a single company or a list of companies

        Parameters
        ----------
        code_type : str
            String describing the code type used to request the information about the company. E.g. isin, ticker.
        company_code: str, Optional
            Single company code to request data about. Not compatible with companies_codes.
        companies_codes: List[str], Optional
            List of string that contains the company codes to request data about. Not compatible with company_code.
        
        Returns
        -------
        Dataframe with the information about the requested company(ies)

        Raises
        -------
        ValueError: When any given argument is not of the expected type
        RuntimeError: 
            - When both company and companies arguments are set
            - When API request returns unexpected error
        '''
        if company_code is not None and companies_codes is not None:
            raise RuntimeError('company and companies parameters cannot be set simultaneously')
        
        if company_code is not None:
            return self.get_single_company(code_type, company_code)
        
        if companies_codes is not None:
            return self.get_multiple_companies(code_type, companies_codes)
