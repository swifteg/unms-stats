import requests
import json

class UnmsApi():
    def __init__(self, endpoint, api_key=''):
        self.__endpoint = endpoint
        self.__sess = requests.Session()
        self.__sess.headers.update({'x-auth-token':api_key})

    def __get(self, action, params=None):
        url = self.__endpoint+action
        if params is not None:
            query_str = '&'.join([k+'='+v for k,v in params.items()])
            url += '?'+query_str
        return self.__sess.get(url)

    def devices(self):
        return self.__get('/devices').json()

    def device_stats(self, id, interval='hour'):
        return self.__get(f'/devices/{id}/statistics', params={
            'interval':interval}).json()
    


