import os.path
from copy import copy
from urlparse import urlunsplit, urlsplit
from uuid import uuid4
import logging

log = logging.getLogger(__name__)

import requests


import json


class DataResponse(object):
    def __init__(self, client, json):
        self.client = client
        self.id = json['id']
        self.fname = json['fname']
        self.uri = json['uri']
        self.tags = [tag['tag'] for tag in json['tags']]

    @property
    def filename(self):
        return self.fname

    def open(self):
        return requests.get(self.uri, stream=True).raw

    def __repr__(self):
        return '<DataResponse({0}, {1}, {2}, {3})>'.format(
            self.id, self.uri, self.fname, self.tags)


class QueryResponse(object):
    def __init__(self, client, params):
        self.client = client
        self.params = params

        self.objects = []
        self.iii = 0
        self.page = 1

        self.get_page()

    @classmethod
    def query(cls, client, params):
        return requests.get(client._api_endpoint('data'), params=params,
                            headers=client.headers_json)

    def get_page(self, page=None):
        params = copy(self.params)
        if page is not None:
            if page > self.num_pages:
                raise IndexError()
            params['page'] = page

        response = self.query(self.client, params)
        assert response.status_code == 200

        json = response.json()
        self.objects += [DataResponse(self.client, obj) for obj in json['objects']]
        self.num_pages = json['total_pages']
        self.num_results = json['num_results']

    def __getitem__(self, value):
        try:
            return self.objects[value]
        except IndexError:
            self.page += 1
            self.get_page(self.page)
            return self[value]

    def __iter__(self):
        return self

    def next(self):
        if self.iii >= len(self.objects):
            if self.page >= self.num_pages:
                raise StopIteration
            else:
                self.page += 1
                self.get_page(self.page)
        self.iii += 1
        return self.objects[self.iii - 1]

    def __len__(self):
        return self.num_results

    def __repr__(self):
        return '<QueryResponse({0})>'.format(len(self))


class TagStoreClient(object):
    headers_json = {'Content-Type': 'application/json'}

    def __init__(self, endpoint):
        self.endpoint = endpoint

    def _api_endpoint(self, *segments):
        return '/'.join([self.endpoint] + map(unicode, segments))

    def _wrap_tag(self, tag):
        """Wrap a Tag for restless."""
        assert isinstance(tag, basestring)
        return dict(tag=tag)

    def _list_to_filter(self, lll):
        """Convert the client's 3-ple filter format to that of restless."""
        name, op, val = lll
        if isinstance(val, tuple) or isinstance(val, list):
            val = self._list_to_filter(val)
        return self._filter(name, op, val)

    def _wrap_filters(self, filters, **kwargs):
        """Wrap the filters for restless."""
        assert isinstance(filters, tuple) or isinstance(filters, list)
        return dict(filters=filters, **kwargs)

    def _data(self, uri, fname, tags):
        """JSON representation of a Datum."""
        return dict(uri=uri, fname=fname, tags=map(self._wrap_tag, tags))

    def create(self, uri_or_fobj, fname=None, tags=[]):
        """Create a Datum."""
        if not isinstance(uri_or_fobj, basestring):
            # Store the file first.
            fobj = uri_or_fobj
            if fname is None:
                try:
                    fname = fobj.name
                except AttributeError:
                    fname = 'blob'
            files = {'blob': (fname, fobj)}
            resp = requests.post(self._api_endpoint('ofs'), files=files)
            assert resp.status_code in (200, 201)
            data = resp.json()
            uri = data['uri']
        else:
            uri = uri_or_fobj
            if fname is None:
                fname = os.path.basename(uri)
                if not fname:
                    fname = 'blob'

        data = json.dumps(self._data(uri, fname, tags))
        response = requests.post(self._api_endpoint('data'),
                                 data=data, headers=self.headers_json)
        assert response.status_code in (201, 409)
        if response.status_code == 201:
            return DataResponse(self, response.json())
        else:
            return None

    def edit(self, instanceid, uri_or_fobj=None, fname=None, tags=None):
        """Edit a Datum."""
        data_endpoint = self._api_endpoint('data', unicode(instanceid))
        resp = requests.get(data_endpoint)
        if resp.status_code != 200:
            abort(404)
        dresp = DataResponse(self, resp.json())
        data = {}
        if uri_or_fobj is not None:
            uri = dresp.uri
            if not isinstance(uri_or_fobj, basestring):
                # Update the stored file
                fobj = uri_or_fobj
                resp = requests.put(uri, files={'blob': fobj})
                assert resp.status_code == 200
            else:
                # Update the the URI
                if uri != uri_or_fobj:
                    raise ValueError(u'Attempt to update blob while changing URI.')
                else:
                    data['uri'] = uri
        if fname is not None:
            data['fname'] = fname
            # If file is stored locally, also change its fname
            if self._is_local(uri) and dresp.fname != fname:
                response = requests.put(uri, data=dict(fname=fname))
        if tags is not None:
            data['tags'] = map(self._wrap_tag, tags)
        data = json.dumps(data)
        response = requests.put(data_endpoint, data=data,
                                headers=self.headers_json)
        assert response.status_code == 200
        return DataResponse(self, response.json())

    @classmethod
    def _tagobjs_to_tags(cls, tagobjs):
        return [tagobj['tag'] for tagobj in tagobjs]

    @classmethod
    def get_tag_value(cls, tagobjs, key):
        for tag in cls._tagobjs_to_tags(tagobjs):
            if tag.startswith(u'{0}:'.format(key)):
                return tag[len(key) + 1:]
        return None

    def _is_local(self, uri):
        return uri.startswith(self._api_endpoint('ofs'))

    def delete(self, instanceid):
        """Delete a Datum.
        
        force - delete the Datum even if stored locally and local blob is
        missing.

        """
        # If file is stored locally, delete it
        data_endpoint = self._api_endpoint('data', unicode(instanceid))
        resp = requests.get(data_endpoint)
        if resp.status_code == 200:
            obj = DataResponse(self, resp.json())
            if self._is_local(obj.uri):
                response = requests.delete(obj.uri)
        response = requests.delete(data_endpoint)
        assert response.status_code == 204
        return None

    def query(self, *filters, **kwargs):
        """Query the tag store for Data that satisfies the filters.

        filters - many 3-ples consisting of name, operation, and value.
        
        See https://flask-restless.readthedocs.org/en/latest/
        searchformat.html#query-format

        The search format is simplified from a dictionary to a 3-ple and
        automatically reconstructed.

        """
        params = dict(q=json.dumps(
            self._wrap_filters(map(self._list_to_filter, filters), **kwargs)))
        if kwargs.get('single', False):
            single = QueryResponse.query(self, params)
            if single.status_code == 200:
                return DataResponse(self, single.json())
            return None
        return QueryResponse(self, params)

    @classmethod
    def _filter(cls, name=None, op=None, val=None):
        """Shorthand to create a filter object for REST API."""
        return dict(name=name, op=op, val=val)


class Query(object):
    """Collection of methods to generate common queries."""
    @classmethod
    def tags_any(cls, op, value):
        return ['tags', 'any', ['tag', op, value]]
