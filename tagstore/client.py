import os.path
from copy import copy
from urlparse import urlunsplit, urlsplit
from uuid import uuid4

import requests

from ofs.local import PTOFS

import json


class QueryResponse(object):
    def __init__(self, client, params):
        self.client = client
        self.params = params

        self.objects = []
        self.iii = 0
        self.page = 1

        self.get_page()

    def get_page(self, page=None):
        params = copy(self.params)
        if page is not None:
            if page > self.num_pages:
                raise IndexError()
            params['page'] = page

        response = requests.get(self.client._api_endpoint('data'),
                                params=params, headers=self.client.headers_json)
        assert response.status_code == 200

        json = response.json()
        self.objects += json['objects']
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
    # 2-char bucket label for shallower pairtree
    BUCKET_LABEL = u'ts'
    OFS_SCHEME = u'ofs'

    def __init__(self, endpoint, ofs=None):
        self.endpoint = endpoint
        if not ofs:
            ofs = PTOFS(storage_dir='tagstore-data', uri_base='urn:uuid:',
                        hashing_type='sha256')
        self.ofs = ofs
        if self.BUCKET_LABEL not in self.ofs.list_buckets():
            self.bucket_id = self.ofs.claim_bucket(self.BUCKET_LABEL)
        else:
            self.bucket_id = self.BUCKET_LABEL

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

    def _wrap_filters(self, filters):
        """Wrap the filters for restless."""
        assert isinstance(filters, tuple) or isinstance(filters, list)
        return dict(filters=filters)

    def _data(self, uri, tags):
        """JSON representation of a Datum."""
        return dict(uri=uri, tags=map(self._wrap_tag, tags))

    def create(self, uri_or_fobj, tags):
        """Create a Datum."""
        if not isinstance(uri_or_fobj, basestring):
            # Store the file first.
            fobj = uri_or_fobj
            label = str(uuid4())
            self.ofs.put_stream(self.bucket_id, label, fobj)
            uri = urlunsplit((self.OFS_SCHEME, '', label, None, None))
        else:
            uri = uri_or_fobj

        response = requests.post(self._api_endpoint('data'),
                                 data=json.dumps(self._data(uri, tags)),
                                 headers=self.headers_json)
        assert response.status_code in (201, 409)
        return response

    def edit(self, instanceid, uri, tags):
        """Edit a Datum."""
        response = requests.put(self._api_endpoint('data', instanceid),
                            data=json.dumps(self._data(uri, tags)),
                            headers=self.headers_json)
        return response

    @classmethod
    def _tagobjs_to_tags(cls, tagobjs):
        return [tagobj['tag'] for tagobj in tagobjs]

    @classmethod
    def _get_tag_value(cls, tagobjs, key):
        for tag in cls._tagobjs_to_tags(tagobjs):
            if tag.startswith(u'{0}:'.format(key)):
                return tag[len(key) + 1:]
        return None

    def delete(self, instanceid):
        """Delete a Datum."""
        # If file is stored locally, delete it
        result = self.query([u'id', u'eq', unicode(instanceid)])
        if len(result) >= 1:
            obj = result.next()
            parts = urlsplit(obj['uri'])
            if parts.scheme == self.OFS_SCHEME:
                label = parts.path
                self.ofs.del_stream(self.bucket_id, label)
            else:
                raise BaseException(u'Cannot delete missing locally stored file')
        response = requests.delete(self._api_endpoint('data', instanceid),
                                   headers=self.headers_json)
        return response

    def query(self, *filters):
        """Query the tag store for Data that satisfies the filters.

        filters - many 3-ples consisting of name, operation, and value.
        
        See https://flask-restless.readthedocs.org/en/latest/
        searchformat.html#query-format

        The search format is simplified from a dictionary to a 3-ple and
        automatically reconstructed.

        """
        params = dict(q=json.dumps(
            self._wrap_filters(map(self._list_to_filter, filters))))
        return QueryResponse(self, params)

    @classmethod
    def _filter(cls, name=None, op=None, val=None):
        """Shorthand to create a filter object for REST API."""
        return dict(name=name, op=op, val=val)
