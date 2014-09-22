import os.path
from uuid import uuid4

import requests

from ofs.local import PTOFS

import json


# Monkey patch PTOFS use of PairTreeFileStorageClient
def get_url(self, bucket, label):
    if self.exists(bucket) and self.exists(bucket, label):
        return os.path.join(self._store._id_to_dirpath(bucket), label)
    else:
        raise FileNotFoundException
PTOFS.get_url = get_url


class TagStoreClient(object):
    headers_json = {'Content-Type': 'application/json'}
    BUCKET_LABEL = u'tagged'
    STORED_LOCALLY = u'stored_locally'

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
            uri = self.ofs.get_url(self.bucket_id, label)
            tags.append(u'{0}:{1}'.format(self.STORED_LOCALLY, label))
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
        response = self.query([u'id', u'eq', unicode(instanceid)])
        result = response.json()
        if result['num_results'] >= 1:
            tagobjs = result['objects'][0]['tags']
            label = self._get_tag_value(tagobjs, self.STORED_LOCALLY)
            if label:
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
        response = requests.get(self._api_endpoint('data'), params=params,
                                headers=self.headers_json)
        assert response.status_code == 200
        return response

    @classmethod
    def _filter(cls, name=None, op=None, val=None):
        """Shorthand to create a filter object for REST API."""
        return dict(name=name, op=op, val=val)
