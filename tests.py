import json
import os.path
from datetime import datetime, timedelta
from StringIO import StringIO
import logging
from shutil import rmtree
from urlparse import urlsplit

log = logging.getLogger(__name__)

from flask import Flask
from flask.ext.testing import TestCase, LiveServerTestCase
from flask.ext.restless import ProcessingException

import requests

import tagstore
from tagstore.server import (
    init_app, ofs, replace_existing_tags, data_post, gc_ofs
)
from tagstore.client import TagStoreClient, Query, DataResponse
from tagstore.models import db, Tag, Data


API_ENDPOINT = '/api/v1'


def _create_test_app(self):
    app = Flask(__name__)
    app.config.from_object('tagstore.settings.default')
    app.config.from_object('tagstore.settings.test')
    init_app(app)
    return app


class BaseTest(TestCase):
    create_app = _create_test_app

    def setUp(self):
        db.create_all()
        try:
            rmtree(self.app.config['PTOFS_DIR'])
        except OSError:
            pass

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        try:
            rmtree(self.app.config['PTOFS_DIR'])
        except OSError:
            pass


class TestUnit(BaseTest):
    def test_replace_existing_tags(self):
        data = {'tags': []}
        replace_existing_tags(data)
        self.assertTrue(data['tags'] == [])

        data = {'tags': [{'tag': 'aaa'}]}
        replace_existing_tags(data)
        self.assertTrue(data['tags'][0] == {'tag': 'aaa'})

        db.session.add(Tag('aaa'))
        db.session.flush()
        data = {'tags': [{'tag': 'aaa'}]}
        replace_existing_tags(data)
        tag = Tag.query.filter_by(tag='aaa').first()
        self.assertTrue(data['tags'][0] == {'id': tag.id})

        data = {'tags': [{'tag': 'aaa'}, {'tag': 'bbb'}]}
        replace_existing_tags(data)
        tag = Tag.query.filter_by(tag='aaa').first()
        self.assertTrue(data['tags'][0] == {'id': tag.id})
        self.assertTrue(data['tags'][1] == {'tag': 'bbb'})

    def test_data_post(self):
        data = {'uri': 'abcd'}
        data_post(data)

        db.session.add(Data('abcd'))
        db.session.flush()
        with self.assertRaises(ProcessingException):
            data_post(data)


class RoutedTest(BaseTest):
    headers_json = {'Content-Type': 'application/json'}

    def http(self, func, endpoint, **kwargs):
        func = getattr(self.client, func)
        return func(endpoint, headers=self.headers_json, **kwargs)


class TestViews(RoutedTest):
    api_data_endpoint = '{0}/data'.format(API_ENDPOINT)
    api_ofs_endpoint = '{0}/ofs'.format(API_ENDPOINT)

    def test_data_post(self):
        data = {'uri': 'http://example.com', 'fname': 'testname'}
        response = self.http('post', self.api_data_endpoint, data=json.dumps(data))
        self.assert_status(response, 201, 'Failed to create data')
        data = {'uri': 'aaa'}
        response = self.http('post', self.api_data_endpoint, data=json.dumps(data))
        self.assert_status(response, 201, 'Failed to create data')
        data = {'uri': 'bbb', 'tags': []}
        response = self.http('post', self.api_data_endpoint, data=json.dumps(data))
        self.assert_status(response, 201, 'Failed to create data')
        data = {'uri': 'ccc', 'tags': [{'tag': 'ddd'}, {'tag': 'eee'}]}
        response = self.http('post', self.api_data_endpoint, data=json.dumps(data))
        self.assert_status(response, 201, 'Failed to create data')
        data = {'uri': 'ccc', 'tags': []}
        response = self.http('post', self.api_data_endpoint, data=json.dumps(data))
        self.assert_status(response, 409, 'Failed to identify conflict')

    def test_data_patch(self):
        data = {'uri': 'http://example.com'}
        response = self.http('post', self.api_data_endpoint, data=json.dumps(data))
        self.assert_status(response, 201, 'Failed to create data')
        tags = [{'tag': 'ddd'}, {'tag': 'eee'}]
        data = {'uri': 'http://example.com', 'tags': tags}
        uri = '{0}/{1}'.format(self.api_data_endpoint, response.json['id'])
        response = self.http('put', uri, data=json.dumps(data))
        self.assert_status(response, 200, 'Failed to edit data')
        self.assertEqual(response.json['tags'][0]['tag'], tags[0]['tag'])

        tags = [{'tag': 'eee'}]
        data = {'uri': 'http://example.com', 'tags': tags}
        uri = '{0}/{1}'.format(self.api_data_endpoint, response.json['id'])
        response = self.http('put', uri, data=json.dumps(data))
        self.assert_status(response, 200, 'Failed to edit data')
        self.assertEqual(response.json['tags'][0]['tag'], tags[0]['tag'])

    def test_data_query(self):
        filters = [dict(name='tags', op='any', val=dict(name='tag', op='eq', val='d'))]
        params = dict(q=json.dumps(dict(filters=filters)))
        response = self.http('get', self.api_data_endpoint, data=params)
        self.assert_200(response)

    def test_ofs_get(self):
        filecontents = 'btlex'
        aaa = StringIO(filecontents)
        fname = 'propername'
        resp = self.http('post', self.api_ofs_endpoint,
                         data={'blob': (aaa, fname)},
                         content_type='multipart/form-data')
        self.assert_200(resp)
        data = json.loads(resp.data)
        self.assertEqual(data['fname'], fname)

        path = urlsplit(data['uri']).path
        resp = self.http('get', path)
        self.assert_200(resp)
        self.assertEqual(resp.data, filecontents)

    def test_ofs_put(self):
        filecontents0 = 'btlex'
        filecontents1 = 'btlex'
        aaa = StringIO(filecontents0)
        fname = 'propername'
        resp = self.http('post', self.api_ofs_endpoint,
                         data={'blob': (aaa, fname)},
                         content_type='multipart/form-data')
        self.assert_200(resp)
        data = json.loads(resp.data)
        path = urlsplit(data['uri']).path

        aaa = StringIO(filecontents1)
        fname = 'propername'
        resp = self.http('put', path,
                         data={'blob': (aaa, fname)},
                         content_type='multipart/form-data')
        self.assert_200(resp)

        path = urlsplit(data['uri']).path
        resp = self.http('get', path)
        self.assert_200(resp)
        self.assertEqual(resp.data, filecontents1)

    def test_ofs_delete(self):
        filecontents0 = 'btlex'
        aaa = StringIO(filecontents0)
        fname = 'propername'
        resp = self.http('post', self.api_ofs_endpoint,
                         data={'blob': (aaa, fname)},
                         content_type='multipart/form-data')
        self.assert_200(resp)
        data = json.loads(resp.data)
        path = urlsplit(data['uri']).path
        resp = self.http('delete', path)
        self.assert_status(resp, 204)
        resp = self.http('get', path)
        self.assert_404(resp)

    def test_ofs_create(self):
        aaa = StringIO('btlex')
        injected_name = '", {"injected": "json"}'
        resp = self.http('post', self.api_ofs_endpoint,
                         data={'blob': (aaa, injected_name)},
                         content_type='multipart/form-data')
        self.assert_200(resp)
        data = json.loads(resp.data)
        self.assertEqual(data['fname'], '')

    def test_gc(self):
        faa = StringIO('aaa')
        fbb = StringIO('bbb')
        fcc = StringIO('ccc')

        resp = self.http('post', self.api_ofs_endpoint,
                         data={'blob': (faa, 'namea')},
                         content_type='multipart/form-data')
        dataa = json.loads(resp.data)
        daa = Data(dataa['uri'], 'namea')
        db.session.add(daa)
        db.session.flush()
        resp = self.http('post', self.api_ofs_endpoint,
                         data={'blob': (fbb, 'nameb')},
                         content_type='multipart/form-data')
        datab = json.loads(resp.data)
        resp = self.http('post', self.api_ofs_endpoint,
                         data={'blob': (fcc, 'namec')},
                         content_type='multipart/form-data')
        datac = json.loads(resp.data)

        dcclabel = os.path.basename(datac['uri'])
        olddate = (datetime.now() - timedelta(seconds=61)).strftime('%Y-%m-%dT%H:%M:%S')
        _, json_payload = ofs.ofs._get_object(ofs.BUCKET_LABEL)
        json_payload[dcclabel]['_last_modified'] = olddate
        json_payload.sync()

        self.assertEqual(len(ofs.call('list_labels')), 3)
        # If a blob is referenced, do not delete it
        # If a blob was created in the last minute, do not delete it, it may not
        # have been associated with its Data resource yet.
        # else, go ahead and remove the blob...
        gc_ofs()
        self.assertEqual(sorted(ofs.call('list_labels')),
                         sorted([os.path.basename(dataa['uri']),
                          os.path.basename(datab['uri'])]))


class TestClient(LiveServerTestCase):
    def create_app(self):
        app = _create_test_app(self)
        self.port = app.config['LIVESERVER_PORT']
        self.FQ_API_ENDPOINT = '{0}{1}'.format(self.get_server_url(),
                                               API_ENDPOINT)
        with app.app_context():
            db.create_all()
        return app

    def setUp(self):
        super(TestClient, self).setUp()
        try:
            rmtree(self.app.config['PTOFS_DIR'])
        except OSError:
            pass
        self.tstore = TagStoreClient(self.FQ_API_ENDPOINT)

    def tearDown(self):
        super(TestClient, self).tearDown()
        try:
            rmtree(self.app.config['PTOFS_DIR'])
        except OSError:
            pass
        with self.app.app_context():
            db.session.remove()
            db.drop_all()

    def test_create(self):
        uri = 'aaa'
        tags = [u'm', u'n']
        resp = self.tstore.create(uri, None, tags)
        self.assertEqual(resp.uri, uri)
        self.assertEqual(sorted(resp.tags), sorted(tags))

        uri = 'bbb'
        tags = [u'm', u'o']
        resp = self.tstore.create(uri, None, tags)
        self.assertEqual(resp.uri, uri)
        self.assertEqual(sorted(resp.tags), sorted(tags))

        resp = self.tstore.create('aaa', None, [u'm', u'n'])
        self.assertEqual(resp, None)

    def test_query_data(self):
        self.tstore.create('aaa', None, [u'm', u'n'])
        self.tstore.create('bbb', None, [u'm', u'o'])

        resp = self.tstore.query_data(Query.tags_any('eq', u'm'))
        self.assertEquals(len(resp), 2)

    def test_query_tags(self):
        self.tstore.create('aaa', None, [u'm', u'n:/asdf'])
        self.tstore.create('bbb', None, [u'm', u'n:/asdf/qwer'])

        resp = self.tstore.query_tags(['tag', 'eq', u'm'])
        self.assertEquals(len(resp), 1)

        resp = self.tstore.query_tags(['tag', 'like', u'n:/asdf%'])
        self.assertEquals(len(resp), 2)

    def test_edit(self):
        resp = self.tstore.create('aaa', None, [u'm', u'n'])

        d_id = resp.id

        resp = self.tstore.edit(d_id, 'aaa', '', [u'n'])
        self.assertEqual(resp.tags, [u'n'])

    def test_local_file(self):
        aaa = StringIO('btlex')
        self.tstore.create(aaa, None, [
            'cruise:1234', 'datatype:bottle', 'format:exchange', 'preliminary'])

        bbb = StringIO('ctdex')
        resp = self.tstore.create(bbb, 'bname', [
            'cruise:1234', 'datatype:ctd', 'format:exchange'])

        self.assertEqual(resp.fname, 'bname')

        ccc = StringIO('ctdzipnc')
        self.tstore.create(ccc, None, [
            'cruise:1234', 'datatype:ctd', 'format:zip.netcdf'])

        response = self.tstore.query_data(Query.tags_any('eq', 'format:exchange'))
        self.assertEqual(len(response), 2)

    def test_local_file_http(self):
        """HTTP headers should be set appropriately."""
        aaa = StringIO('btlex')
        resp = self.tstore.create(aaa, None, [])
        resp = requests.get(resp.uri)
        headers = resp.headers
        self.assertTrue(headers['content-disposition'].endswith('blob'))
        self.assertEqual(headers['content-length'], '5')

    def test_delete_local_file(self):
        ccc = StringIO('ctdzipnc')
        resp = self.tstore.create(ccc)
        d_id = resp.id
        self.tstore.delete(d_id)

    def test_query_response(self):
        for iii in range(20):
            self.tstore.create(u'test:{0}'.format(iii), None, [u'm'])
        resp = self.tstore.query_data(Query.tags_any('eq', u'm'))
        self.assertEqual(len(resp), 20)
        self.assertEqual(resp[15].uri, u'test:15')
        self.assertEqual(len(resp[9:15]), 6)
        self.assertEqual(resp[::-1][0].uri, u'test:19')
        with self.assertRaises(IndexError):
            resp[20]['uri']

        resp = self.tstore.query_data(Query.tags_any('eq', u'asdf'))
        self.assertEqual(len(resp), 0)
        resp = self.tstore.query_data(['uri', 'eq', u'test:19'], single=True)
        self.assertTrue(isinstance(resp, DataResponse))

        # More than one result
        with self.assertRaises(ValueError):
            self.tstore.query_data(Query.tags_any('eq', 'm'), single=True)

        resp = self.tstore.query_data(Query.tags_any('eq', 'm'), limit=1, single=True)
        self.assertTrue(isinstance(resp, DataResponse))

        resp = self.tstore.query_data(Query.tags_any('eq', u'asdf'), single=True)
        self.assertIsNone(resp)

    def test_data_response(self):
        """Reading from a Data pointing to a URL should make the request."""
        data = self.tstore.create(self.FQ_API_ENDPOINT + '/data')
        self.assertEqual(data.filename, 'data')
        self.assertEqual(data.open().read(1), '{')

        aaa = StringIO('hi')
        aaa.name = 'fname.txt'
        data = self.tstore.create(aaa)
        self.assertEqual(data.filename, aaa.name)
        self.assertEqual(data.open().read(2), 'hi')

    def test_edit_fname(self):
        aaa = StringIO('ctdzipnc')
        data = self.tstore.create(aaa, 'testname', ['tag0'])
        data = self.tstore.edit(data.id, data.uri, 'newname', data.tags)
        label = data.uri.split('/')[-1]
        with self.app.app_context():
            meta = ofs.call('get_metadata', label)
            self.assertEqual(meta['fname'], 'newname')

