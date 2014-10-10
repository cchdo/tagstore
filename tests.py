import json
import types
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
from tagstore import server
from tagstore.server import ofs
from tagstore.client import TagStoreClient, Query, DataResponse
from tagstore.models import db, Tag, Data


API_ENDPOINT = '/api/v1'


def _create_test_app(self):
    app = Flask(__name__)
    app.config.from_object('tagstore.settings.default')
    app.config.from_object('tagstore.settings.test')
    server.init_app(app)
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
        server.replace_existing_tags(data)
        self.assertTrue(data['tags'] == [])

        data = {'tags': [{'tag': 'aaa'}]}
        server.replace_existing_tags(data)
        self.assertTrue(data['tags'][0] == {'tag': 'aaa'})

        db.session.add(Tag('aaa'))
        db.session.flush()
        data = {'tags': [{'tag': 'aaa'}]}
        server.replace_existing_tags(data)
        tag = Tag.query.filter_by(tag='aaa').first()
        self.assertTrue(data['tags'][0] == {'id': tag.id})

        data = {'tags': [{'tag': 'aaa'}, {'tag': 'bbb'}]}
        server.replace_existing_tags(data)
        tag = Tag.query.filter_by(tag='aaa').first()
        self.assertTrue(data['tags'][0] == {'id': tag.id})
        self.assertTrue(data['tags'][1] == {'tag': 'bbb'})

    def test_data_post(self):
        data = {'uri': 'abcd'}
        server.data_post(data)

        db.session.add(Data('abcd'))
        db.session.flush()
        with self.assertRaises(ProcessingException):
            server.data_post(data)

    def test_update_http_headers(self):
        headers = {}
        metadata = {}
        server._update_http_headers(headers, metadata, as_attachment=True)
        self.assertEqual(headers['Content-Disposition'], 'attachment; filename=')
        self.assertEqual(headers['Content-Type'], 'application/octet-stream')
        metadata['fname'] = 'test.txt'
        server._update_http_headers(headers, metadata)
        self.assertEqual(headers['Content-Disposition'],
                         'inline; filename=test.txt')
        self.assertEqual(headers['Content-Type'], 'text/plain')

    def test_zip_load(self):
        zfile = server._zip_load([], '')
        self.assertTrue(isinstance(zfile, types.GeneratorType))
        contents = zfile.next()
        self.assertEqual(len(contents), 22)
        self.assertTrue(contents[:2], 'PK')

        data = 'http://999.0.0.0'
        ddd = Data(data, 'broken')
        arcname = 'namea'
        zfile = server._zip_load([(ddd, arcname)], 'ofs')
        contents = zfile.next()
        self.assertEqual(len(contents), 22)

    def test_zip_max_size(self):
        self.assertEqual(server._zip_max_size([], ''), 22)

        data = 'data:text/html,'
        ddd = Data(data, 'nameb')
        arcname = 'namea'
        self.assertEqual(server._zip_max_size([(ddd, arcname)], 'ofs'),
                         22 + 88 + (len(arcname) + 1) * 2)


class RoutedTest(BaseTest):
    headers_json = {'Content-Type': 'application/json'}

    def http(self, func, endpoint, **kwargs):
        try:
            headers = kwargs['headers']
        except KeyError:
            headers = self.headers_json
        else:
            headers.update(self.headers_json)
            del kwargs['headers']
        func = getattr(self.client, func)
        return func(endpoint, headers=headers, **kwargs)


class TestViews(RoutedTest):
    api_data_endpoint = '{0}/data'.format(API_ENDPOINT)
    api_ofs_endpoint = '{0}/ofs'.format(API_ENDPOINT)
    api_zip_endpoint = '{0}/zip'.format(API_ENDPOINT)

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

        resp = self.http('head', path)
        self.assertEqual(resp.headers['content-length'], str(5))

        resp = self.http('get', path, headers={'X-As-Attachment': 'yes'})
        self.assertTrue(
            resp.headers['content-disposition'].startswith('attachment'))

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
        server.gc_ofs()
        self.assertEqual(sorted(ofs.call('list_labels')),
                         sorted([os.path.basename(dataa['uri']),
                          os.path.basename(datab['uri'])]))

    def test_zip(self):
        faa = StringIO('aaa')
        resp = self.http('post', self.api_ofs_endpoint,
                         data={'blob': (faa, 'namea')},
                         content_type='multipart/form-data')
        dataa = json.loads(resp.data)
        daa = Data(dataa['uri'], 'namea')
        db.session.add(daa)
        db.session.flush()
        dbb = Data('data:text/html,', 'nameb')
        db.session.add(dbb)
        db.session.flush()

        data_arcnames = [(daa.id, 'namea'), (dbb.id, 'namea/nameb')]
        fname = 'test.zip'
        data = dict(data_arcnames=data_arcnames,
                    ofs_endpoint=self.api_ofs_endpoint, fname=fname)
        resp = self.http('post', self.api_zip_endpoint, data=json.dumps(data))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.headers['Content-Type'], 'application/zip')
        self.assertEqual(resp.headers['Content-Disposition'],
                         'attachment; filename={0}'.format(fname))


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

    def test_delete_tag(self):
        resp = self.tstore.create('aaa', 'aname', ['taga'])
        tag = self.tstore.query_tags(['tag', 'eq', 'taga'])[0]
        t_id = tag.id
        with self.assertRaises(ValueError):
            self.tstore.delete_tag(t_id)
        self.tstore.edit(resp.id, 'aaa', 'aname', [])
        self.tstore.delete_tag(t_id)
        self.assertEqual(self.tstore.query_data(['fname', 'eq',
                                                 'aname'])[0].tags, [])

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

    def test_edit_tag(self):
        data = self.tstore.create('uri', 'fname', ['oldtag'])
        tag = self.tstore.query_tags(
            ['tag', 'eq', 'oldtag'], limit=1, single=True)
        tag = self.tstore.edit_tag(tag.id, 'newtag')
        self.assertEqual(tag.tag, 'newtag')

    def test_edit_tag_already_present(self):
        data = self.tstore.create('uri', 'fname', ['oldtag1', 'oldtag2'])
        tag = self.tstore.query_tags(
            ['tag', 'eq', 'oldtag1'], limit=1, single=True)
        tag = self.tstore.edit_tag(tag.id, 'oldtag2')
        self.assertEqual(tag.tag, 'oldtag2')
        tags = self.tstore.query_tags(['tag', 'eq', 'oldtag2'])
        self.assertEqual(tags[0].tag, 'oldtag2')
        tags = self.tstore.query_tags()
        self.assertEqual(len(tags), 1)

    def test_swap_tags(self):
        data = self.tstore.create('uri0', 'fname', ['oldtag1'])
        data = self.tstore.create('uri1', 'fname', ['oldtag2', 'oldtag1'])
        self.tstore.swap_tags('oldtag1', 'oldtag2')

        for data in self.tstore.query_data():
            self.assertEqual(data.tags, ['oldtag2'])

        self.tstore.swap_tags('oldtag2', 'oldtag1')

        for data in self.tstore.query_data():
            self.assertEqual(data.tags, ['oldtag1'])
