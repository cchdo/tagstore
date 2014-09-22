import json
from StringIO import StringIO
import logging


log = logging.getLogger(__name__)


from flask.ext.testing import TestCase, LiveServerTestCase

from flask.ext.restless import ProcessingException

from tagstore import app, replace_existing_tags, data_post
from tagstore.client import TagStoreClient
from tagstore.models import db, Tag, Data


def _create_app(self):
    app.config['TESTING'] = True
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///'
    db.init_app(app)
    db.create_all(app=app)
    return app



class BaseTest(TestCase):
    create_app = _create_app


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
    api_data_endpoint = '/api/v1/data'

    def test_data_post(self):
        data = {'uri': 'http://example.com'}
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


class TestClient(LiveServerTestCase):
    def create_app(self):
        app = _create_app(self)
        self.port = 8943
        app.config['LIVESERVER_PORT'] = self.port
        self.tstore = TagStoreClient('{0}/api/v1'.format(self.get_server_url()))
        return app

    def test_create(self):
        resp = self.tstore.create('aaa', [u'm', u'n'])
        self.assertEqual(resp.status_code, 201)

        resp = self.tstore.create('bbb', [u'm', u'o'])
        self.assertEqual(resp.status_code, 201)

        resp = self.tstore.create('aaa', [u'm', u'n'])
        self.assertEqual(resp.status_code, 409)

    def test_query(self):
        resp = self.tstore.create('aaa', [u'm', u'n'])
        self.assertEqual(resp.status_code, 201)

        resp = self.tstore.create('bbb', [u'm', u'o'])
        self.assertEqual(resp.status_code, 201)

        resp = self.tstore.query(['tags', 'any', ['tag', 'eq', u'm']])
        result = resp.json()
        self.assertEquals(result['num_results'], 2)

    def test_edit(self):
        resp = self.tstore.create('aaa', [u'm', u'n'])
        self.assertEqual(resp.status_code, 201)

        d_id = resp.json()['id']

        resp = self.tstore.edit(d_id, 'aaa', [u'n'])
        self.assertEqual(resp.status_code, 200)
        tags = [tag['tag'] for tag in resp.json()['tags']]
        self.assertEqual(tags, [u'n'])

    def test_local_file(self):
        aaa = StringIO('btlex')
        response = self.tstore.create(aaa, [
            'cruise:1234', 'datatype:bottle', 'format:exchange', 'preliminary'])

        bbb = StringIO('ctdex')
        response = self.tstore.create(bbb, [
            'cruise:1234', 'datatype:ctd', 'format:exchange'])

        ccc = StringIO('ctdzipnc')
        response = self.tstore.create(ccc, [
            'cruise:1234', 'datatype:ctd', 'format:zip.netcdf'])

        response = self.tstore.query(
            ['tags', 'any', ['tag', 'eq', 'format:exchange']])
        self.assertEqual(response.json()['num_results'], 2)

    def test_delete_local_file(self):
        ccc = StringIO('ctdzipnc')
        resp = self.tstore.create(ccc, [])
        d_id = resp.json()['id']

        self.tstore.delete(d_id)
