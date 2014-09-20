import json

from flask.ext.testing import TestCase

from flask.ext.restless import ProcessingException

from tagstore import app, replace_existing_tags, data_post
from tagstore.models import db, Tag, Data


class BaseTest(TestCase):
    def create_app(self):
        app.config['TESTING'] = True
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///'
        db.init_app(app)
        db.create_all(app=app)
        return app


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


class TestViews(BaseTest):
    api_data_endpoint = '/api/v1/data'
    headers_json = {'Content-Type': 'application/json'}
    def test_data_post(self):
        data = {'uri': 'http://example.com'}
        response = self.client.post(self.api_data_endpoint, data=json.dumps(data),
                                    headers=self.headers_json)
        self.assert_status(response, 201, 'Failed to create data')
        data = {'uri': 'aaa'}
        response = self.client.post(self.api_data_endpoint, data=json.dumps(data),
                                    headers=self.headers_json)
        self.assert_status(response, 201, 'Failed to create data')
        data = {'uri': 'bbb', 'tags': []}
        response = self.client.post(self.api_data_endpoint, data=json.dumps(data),
                                    headers=self.headers_json)
        self.assert_status(response, 201, 'Failed to create data')
        data = {'uri': 'ccc', 'tags': [{'tag': 'ddd'}, {'tag': 'eee'}]}
        response = self.client.post(self.api_data_endpoint, data=json.dumps(data),
                                    headers=self.headers_json)
        self.assert_status(response, 201, 'Failed to create data')
        data = {'uri': 'ccc', 'tags': []}
        response = self.client.post(self.api_data_endpoint, data=json.dumps(data),
                                    headers=self.headers_json)
        self.assert_status(response, 409, 'Failed to identify conflict')

    def test_data_patch(self):
        data = {'uri': 'http://example.com'}
        response = self.client.post(self.api_data_endpoint, data=json.dumps(data),
                                    headers=self.headers_json)
        self.assert_status(response, 201, 'Failed to create data')
        tags = [{'tag': 'ddd'}, {'tag': 'eee'}]
        data = {'uri': 'http://example.com', 'tags': tags}
        uri = '{0}/{1}'.format(self.api_data_endpoint, response.json['id'])
        response = self.client.put(uri, data=json.dumps(data),
                                   headers=self.headers_json)
        self.assert_status(response, 200, 'Failed to edit data')
        self.assertEqual(response.json['tags'][0]['tag'], tags[0]['tag'])

        tags = [{'tag': 'eee'}]
        data = {'uri': 'http://example.com', 'tags': tags}
        uri = '{0}/{1}'.format(self.api_data_endpoint, response.json['id'])
        response = self.client.put(uri, data=json.dumps(data),
                                   headers=self.headers_json)
        self.assert_status(response, 200, 'Failed to edit data')
        self.assertEqual(response.json['tags'][0]['tag'], tags[0]['tag'])

    def test_data_query(self):
        filters = [dict(name='tags', op='any', val=dict(name='tag', op='eq', val='d'))]
        params = dict(q=json.dumps(dict(filters=filters)))
        response = self.client.get(self.api_data_endpoint, data=params,
                                   headers=self.headers_json)
        self.assert_200(response)

