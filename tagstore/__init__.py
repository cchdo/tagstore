from flask import Flask, jsonify, abort, request

from flask.ext.restless import APIManager, ProcessingException


from models import db, Tag, Data


app = Flask(__name__)


manager = APIManager(app, flask_sqlalchemy_db=db)


api_v1_prefix = '/api/v1'


def replace_existing_tags(data):
    """Replace any existing tags with the tag id."""
    try:
        tags = data['tags']
        new_tags = [tag['tag'] for tag in tags]
        old_tags = Tag.query.filter(Tag.tag.in_(new_tags)).all()
        old_tag_ids = {}
        for tag in old_tags:
            old_tag_ids[tag.tag] = tag.id
        for tag in tags:
            ttt = tag['tag']
            if ttt in old_tag_ids:
                del tag['tag']
                tag['id'] = old_tag_ids[ttt]
    except KeyError:
        pass


def data_patch_single(instance_id=None, data=None, **kw):
    replace_existing_tags(data)


def data_post(data=None, **kw):
    try:
        uri = data['uri']
        old = Data.query.filter_by(uri=uri).first()
        if old:
            raise ProcessingException(
                    description='Already present', code=409)
    except KeyError:
        pass
    replace_existing_tags(data)


manager.create_api(Data, url_prefix=api_v1_prefix,
                   preprocessors={
                       'PATCH_SINGLE': [data_patch_single],
                       'POST': [data_post],
                   },
                   methods=['GET', 'POST', 'DELETE'])


manager.create_api(Tag, url_prefix=api_v1_prefix,
                   methods=['GET'],
                   exclude_columns=['id'],
                   collection_name='tags')


if __name__ == "__main__":
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/tagstore.db'
    db.init_app(app)
    db.create_all(app=app)
    app.run('0.0.0.0', debug=True)
