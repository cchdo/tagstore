from uuid import uuid4
import os.path
import logging

log = logging.getLogger(__name__)

from flask import Flask, jsonify, abort, request, send_file, make_response
from flask.ext.restless import APIManager, ProcessingException

from ofs.local import PTOFS

from models import db, Tag, Data


app = Flask(__name__)


# 2-char bucket label for shallower pairtree
BUCKET_LABEL = u'ts'
OFS_SCHEME = u'ofs'

ofs = PTOFS(storage_dir='tagstore-data', uri_base='urn:uuid:',
            hashing_type='sha256')
if BUCKET_LABEL not in ofs.list_buckets():
    BUCKET_ID = ofs.claim_bucket(BUCKET_LABEL)
else:
    BUCKET_ID = BUCKET_LABEL


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


def is_uri_present_for_data(data):
    try:
        uri = data['uri']
    except KeyError:
        pass
    else:
        old = Data.query.filter_by(uri=uri).first()
        if old:
            return True
    return False


def data_post(data=None, **kw):
    if is_uri_present_for_data(data):
        raise ProcessingException(description='Already present', code=409)

    replace_existing_tags(data)


manager.create_api(Data, url_prefix=api_v1_prefix,
                   preprocessors={
                       'PATCH_SINGLE': [data_patch_single],
                       'POST': [data_post],
                   },
                   methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])


manager.create_api(Tag, url_prefix=api_v1_prefix,
                   methods=['GET'],
                   exclude_columns=['id', 'data'],
                   collection_name='tags')


@app.route('{0}/ofs'.format(api_v1_prefix), methods=['POST'])
def ofs_create():
    fobj = request.files['blob']
    label = str(uuid4())
    ofs.put_stream(BUCKET_ID, label, fobj)
    fname = fobj.filename
    ofs.update_metadata(BUCKET_ID, label, {'fname': fname})
    return jsonify(dict(uri='{0}/{1}'.format(request.url, label),
                        fname=fname))


@app.route('{0}/ofs/<label>'.format(api_v1_prefix),
           methods=['HEAD', 'GET', 'PUT', 'DELETE'])
def ofs_get(label):
    if request.method == 'HEAD':
        try:
            fname = ofs.get_metadata(BUCKET_ID, label)['fname']
        except KeyError:
            fname = ''
        return make_response('', 200, {'content-disposition': fname})
    elif request.method == 'GET':
        try:
            stream = ofs.get_stream(BUCKET_ID, label)
            fname = ofs.get_metadata(BUCKET_ID, label)['fname']
            # Flask converts the filename to an absolute path by prepending the
            # app directory which is incorrect. This is only used to add etags,
            # so just turn that off.
            return send_file(stream, attachment_filename=fname, add_etags=False)
        except Exception as err:
            log.error(u'Local blob is missing for label {0}'.format(label))
            abort(404)
    elif request.method == 'PUT':
        fobj = request.files['blob']
        ofs.put_stream(BUCKET_ID, label, fobj)
        return make_response('', 200)
    elif request.method == 'DELETE':
        ofs.del_stream(BUCKET_ID, label)
        return make_response('', 204)


if __name__ == "__main__":
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/tagstore.db'
    db.init_app(app)
    db.create_all(app=app)
    app.run('0.0.0.0', debug=True)
