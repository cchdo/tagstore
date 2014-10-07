from uuid import uuid4
import os.path
import logging
from datetime import datetime, timedelta
from mimetypes import guess_type

log = logging.getLogger(__name__)

from flask import (
    Flask, g, Blueprint, current_app, jsonify, abort, request, send_file,
    make_response
)
from flask.ext.restless import APIManager, ProcessingException

from werkzeug.local import LocalProxy

from ofs.local import PTOFS

from models import db, Tag, Data, tags


class OFSWrapper(object):
    # 2-char bucket label for shallower pairtree
    BUCKET_LABEL = u'ts'

    def __init__(self, **kwargs):
        self.init(**kwargs)

    def init(self, **kwargs):
        self.ofs = PTOFS(uri_base='urn:uuid:', hashing_type='sha256', **kwargs)
        if self.BUCKET_LABEL not in self.ofs.list_buckets():
            self.bucket_id = self.ofs.claim_bucket(self.BUCKET_LABEL)
        else:
            self.bucket_id = self.BUCKET_LABEL

    def call(self, method, *args, **kwargs):
        return getattr(self.ofs, method)(self.bucket_id, *args, **kwargs)


def get_ofs():
    ofs = getattr(g, '_ofs', None)
    if ofs is None:
        ofs = g._ofs = OFSWrapper(storage_dir=current_app.config['PTOFS_DIR'])
    return ofs


ofs = LocalProxy(get_ofs)


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
        try:
            old = Data.query.filter_by(uri=uri).first()
        except Exception as err:
            log.error(err)
            raise
        if old:
            return True
    return False


def data_post(data=None, **kw):
    if is_uri_present_for_data(data):
        raise ProcessingException(description='Already present', code=409)

    replace_existing_tags(data)


class TagPatchSingle(object):
    new_tag = None

    @classmethod
    def pre(cls, instance_id=None, data=None, **kw):
        tag = Tag.query.filter_by(tag=data['tag']).first()
        if tag:
            # The "new" tag is really replacing with a preexisting tag.
            tags.update().where(
                tags.c.tag_id == instance_id).values(tag_id=tag.id)
            data['tag'] = 'newtag{0}'.format(tag.id)
            cls.new_tag = tag.id

    @classmethod
    def post(cls, result=None, **kw):
        if cls.new_tag != None:
            tag = Tag.query.get(result['id'])
            db.session.delete(tag)
            db.session.commit()
            newtag = Tag.query.get(cls.new_tag)
            result['id'] = newtag.id
            result['tag'] = newtag.tag
            cls.new_tag = None


def tag_delete(instance_id=None, **kw):
    any_referencing = db.session.query(tags.c.tag_id).filter(
        tags.c.tag_id == instance_id).count()
    if any_referencing:
        raise ProcessingException(description='Tag is referenced', code=409)


store_blueprint = Blueprint('storage', __name__, )


@store_blueprint.route('{0}/ofs'.format(api_v1_prefix), methods=['POST'])
def ofs_create():
    fobj = request.files['blob']
    label = str(uuid4())
    ofs.call('put_stream', label, fobj)
    fname = fobj.filename
    ofs.call('update_metadata', label, {'fname': fname})
    return jsonify(dict(uri='{0}/{1}'.format(request.url, label), fname=fname))


def _update_http_headers(headers, metadata, as_attachment=False):
    fname = metadata.get('fname', '')
    disposition = 'inline'
    if as_attachment:
        disposition = 'attachment'
    headers['Content-Disposition'] = '{0}; filename={1}'.format(
        disposition, fname)
    mtype = guess_type(fname)[0]
    if not mtype:
        mtype = 'application/octet-stream'
    headers['Content-Type'] = metadata.get('_format', mtype)
    try:
        headers['Content-Length'] = metadata['_content_length']
    except KeyError:
        pass


@store_blueprint.route('{0}/ofs/<label>'.format(api_v1_prefix),
           methods=['HEAD', 'GET', 'PUT', 'DELETE'])
def ofs_get(label):
    as_attachment = request.headers.get('X-As-Attachment', 'no') == 'yes'
    if request.method == 'HEAD':
        metadata = ofs.call('get_metadata', label)
        headers = {}
        _update_http_headers(headers, metadata, as_attachment)
        return make_response('', 200, headers)
    elif request.method == 'GET':
        try:
            stream = ofs.call('get_stream', label)
        except Exception as err:
            log.error(u'Local blob is missing for label {0}: {1!r}'.format(
                label, err))
            abort(404)
        else:
            metadata = ofs.call('get_metadata', label)
            # Flask converts the filename to an absolute path by prepending the
            # app directory which is incorrect. This is only used to add etags,
            # so just turn that off.
            resp = send_file(stream, add_etags=False)
            _update_http_headers(resp.headers, metadata, as_attachment)
            return resp
    elif request.method == 'PUT':
        try:
            fname = request.form['fname']
        except KeyError:
            pass
        else:
            params = {'fname': fname}
            ofs.call('update_metadata', label, params)

        if request.files:
            fobj = request.files['blob']
            ofs.call('put_stream', label, fobj)
        return make_response('', 200)
    elif request.method == 'DELETE':
        try:
            ofs.call('del_stream', label)
        except Exception:
            pass
        return make_response('', 204)


def gc_ofs():
    local_data = Data.query.filter(Data.uri.like('%/api/%/ofs/%')).all()
    present_labels = set([os.path.basename(ddd.uri) for ddd in local_data])
    for label in ofs.call('list_labels'):
        meta = ofs.call('get_metadata', label)
        mtime = datetime.strptime(meta['_last_modified'], '%Y-%m-%dT%H:%M:%S')
        grace_time = datetime.now() - timedelta(seconds=60)
        # Still within the grace period
        if mtime >= grace_time:
            continue
        if label in present_labels:
            continue
        ofs.call('del_stream', label)


def init_app(app):
    with app.app_context():
        db.init_app(app)

    app.register_blueprint(store_blueprint)

    manager = APIManager(app, flask_sqlalchemy_db=db)
    manager.create_api(Data, url_prefix=api_v1_prefix,
                       preprocessors={
                           'PATCH_SINGLE': [data_patch_single],
                           'POST': [data_post],
                       },
                       methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])
    manager.create_api(Tag, url_prefix=api_v1_prefix,
                       preprocessors={
                           'PATCH_SINGLE': [TagPatchSingle.pre],
                           'DELETE': [tag_delete],
                       },
                       postprocessors={
                           'PATCH_SINGLE': [TagPatchSingle.post],
                       },
                       methods=['GET', 'PUT', 'PATCH', 'DELETE'],
                       include_columns=['id', 'tag'],
                       collection_name='tags')


if __name__ == "__main__":
    import sys
    app = Flask(__name__)
    app.config.from_object('tagstore.settings.default')
    try:
        app.config.from_pyfile(sys.argv[1])
    except IndexError:
        raise IndexError(u'Please supply a configuration file.')
    init_app(app)
    app.run('0.0.0.0')
