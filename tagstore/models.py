from flask.ext.sqlalchemy import SQLAlchemy

from sqlalchemy.ext.associationproxy import association_proxy


db = SQLAlchemy()
# NOTE: SQLite performance is surprisingly slow.


tags = db.Table('tags',
    db.Column('tag_id', db.Integer, db.ForeignKey('tag.id')),
    db.Column('data_id', db.Integer, db.ForeignKey('data.id'))
)


class Data(db.Model):
    id = db.Column(db.Integer, primary_key=True)

    # http://stackoverflow.com/questions/2659952
    uri = db.Column(db.Unicode(2**11), unique=True)

    fname = db.Column(db.Unicode(255))

    tags = db.relationship('Tag', secondary=tags,
        backref=db.backref('data', lazy='dynamic'))

    def __init__(self, uri, fname=None):
        self.uri = uri
        self.fname = fname

    def __repr__(self):
        return u'<Data {0!r}>'.format(self.uri)


class Tag(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tag = db.Column(db.Unicode(2**9), unique=True)

    def __init__(self, tag):
        self.tag = tag

    def __repr__(self):
        return u'<Tag {0!r}>'.format(self.tag)
