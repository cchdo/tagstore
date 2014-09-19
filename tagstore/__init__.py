from flask import Flask, jsonify, abort, request


from models import db, Tag


app = Flask(__name__)


@app.route("/")
def home():
    return "tag store"


@app.route("/tags")
def tags():
    tags = Tag.query.all()
    return jsonify(tags=[ttt.tag for ttt in tags])


@app.route("/tag/<tag>")
def tag(tag):
    tag = Tag.query.filter_by(tag=tag).first()
    if not tag:
        abort(404)
    # TODO return the right thing
    return jsonify(tag.data)

# TODO get all data that match some tags


@app.route("/data", methods=['GET', 'POST'])
def data():
    if request.method == 'POST':
        uri = request.form['uri']
        tags = request.form['tags']

        data = Data(uri)
        data.tags = tags.split(',')

        # TODO store this data!

        return jsonify()


if __name__ == "__main__":
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/tagstore.db'
    db.init_app(app)
    db.create_all(app=app)
    app.run('0.0.0.0', debug=True)
