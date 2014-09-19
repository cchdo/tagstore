import requests

import json

url = 'http://127.0.0.1:5000/api/v1/data'
headers = {'Content-Type': 'application/json'}

data = dict(uri='a_uri', tags=[dict(tag='d'), dict(tag='g')])
response = requests.post(url, data=json.dumps(data), headers=headers)
assert response.status_code == 201 or response.status_code == 409
if response.status_code == 201:
    print(response.json())

data = dict(uri='b_uri', tags=[dict(tag='d'), dict(tag='e')])
response = requests.post(url, data=json.dumps(data), headers=headers)
assert response.status_code == 201 or response.status_code == 409
if response.status_code == 201:
    print(response.json())

filters = [dict(name='tags', op='any', val=dict(name='tag', op='eq', val='d'))]
params = dict(q=json.dumps(dict(filters=filters)))

response = requests.get(
    url, params=params, headers=headers)
assert response.status_code == 200
print(response.json())

#filters = [dict(name='name', op='like', val='%y%')]
