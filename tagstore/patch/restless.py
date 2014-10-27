"""Add operators to restless"""
from flask.ext.restless import search


search.OPERATORS['not_any'] = lambda f, a, fn: ~f.any(search._sub_operator(f, a, fn))
search.OPERATORS['not_ilike'] = lambda f, a: ~f.ilike(a)
search.OPERATORS['not_like'] = lambda f, a: ~f.like(a)
