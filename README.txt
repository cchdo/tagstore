========
Tagstore
========

Summary
-------------

Tagstore provides storage and querying of URI and tag relationships. For
example, a URI "http://example.com" could be stored with multiple tags
"website:example" and "type:example". Tags are allowed to be any Unicode.

Motivation
--------------

Organization and presentation of data is fundamental to making it useful. Storage is critical, but secondary. Fortunately, URIs refer to data on a network, precluding the need to store data locally. We often organize data in hierarchies, for example, filesystems might organize data by ocean, then year, then cruise, as directories. Tags provide a flexible organization method that allow for different views to be created based on tag values. Perhaps organizing data by the instrument used to collect it or by the time it was collected is more important than ocean first. One can continue to provide a filesystem-like view by using tags with paths as their values. Additional tags can allow for different views of the stored data.

Object storage
--------------------

As a convenience, tagstore also provides storage of files through OFS's PTOFS.
This allows for indirectly tagging of files by storing first and tagging the
resulting URI.

Tag conventions
----------------------

As tags can be arbitrary, it is prudent to establish some external order before
using tagstore. The CCHDO's tagging conventions are laid out here:
https://docs.google.com/document/d/13u8qybFouIcR92vXm_OEgsP2DrMvf_nkJKYGmlE78V8/edit

API
-----

``GET /data``

``GET /tags``

``GET /ofs``

``POST /ofs``

Details
---------

Tags are stored in a database where URIs have a many-to-many relationship with tags.
Object storage is provided by a client that is aware of certain URIs being stored by tagstore. It writes the file to the OFS, then stores the available URI in tagstore.
