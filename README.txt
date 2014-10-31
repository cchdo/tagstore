========
Tagstore
========

Tagstore provides storage and querying of URI and tag relationships. For
example, a URI "http://example.com" could be stored with multiple tags
"website:example" and "type:example". Tags are allowed to be any Unicode.

As a convenience, tagstore also provides storage of files through OFS's PTOFS.
This allows for indirectly tagging of files by storing first and tagging the
resulting URI.

As tags can be arbitrary, it is prudent to establish some external order before
using tagstore. The CCHDO's tagging conventions are laid out here:
https://docs.google.com/document/d/13u8qybFouIcR92vXm_OEgsP2DrMvf_nkJKYGmlE78V8/edit

