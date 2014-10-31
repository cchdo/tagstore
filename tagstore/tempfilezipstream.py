from contextlib import contextmanager, closing
from tempfile import NamedTemporaryFile
from shutil import copyfileobj

from zipstream import ZipFile, ZIP_DEFLATED


class FileWrapper(object):
    @property
    def arcname(self):
        raise NotImplementedError()
        return None

    def get_stream(self):
        raise NotImplementedError()
        return None

    def get_size(self):
        raise NotImplementedError()
        return 0


class TempFileStreamingZipFile(ZipFile):
    """Temporarily caches the files that will be streamed.

    """
    def __init__(self, wrappers, mode='w', allowZip64=True, *args, **kwargs):
        super(TempFileStreamingZipFile, self).__init__(*args, **kwargs)
        self.wrappers = wrappers
        self._tfiles = []

    def max_size(self):
        # 22 is the minimum size of a Zip EOCD
        max_size = 22
        for wrapper in self.wrappers:
            content_len = len(wrapper)

            # https://en.wikipedia.org/wiki/Zip_(file_format)#File_headers
            # 88 = local file header (30) + central directory file header (46) +
            # possible data descriptor (12) + 
            # (arcname + null byte string terminator)
            # x2 once for the local header, once for the central directory
            max_size += 88 + (len(wrapper.arcname) + 1) * 2
            max_size += content_len
        return max_size

    def _load(self):
        for wrapper in self.wrappers:
            if wrapper.arcname is None:
                continue

            zip_opts = {}
            if wrapper.arcname.endswith(".zip"):
                zip_opts['compress_type'] = ZIP_DEFLATED

            stream = wrapper.get_stream()
            if not stream:
                continue

            tfile = NamedTemporaryFile()
            self._tfiles.append(tfile)
            with closing(stream) as fobj:
                copyfileobj(fobj, tfile)
            tfile.seek(0)
            self.write(tfile.name, arcname=wrapper.arcname, **zip_opts)

    def __iter__(self):
        self._load()
        try:
            for chunk in super(TempFileStreamingZipFile, self).__iter__():
                yield chunk
        finally:
            self.close()

    def __close(self):
        for data in super(TempFileStreamingZipFile, self).__close():
            yield data
        for tfile in self._tfiles:
            tfile.close()
