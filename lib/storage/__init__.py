
import contextlib
import tempfile

import config


__all__ = ['load']


class Storage(object):

    """Storage is organized as follow:
    $ROOT/images/<image_id>/json
    $ROOT/images/<image_id>/layer
    $ROOT/repositories/<namespace>/<repository_name>/<tag_name>
    """

    # Useful if we want to change those locations later without rewriting
    # the code which uses Storage
    repositories = 'repositories'
    images = 'images'
    # Set the IO buffer to 64kB
    buffer_size = 64 * 1024

    #FIXME(samalba): Move all path resolver in each module (out of the base)
    def images_list_path(self, namespace, repository):
        return '{0}/{1}/{2}/_images_list'.format(self.repositories,
                                                 namespace,
                                                 repository)

    def image_json_path(self, image_id):
        return '{0}/{1}/json'.format(self.images, image_id)

    def image_mark_path(self, image_id):
        return '{0}/{1}/_inprogress'.format(self.images, image_id)

    def image_checksum_path(self, image_id):
        return '{0}/{1}/_checksum'.format(self.images, image_id)

    def image_layer_path(self, image_id):
        return '{0}/{1}/layer'.format(self.images, image_id)

    def image_ancestry_path(self, image_id):
        return '{0}/{1}/ancestry'.format(self.images, image_id)

    def tag_path(self, namespace, repository, tagname=None):
        if not tagname:
            return '{0}/{1}/{2}'.format(self.repositories,
                                        namespace,
                                        repository)
        return '{0}/{1}/{2}/tag_{3}'.format(self.repositories,
                                            namespace,
                                            repository,
                                            tagname)

    def index_images_path(self, namespace, repository):
        return '{0}/{1}/{2}/_index_images'.format(self.repositories,
                                                  namespace,
                                                  repository)

    def get_content(self, path):
        raise NotImplementedError

    def put_content(self, path, content):
        raise NotImplementedError

    def stream_read(self, path):
        raise NotImplementedError

    def stream_write(self, path, fp):
        raise NotImplementedError

    def list_directory(self, path=None):
        raise NotImplementedError

    def exists(self, path):
        raise NotImplementedError

    def remove(self, path):
        raise NotImplementedError

    def get_size(self, path):
        raise NotImplementedError


@contextlib.contextmanager
def store_stream(stream):
    """Stores the entire stream to a temporary file."""
    tmpf = tempfile.TemporaryFile()
    while True:
        try:
            buf = stream.read(4096)
            if not buf:
                break
            tmpf.write(buf)
        except IOError:
            break
    tmpf.seek(0)
    yield tmpf
    tmpf.close()


def temp_store_handler():
    tmpf = tempfile.TemporaryFile()

    def fn(buf):
        try:
            tmpf.write(buf)
        except IOError:
            pass

    return tmpf, fn


from glance import GlanceStorage
from local import LocalStorage
from s3 import S3Storage
from swift import SwiftStorage

_storage = {}


def load(kind=None):
    """Returns the right storage class according to the configuration."""
    global _storage
    cfg = config.load()
    if not kind:
        kind = cfg.storage.lower()
    if kind in _storage:
        return _storage[kind]
    if kind == 's3':
        store = S3Storage(cfg)
    elif kind == 'local':
        store = LocalStorage(cfg)
    elif kind == 'glance':
        store = GlanceStorage(cfg)
    elif kind == 'swift':
        store = SwiftStorage(cfg)
    else:
        raise ValueError('Not supported storage \'{0}\''.format(kind))
    _storage[kind] = store
    return store
