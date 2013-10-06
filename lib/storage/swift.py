from tempfile import *
import os
import shutil
from swiftclient import Connection
from swiftclient import ClientException
import logging
from uuid import *
from datetime import datetime
from . import Storage

logger = logging.getLogger(__name__)

class SwiftStorage(Storage):

    def __init__(self, config):
        self._config = config
        self._root_path = self._config.storage_path
        self._connection = Connection(self._config.storage_auth_url, self._config.storage_account + ':' + self._config.storage_username, self._config.storage_password)
        self._container = self._config.storage_container
        self._index_lastupdate = None
    
    def _update_index(self):
        logger.info("_update_index")
        try:
            headers = self._connection.head_object(self._container, 'docker-registry.index')  
            lastupdatestr = headers['last-modified']
            if not lastupdatestr:
                return
            lastupdate = datetime.strptime(lastupdatestr, "%a, %d %b %Y %H:%M:%S %Z")
            logger.info('lastupdate: ' + str(lastupdate))
            refresh_index = False
            
            if not self._index_lastupdate:
                refresh_index = True
            elif self._index_lastupdate < lastupdate:
                refresh_index = True
            
            if not refresh_index:
                return
            logger.info("refresh index...")
            
            (headers, data) = self._connection.get_object(self._container, 'docker-registry.index')
            (level, temppath) = mkstemp()
            tempfile = open(temppath, 'w')
            tempfile.write(data)
            tempfile.close()
            
            with open(temppath, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("#"):
                        continue
                    tokens = line.split('\t')
                    if len(tokens) < 2:
                        continue
                    logger.info("Index: " + str(tokens))
                    realpath = os.path.join(self._root_path, tokens[0])
                    if not os.path.exists(realpath):
                        os.makedirs(realpath)
                    metafile = open(os.path.join(realpath, '.object'), 'w')
                    metafile.write(tokens[1])
                    metafile.close()
            
            os.remove(temppath)

            self._index_lastupdate = lastupdate
        except ClientException, inst:
            if inst.http_status != 404:
                raise inst
    
    def _load_object(self, path):
        if not os.path.exists(path):
            return None
        logger.info("_load_object: " + path)
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("#"):
                    continue
                return line
        return None
    
    def _create_object(self, path, uuidfile):
        logger.info("_create_object: " + path)
        (level, temppath) = mkstemp()
        tempfile = open(temppath, 'w')
        
        try:
            (headers, data) = self._connection.get_object(self._container, 'docker-registry.index')
            tempfile.write(data)
        except ClientException, inst:
            if inst.http_status != 404:
                tempfile.close()
                os.remove(temppath)
                raise inst

        uuid = uuid4()
        logger.info(path + ' = ' + uuid.hex)
        tempfile.write(path + '\t' + uuid.hex + '\n');
        tempfile.close()

        tempfile = open(temppath, 'r')
        try:
            self._connection.put_object(self._container, 'docker-registry.index', tempfile, chunk_size=4096)  
        except ClientException, inst:
            tempfile.close()
            os.remove(temppath)
            raise inst
        tempfile.close()
        os.remove(temppath)
        
        metafile = open(uuidfile, 'w')
        metafile.write(uuid.hex)
        metafile.close()
        return uuid.hex

    def _is_local_path(self, path):
        if not path:
            return False
        return path.endswith('/_inprogress')

    def _init_path(self, path=None, create=False):
	if not path:
            return (self._root_path, None)
        sep = path.rfind('/')
        object_name = None
        object_prefix = None
        basedirname = None
        if sep < 0:
            # directory
            basedirname = path
            path = os.path.join(self._root_path, path)
        else:
            # object
            object_name = path[(sep + 1):len(path)]
            basedirname = path[0:sep]
            path = os.path.join(self._root_path, path)
        
        self._update_index()
        if create is True:
            dirname = os.path.dirname(path)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            if object_name:
                uuidfile = os.path.join(dirname, '.object')
                object_prefix = self._load_object(uuidfile)
                if not object_prefix:
                    object_prefix = self._create_object(basedirname, uuidfile)
        elif object_name:
            dirname = os.path.dirname(path)
            if os.path.exists(dirname):
                uuidfile = os.path.join(dirname, '.object')
                object_prefix = self._load_object(uuidfile)
        
        if object_prefix:
            return (path, object_prefix + '_' + object_name)
        return (path, None)

    def get_content(self, path):
        logger.info('get_content: ' + str(path))
        (path, object) = self._init_path(path)
        logger.info('object[' + path + '] = ' + str(object))
        if self._is_local_path(path):
            with open(path, mode='r') as f:
                return f.read()
        
        if not object:
            raise IOError("Directory entry not found: " + path)
        try:
            (headers, data) = self._connection.get_object(self._container, object)
            return data
        except ClientException, inst:
            if inst.http_status == 404:
                raise IOError("Object(" + object + ") not found: " + path)
            raise inst

    def put_content(self, path, content):
        logger.info('put_content: ' + str(path))
        (path, object) = self._init_path(path, create=True)
        logger.info('object[' + path + '] = ' + str(object))
        if self._is_local_path(path):
            with open(path, mode='w') as f:
                f.write(content)
            return path

        if not object:
            raise IOError("Directory entry not found: " + path)
        (level, temppath) = mkstemp()
        with open(temppath, mode='w') as f:
            f.write(content)
        
        tempfile = open(temppath, 'r')
        try:
            self._connection.put_object(self._container, object, tempfile, chunk_size=4096)  
        except ClientException, inst:
            tempfile.close()
            os.remove(temppath)
            if inst.http_status == 404:
                raise IOError("Object(" + object + ") not found: " + path)
            raise inst
        tempfile.close()
        os.remove(temppath)

        return path

    def stream_read(self, path):
        logger.info('stream_read: ' + str(path))
        (path, object) = self._init_path(path)
        logger.info('object[' + path + '] = ' + str(object))
        if not object:
            raise IOError("Directory entry not found: " + path)
        
        try:
            (headers, data) = self._connection.get_object(self._container, object)
            return data
        except ClientException, inst:
            if inst.http_status == 404:
                raise IOError("Object(" + object + ") not found: " + path)
            raise inst

    def stream_write(self, path, fp):
        logger.info('stream_write: ' + str(path))
        # Size is mandatory
        (path, object) = self._init_path(path, create=True)
        logger.info('object[' + path + '] = ' + str(object))
        if not object:
            raise IOError("Directory entry not found: " + path)

        (level, temppath) = mkstemp()
        with open(temppath, mode='wb') as f:
            while True:
                try:
                    buf = fp.read(self.buffer_size)
                    if not buf:
                        break
                    f.write(buf)
                except IOError:
                    break
        
        tempfile = open(temppath, 'rb')
        try:
            self._connection.put_object(self._container, object, tempfile, chunk_size=4096)  
        except ClientException, inst:
            tempfile.close()
            os.remove(temppath)
            if inst.http_status == 404:
                raise IOError("Object(" + object + ") not found: " + path)
            raise inst
        tempfile.close()
        os.remove(temppath)

    def list_directory(self, path=None):
        logger.info('list_directory: ' + str(path))
        (path, object) = self._init_path(path)
        prefix = path[len(self._root_path) + 1:] + '/'
        uuidfile = os.path.join(path, '.object')
        if os.path.exists(uuidfile):
            uuid = self._load_object(uuidfile)
            logger.info("list_directory_from_swift: " + uuid)
            (headers, objects) = self._connection.get_container(self._container)
            for object in objects:
                object_name = object['name']
                if not object_name:
                    continue
                if object_name.startswith(uuid + '_'):
                    yield prefix + object_name[len(uuid) + 1:]
            return
        exists = False
        for d in os.listdir(path):
            exists = True
            yield prefix + d
        if exists is False:
            # Raises OSError even when the directory is empty
            # (to be consistent with S3)
            raise OSError('No such directory: \'{0}\''.format(path))

    def exists(self, path):
        logger.info('exists: ' + str(path))
        (path, object) = self._init_path(path)
        logger.info('object[' + path + '] = ' + str(object))
        if self._is_local_path(path):
            return os.path.exists(path)
        if not object:
            return False
        try:
            headers = self._connection.head_object(self._container, object)
            lastupdatestr = headers['last-modified']
        except ClientException, inst:
            if inst.http_status != 404:
                raise inst
            return False
        return True

    def remove(self, path):
        logger.info('remove: ' + str(path))
        (path, object) = self._init_path(path)
        logger.info('object[' + path + '] = ' + str(object))
        if os.path.isdir(path):
            shutil.rmtree(path)
            return
        if self._is_local_path(path):
            try:
                os.remove(path)
            except OSError:
                pass
            return
        if not object:
            return
        try:
            headers = self._connection.delete_object(self._container, object)
            lastupdatestr = headers['last-modified']
        except ClientException, inst:
            pass

    def get_size(self, path):
        logger.info('get_size: ' + str(path))
        (path, object) = self._init_path(path)
        logger.info('object[' + path + '] = ' + str(object))
        if self._is_local_path(path):
            return os.path.getsize(path)
        if not object:
            raise IOError("Directory entry not found: " + path)
        try:
                headers = self._connection.head_object(self._container, object)
                sizestr = headers['content-length']
                size = int(sizestr)
                logger.info("Size: " + str(size))
                return size
        except ClientException, inst:
            if inst.http_status == 404:
                raise IOError("Object(" + object + ") not found: " + path)
            raise inst

