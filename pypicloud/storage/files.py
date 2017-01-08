""" Store packages as files on disk """
import json
from datetime import datetime
from contextlib import closing

from pyramid.response import FileResponse

import os
from .base import IStorage
from pypicloud.models import Package


class FileStorage(IStorage):

    """ Stores package files on the filesystem """

    def __init__(self, request=None, **kwargs):
        self.directory = kwargs.pop('directory')
        super(FileStorage, self).__init__(request, **kwargs)

    @classmethod
    def configure(cls, settings):
        kwargs = super(FileStorage, cls).configure(settings)
        directory = os.path.abspath(settings['storage.dir']).rstrip('/')
        if not os.path.exists(directory):
            os.makedirs(directory)
        kwargs['directory'] = directory
        return kwargs

    def get_path(self, package, config=False):
        """ Get the fully-qualified file path for a package """
        return os.path.join(self.directory, package.name, package.version,
                            package.filename if not config else 'config.json')

    def list(self, factory=Package):
        for root, _, files in os.walk(self.directory):
            config_file = {}
            for filename in files:
                if filename == 'config.json':
                    with open(os.path.join(root, filename), 'r') as f:
                        config_file = json.loads(f.read())
                    continue
                shortpath = root[len(self.directory):].strip('/')
                name, version = shortpath.split('/')
                fullpath = os.path.join(root, filename)
                last_modified = datetime.fromtimestamp(os.path.getmtime(
                    fullpath))
                yield factory(name, version, filename, last_modified, **config_file)

    def download_response(self, package):
        return FileResponse(self.get_path(package),
                            request=self.request,
                            content_type='application/octet-stream')

    def upload(self, package, data, **kwargs):
        destfile = self.get_path(package)
        destconf = self.get_path(package, config=True)
        destdir = os.path.dirname(destfile)
        if not os.path.exists(destdir):
            os.makedirs(destdir)
        uid = os.urandom(4).encode('hex')

        tempconf = os.path.join(destdir, '.config.' + uid)
        with open(tempconf, 'w') as dfile:
            json_data = json.dumps(kwargs)
            dfile.write(json_data)

        os.rename(tempconf, destconf)

        tempfile = os.path.join(destdir, '.' + package.filename + '.' + uid)
        # Write to a temporary file
        with open(tempfile, 'w') as ofile:
            for chunk in iter(lambda: data.read(16 * 1024), ''):
                ofile.write(chunk)

        os.rename(tempfile, destfile)

    def delete(self, package):
        filename = self.get_path(package)
        conf_file = self.get_path(package, config=True)
        os.unlink(filename)
        os.unlink(conf_file)
        version_dir = os.path.dirname(filename)
        try:
            os.rmdir(version_dir)
        except OSError:
            return
        package_dir = os.path.dirname(version_dir)
        try:
            os.rmdir(package_dir)
        except OSError:
            return

    def open(self, package):
        filename = self.get_path(package)
        return closing(open(filename, 'r'))
