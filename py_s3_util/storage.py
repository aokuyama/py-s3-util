import unittest
import os
import shutil
import tempfile
import pathlib

class AwsS3StorageImplement:
    def __init__(self, logger=None):
        self.logger = logger

    def export_dir(self, local_dir, s3_dir):
        self.logger.debug(local_dir + ' -> ' + s3_dir)

    def export_dir_files(self, local_dir, s3_dir):
        self.logger.debug(local_dir + '/* -> ' + s3_dir)

    def import_dir(self, local_dir, s3_dir):
        self.logger.debug(local_dir + ' <- ' + s3_dir)


class AwsS3Storage(AwsS3StorageImplement):
    client = None
    resource = None
    bucket = None
    bucket_name = None

    def get_resource(self):
        if (not self.resource):
            self.resource = boto3.resource('s3')
        return self.resource

    def get_client(self):
        if (not self.client):
            self.client = boto3.client('s3')
        return self.client

    def get_bucket(self):
        if (not self.bucket):
            self.bucket = self.get_resource().Bucket(self.bucket_name)
        return self.bucket

    def get_zip_name(self, dir_name):
        dir_name = dir_name.rstrip('/')
        return os.path.basename(dir_name + '.zip')

    def export_dir(self, local_dir, s3_dir, zip_name=None):
        fp = tempfile.NamedTemporaryFile()
        local_zip_path = fp.name
        if (not zip_name):
            zip_name = self.get_zip_name(local_dir)
        s3_zip_path = s3_dir + '/' + zip_name
        self.logger.debug('zipping ' + local_dir)
        shutil.make_archive(local_zip_path, 'zip', root_dir=local_dir)
        return self.upload_file(local_zip_path + '.zip', s3_zip_path)

    def import_dir(self, local_dir, s3_dir, zip_name=None):
        if (not zip_name):
            zip_name = self.get_zip_name(local_dir)
        s3_zip_path = s3_dir + '/' + zip_name
        if (not self.is_exist_s3(s3_zip_path)):
            self.logger.debug('not exist ' + s3_zip_path)
            return False
        fp = tempfile.NamedTemporaryFile()
        self.download_file(s3_zip_path, fp.name)
        self.logger.debug('unzip ' + local_dir + ' <- ' + s3_zip_path)
        return shutil.unpack_archive(fp.name, extract_dir=local_dir, format='zip')

    def is_exist_s3(self, s3_zip_path):
        result = self.get_client().list_objects(
            Bucket=self.bucket_name, Prefix=s3_zip_path)
        return ('Contents' in result)

    def download_file(self, local_path, s3_path):
        self.logger.debug('download ' + local_path + ' <- ' + s3_path)
        return self.get_bucket().download_file(local_path, s3_path)

    def upload_file(self, local_path, s3_path):
        self.logger.debug('upload ' + local_path + ' -> ' + s3_path)
        return self.get_bucket().upload_file(local_path, s3_path)

    def export_dir_files(self, local_dir, s3_dir):
        p = pathlib.Path(local_dir)
        for file in p.glob("./**/*"):
            if (os.path.isdir(file)):
                continue
            s3 = s3_dir + '/' + str(file.relative_to(local_dir))
            self.upload_file(str(file), s3)


class TestAwsS3Storage(unittest.TestCase):
    def setUp(self):
        self.storage = AwsS3Storage()

    def testディレクトリ名からzipファイル名(self):
        self.assertEqual('hoge.zip', self.storage.get_zip_name('/test/hoge/'))
        self.assertEqual(
            'fuga.zip', self.storage.get_zip_name('/tmp/abc/fuga'))
        self.assertEqual('piyo.zip', self.storage.get_zip_name('/piyo'))


if __name__ == '__main__':
    unittest.main()
