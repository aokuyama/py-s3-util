class AwsS3StorageImplement:
    def __init__(self, logger=None):
        self.logger = logger

    def export_dir(self, local_dir, s3_dir):
        self.logger.debug(local_dir + ' -> ' + s3_dir)

    def export_dir_files(self, local_dir, s3_dir):
        self.logger.debug(local_dir + '/* -> ' + s3_dir)

    def import_dir(self, local_dir, s3_dir):
        self.logger.debug(local_dir + ' <- ' + s3_dir)

class AwsS3Storage:
    pass
