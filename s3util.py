from boto.s3.connection import S3Connection
try:
    from private_setting import s3_key, s3_secret, s3_bucket
    print s3_key, s3_secret, s3_bucket   
except:
    from setting import s3_key, s3_secret, s3_bucket
    
if not(s3_key and s3_secret and s3_bucket):
    print "current config is %s, %s, %s"%(s3_key, s3_secret, s3_bucket )
    raise Exception("please configure setting.py")

class StoreBackup():
    def __init__(self, s3_key, s3_secret, s3_bucket):
        self.s3_key = s3_key
        self.s3_secret = s3_secret
        self.conn = S3Connection(s3_key, s3_secret)
        self.bucket = self.conn.get_bucket(s3_bucket)
        self.s3_list_keys = self.bucket.list()
        
    def show_lists(self):
        for i in self.s3_list_keys:
            print i.key
            print i.name, i.size
    def log_complete(self):
        pass

    def log_start(self):
        pass


sb = StoreBackup(s3_key, s3_secret, s3_bucket)

sb.show_lists()

