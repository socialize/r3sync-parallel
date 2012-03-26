try:
    from private_setting import s3_key, s3_secret, s3_bucket,local_path
except:
    from setting import s3_key, s3_secret, s3_bucket,local_path    
import os
from boto.s3.connection import S3Connection
from boto.s3.key import Key                 
import hashlib
import pickle
from multiprocessing import Pool
def storing_keys(obj, p_path):
    pickling_obj = pickle.dump(obj, open(p_path, 'wb'))
       
def obtaining_keys(p_path):
    return pickle.loads(open(p_path,'rb').read())

def compute_file_md5(fullPath):
    block_size=2**20
    f = file(fullPath)
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()

def validate_file_got_updated(key, fullPath):
    """
        check if file get update after last sync
    """
    if os.path.isdir(fullPath):
        print '\tcan not compute md5 on directory'
        return False
    md5 = compute_file_md5(fullPath)
    etag= key.etag.replace('\"','')
    if not etag == md5:
        print '\tetag\t= %s'%etag
        print '\tmd5\t= %s'%md5
        print '\tfile size= %s'%key.size  

    return not etag == md5

def touch_file(fullPath):
    f = file(fullPath,'w')## Touch file
    f.write('')
    f.close()  

def backup_s3_by_key(keyS3):
    key = keyS3.key
    fullPath = keyS3.local_path+key.key
    folder_path = '/'.join(fullPath.split('/')[0:-1])
    print 'managing content to file: %s'%fullPath
    
    ## file not exists
    if not os.path.exists(fullPath):
        try:
            if key.size>0:
                key.get_contents_to_filename(fullPath)
                print '\tfile download'
            else:
                touch_file(fullPath)
                print '\ttouch file'
        except IOError,OSError:       ## it 's not a file should be folder
            os.remove(folder_path)    ## rm file type =0
            os.makedirs(folder_path)  ## create folder 
            key.get_contents_to_filename(fullPath)
            print '\tcreate folder, file'
    ## file exist
    else:
        if validate_file_got_updated(key,fullPath):
            print '\tmd5 not match need to update'
            ## need update
            os.remove(fullPath)                
            key.get_contents_to_filename(fullPath)
        else:
            print '\tmd5 match skipped.'
    return True
                                                               

class KeyS3(object):
    def __init__(self,key,local_path):
        self.key = key
        self.local_path = local_path

## Process Manager Class
class parallelSync:
    def __init__(self, s3_key, s3_secret, s3_bucket, local_path):
        self.s3_key = s3_key
        self.s3_secret = s3_secret
        self.conn = S3Connection(s3_key, s3_secret)
        self.s3_bucket_name = s3_bucket
        self.s3_bucket = self.conn.get_bucket(s3_bucket)
        self.local_path = local_path
        self.total = None
        self.keys = None

    ## Create tasks
    def get_key_list(self):
        keys = []
        total_byte = 0
        for item in self.s3_bucket.list():
            ## This is for replace boto bucket object with string
            ## in order to pickle the list of object
            key = KeyS3(item, self.local_path)
            total_byte += item.size
            keys.append(key)
        print 'total files:  %s'%len(keys)
        print 'total byte: %s'%total_byte
        self.keys = keys
        self.total= len(keys)
        
        #storing_keys(keys, 'test')
        return keys

    def run(self):
        touch_file('start')
        total = len( self.keys )            ## Create process pool     
        map( backup_s3_by_key , self.keys )
        touch_file('finish')

if __name__ == '__main__':
    p = parallelSync(s3_key, s3_secret, s3_bucket,local_path )
    p.get_key_list()
    p.run()
