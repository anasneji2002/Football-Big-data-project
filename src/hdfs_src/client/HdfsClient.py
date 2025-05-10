from hdfs import Config

class HdfsClient:
    def __init__(self, env):
        self.__client = Config().get_client(env) 
        pass

    def list_files(self, path = '.'):
        return self.__client.list(path)
    
    def get_file(self, path):
        with self.__client.read(path) as reader:
            content = reader.read()
        return content
    
    

