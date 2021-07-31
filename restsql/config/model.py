
class PostgreDatabase:
    def __init__(self, db_name, user, password, host, port):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port

class DruidDatabase:
    def __init__(self,host,port,db_name):
        self.host=host
        self.host=port
        self.db_name=db_name
