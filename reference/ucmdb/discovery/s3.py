import entity

class Bucket(entity.HasName):
    def __init__(self, name):
        r'@types: str'
        entity.HasName.__init__(self)
        self.setName(name)
        self.owner = None
        self.is_versioning = False
        self.is_cross_region_replication = False
        self.region = None
        self.create_time = None
        self.arn = None

    def __repr__(self):
        return r's3.Bucket("%s")' % (self.getName())
