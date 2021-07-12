import entity

class ASG(entity.HasName):
    def __init__(self, name):
        r'@types: str'
        entity.HasName.__init__(self)
        self.setName(name)

    def __repr__(self):
        return r'asg.ASG("%s")' % (self.getName())

