import sys

class expMetaData(object):
    """ Base class to hold / interpret experiment specific metadata"""
    def defineMetaData(self, expname):
        print 'You have not implemented a defineMetaData function. No metadata keys will be saved'
    
    def translateKey(self, key):
        print 'You have not implemented a translateKey function. No metadata keys will be changed'
        return key    

class ubMetaData(expMetaData):

    def defineMetaData(self, expname):
        self.metadataList = [expname+'ProjectName', expname+'project.name', expname+'ProjectStage', expname+'ProjectVersion']

    def translateKey(self, key):
        #near as I can tell, we just add an underscore after 'ub'
        return key[:2] + '_' +key[2:]
        #print rKey + ' ' + key
           
           
  
## Why are there two different MetaData classes?  We can have just one with all of those methods, perhaps defining an __init__ method so that when the class is instantiated, it defines the metadata.  Example below:


class Proposed_expMetaData(object):
    """Base class to hold/interpret experiment-specific metadata"""
    def __init__(self,expname):
        self.expname = expname
        self.metadataList = [expname+'ProjectName', expname+'project.name', expname+'ProjectStage', expname+'ProjectVersion']
        
    # Raise the error of not having implemented a defineMetaData function in the code.  I think this is a TypeError?
    # Only use a define metadata if you can instantiate expMetaData without defining the metadata

    def translateKey(self,key):
       return key[:2] + '_' +key[2:]
    
    # Again, if this is separate from actually instantiating the class, then have it be its own separate method.  Otherwise, put it in the init as a variable self.translateKey, pass 'key' into class instantiation


class Even_better_proposed_expMetaData(object):
    """Base class to hold/interpret experiment-specific metadata"""
    def __init__(self,expname,key):
        self.expname = expname
        self.metadataList = [expname+'ProjectName', expname+'project.name', expname+'ProjectStage', expname+'ProjectVersion']
        self.translateKey = key[:2] + '_' + key[2:]
    # Raise the error of not having implemented a defineMetaData function in the code.  I think this is a TypeError?
    # Only use a define metadata if you can instantiate expMetaData without defining the metadata

    
    # Again, if this is separate from actually instantiating the class, then have it be its own separate method.  Otherwise, put it in the init as a variable self.translateKey, pass 'key' into class instantiation

original_base = expMetaData()
original_base.defineMetaData('uboone')
original_base.translateKey('uboone_key')

original = ubMetaData()
print original.defineMetaData('uboone')
print original.metadataList
print original.translateKey('uboone_key')

try:
    proposed_bad = Proposed_expMetaData()
except TypeError:
    print 'You have not implemented a defineMetaData function by providing an experiment.   No metadata keys will be saved'
    print "here, the program would raise the error with a raise statement"
    pass

proposed = Proposed_expMetaData('uboone')
print proposed.metadataList
print proposed.translateKey('uboone_key')

best = Even_better_proposed_expMetaData('uboone','uboone_key')
print best.metadataList
print best.translateKey




