#!/usr/bin/env python
import sys
import os
from subprocess import Popen, PIPE
import threading
import Queue
import project_utilities, root_metadata
import json

# Function to wait for a subprocess to finish and fetch return code,
# standard output, and standard error.
# Call this function like this:
#
# q = Queue.Queue()
# jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# wait_for_subprocess(jobinfo, q)
# rc = q.get()      # Return code.
# jobout = q.get()  # Standard output
# joberr = q.get()  # Standard error


"""extractor_dict.py
Purpose: To extract metadata from output file on worker node, generate JSON file
"""

class MetaData(object):
    """Base class to hold / interpret general metadata"""
    def __init__(self, inputfile):
        self.inputfile = inputfile

    def extract_metadata_to_pipe(self):
        """Extract metadata from inputfile into a pipe for further processing."""
        local = project_utilities.path_to_local(self.inputfile)
        if len(local) > 0:
            proc = subprocess.Popen(["sam_metadata_dumper", local], stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE)
        else:
            url = project_utilities.path_to_url(inputfile)
            proc = subprocess.Popen(["sam_metadata_dumper", url], stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE)
        if len(local) > 0 and local != self.inputfile:
            os.remove(local)
        return proc
    
    def get_job(self, proc):
        """It looks like this manages how the sam_metadata_dumper command is run, returns information from that command?  Need more info"""
        q = Queue.Queue()
        thread = threading.Thread(target=self.wait_for_subprocess, args=[proc, q])
        thread.start()
        thread.join(timeout=60)
        if thread.is_alive():
            print 'Terminating subprocess because of timeout.'
            proc.terminate()
            thread.join()
        rc = q.get()
        jobout = q.get()
        joberr = q.get()
        if rc != 0:
            raise RuntimeError, 'sam_metadata_dumper returned nonzero exit status {}.'.format(rc)
        return jobout,joberr
    
    @staticmethod
    def wait_for_subprocess(jobinfo, q):
        """Want more info about this too.  Looks like we're grabbing the output of sam_metadata_dumper, putting it into jobout and joberr"""
        jobout, joberr = jobinfo.communicate()
        rc = jobinfo.poll()
        q.put(rc)
        q.put(jobout)
        q.put(joberr)
        return

    @staticmethod
    def mdart_gen(jobtuple):
        """Take Jobout and Joberr (in jobtuple) and return mdart object from that"""
        mdtext = ''.join(line.replace(", ,",",") for line in jobtuple[0].split('\n') if line[-3:-1] != ' ,')
        mdtop = json.JSONDecoder().decode(mdtext)
        if len(mdtop.keys()) == 0:
            print 'No top-level key in extracted metadata.'
            sys.exit(1)
        file_name = mdtop.keys()[0]
        return mdtop[file_name]

    @staticmethod
    def md_handle_application(md):
        """If there's no application key in md dict, create the key with a blank dictionary.  Then return md['application'], along with mdval"""
        if not md.has_key('application'):
            md['application'] = {}
        return md['application']


class expMetaData(Metadata):
    """Class to hold/interpret experiment-specific metadata"""
    def __init__(self,expname,inputfile):
        MetaData.__init__(self,inputfile)
        self.expname = expname
        self.metadataList = [expname[:2] + elt for elt in ('ProjectName', 'project.name', 'ProjectStage', 'ProjectVersion')]
        # Is the abbreviation for all experiments the first two letters?  So the metadata list for ub is ubProjectName, etc..; is it noProjectName for Nova?  Or is there a dictionary we need to reference or something like that?  I'd like to do away with the second line if possible   

    @staticmethod
    def translateKey(key):
        """Parse the keys for experiment-specific metadata"""
        return key[:2] + '_' + key[2:]
    # Raise the error of not having implemented a defineMetaData function in the code.  I think this is a TypeError?
    # Only use a define metadata if you can instantiate expMetaData without defining the metadata.  Kirby thinks so

    def md_gen(self, mdart, md0 = {}): 
        """Loop through art metdata, generate metadata dictionary"""
        # define an empty python dictionary which will hold sam metadata.
        # Some fields can be copied directly from art metadata to sam metadata.
        # Other fields require conversion.
        md = {}

        # Loop over art metadata.
        for mdkey, mdval in mdart.iteritems():
            # mdval = mdart[mdkey]
            
            # Skip some art-specific fields.
            # Ignore primary run_type field (if any).
            # Instead, get run_type from runs field.
            if mdkey in ['file_format_version', 'file_format_era','run_type']:
                pass

            # Ignore data_stream if it begins with "out".
            # These kinds of stream names are probably junk module labels.
            elif mdkey == 'data_stream' and mdval[:3] == 'out' and \
                    mdval[3] >= '0' and mdval[3] <= '9':
                pass

            # Application family/name/version.
            elif mdkey == 'applicationFamily':
                md['application'],md['application']['family'] = self.md_handle_application(md), mdval
            elif mdkey == 'process_name':
                md['application'],md['application']['name'] = self.md_handle_application(md), mdval
            elif mdkey == 'applicationVersion':
                md['application'],md['application']['version'] = self.md_handle_application(md), mdval

            # Parents.
            elif mdkey == 'parents':
                md['parents'] = [{'file_name': parent} for parent in mdval]

            # Other fields where the key or value requires minor conversion.
            elif mdkey in ['first_event','last_event']:
                md[mdkey] = mdval[2]
            elif mdkey in self.metadataList:
                md[self.translateKey(mdkey)] = mdval
            elif mdkey == 'fclName':
                md['fcl.name'] = mdval
            elif mdkey == 'fclVersion':
                md['fcl.version']  = mdval    
            
            else:
                md[mdkey] = mdval
            
            '''
            elif mdkey == '%sProjectName' % exp:
                md['%s_project.name' % exp] = mdval
            elif mdkey == '%sProjectStage' % exp:
                md['%s_project.stage' % exp] = mdval
            elif mdkey == '%sProjectVersion' % exp:
                md['%s_project.version' % exp] = mdval
            '''
            # For all other keys, copy art metadata directly to sam metadata.
            # This works for run-tuple (run, subrun, runtype) and time stamps.

        # Get the other meta data field parameters.  You've defaulted md0 as {}. When would it not be {}?		
        
        md['file_name'] =  self.inputfile.split("/")[-1]
        if md0.has_key('file_size'):
            md['file_size'] = md0['file_size']
        else:
            md['file_size'] =  os.path.getsize(self.inputfile)
        if md0.has_key('crc'):
            md['crc'] = md0['crc']
        else:
            md['crc'] = root_metadata.fileEnstoreChecksum(self.inputfile)
        
        self.md = md        # In case we ever want to check out what md is for any instance of MetaData by calling instance.md
        return self.md

    def getmetadata(self):
        """ Get metadata from input file and return as python dictionary. Calls other methods in class and returns metadata dictionary"""
        proc = self.extract_metadata_to_pipe()
        jobt = self.get_job(proc)
        mdart = self.mdart_gen(jobt)
        return self.md_gen(mdart)	


if __name__ == "__main__":
    try:
        expSpecificMetadata = expMetaData(os.environ['SAM_EXPERIMENT'],str(sys.argv[1]))
	except TypeError:
        print 'You have not implemented a defineMetaData function by providing an experiment.   No metadata keys will be saved'
        raise
	mdtext = json.dumps(expSpecificMetadata.getmetadata(), indent=2, sort_keys=True)
	print mdtext
	sys.exit(0)	
