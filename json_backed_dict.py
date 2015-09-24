import os, shutil, json, tempfile 

class JSONBackedDict(dict):
    """ 
    inherits from dict but only supports bare init (can not populate dict at init)
    rewrites the json backing with every setitem
    """
    def __init__(self, filename=""):
        dict.__init__(self)
        self.filename = filename
        if os.path.exists(self.filename):
            self._read()
        else:
            self._write()            
    
    def _write(self):
        fd_out, fd_out_name = tempfile.mkstemp(dir=os.curdir)
        fd_in_name = self.filename
        with os.fdopen(fd_out, "w") as outfile:
            outfile.write(json.dumps(self))
        # then rename the temporary file to the backing file name...
        shutil.move(fd_out_name, fd_in_name)
    
    def _read(self, overwrite=True):
        if overwrite:
            self.clear()
        with open(self.filename, "r") as f:
            self.update(json.loads(f.read()))
    
    def __setitem__(self, *args, **kw):
        dict.__setitem__(self, *args, **kw)
        self._write()
    
    def setdefault(self, *args, **kw):
        dict.setdefault(self, *args, **kw)
        self._write()
        
    def pop(self, *args, **kw):
        val = dict.pop(self, *args, **kw)
        self._write()
        return val
