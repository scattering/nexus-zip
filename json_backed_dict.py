import os, shutil, json, tempfile 

class JSONBackedDict(dict):
    """ 
    inherits from dict but only supports bare init (can not populate dict at init)
    rewrites the json backing with every setitem
    """
    def __init__(self, filename="", encoder=None):
        dict.__init__(self)
        self.filename = filename
        self.encoder = encoder
        if os.path.exists(self.filename):
            self._read()
        else:
            self._write()
    
    def _write(self):
        fd_out, fd_out_name = tempfile.mkstemp()
        fd_in_name = self.filename
        with os.fdopen(fd_out, "w") as outfile:
            outfile.write(json.dumps(self, cls=self.encoder))
        # then rename the temporary file to the backing file name...
        shutil.move(fd_out_name, fd_in_name)
    
    def _read(self, overwrite=True):
        if overwrite:
            self.clear()
        with open(self.filename, "r") as f:
            self.update(json.loads(f.read()))
            
    def __getitem__(self, key):
        # convert on retrieve: 
        value = dict.__getitem__(self, key)
        if isinstance(value, dict) and not isinstance(value, JSONBackedSubDict):
            value = JSONBackedSubDict(self, input_dict=value)
            self[key] = value
        return value
    
    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        self._write()
    
    def update(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError("update expected at most 1 arguments, "
                                "got %d" % len(args))
            other = dict(args[0])
            for key in other:
                self[key] = other[key]
        for key in kwargs:
            self[key] = kwargs[key]
            
    def setdefault(self, *args, **kw):
        dict.setdefault(self, *args, **kw)
        self._write()
    
    def __delitem__(self, key):
        dict.__delitem__(self, key)
        self._write()
        
    def pop(self, *args, **kw):
        val = dict.pop(self, *args, **kw)
        self._write()
        return val
        
class JSONBackedSubDict(JSONBackedDict):
    def __init__(self, root_dict, input_dict=None):
        input_dict = input_dict or {}
        dict.__init__(self, input_dict)
        self.root_dict = root_dict
        
    def _write(self):
        self.root_dict._write()
