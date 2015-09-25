import os, sys
import zipfile, tempfile, shutil
from json_backed_dict import JSONBackedDict
import numpy
import iso8601

if bytes != str:
    def _b(s): return bytes(s, 'utf-8')
else:
    def _b(s): return s

DEFAULT_ENDIANNESS = '<' if (sys.byteorder == 'little') else '>'
__version__ = "0.0.1"

builtin_open = __builtins__['open']

class Node(object):
    _attrs_filename = ".attrs"
    
    def __init__(self, parent_node=None, path="/", nxclass=None, attrs={}):
        self.root_node = self if parent_node is None else parent_node.root_node
        if path.startswith("/"):
            # absolute path
            self.path = path
        else: 
            # relative
            self.path = os.path.join(parent_node.path, path)
    
    @property
    def parent(self):
        return self.root_node[os.path.dirname(self.name)]
               
    @property
    def groups(self):
        groupnames = [x for x in os.listdir(os.path.join(self.os_path, self.path.lstrip("/"))) if os.path.isdir(os.path.join(self.os_path, self.path.lstrip("/"), x))] 
        return dict([(gn, Group(self, gn)) for gn in groupnames])
    
    @property
    def name(self):
        return self.path
    
    def keys(self):
        return [x for x in os.listdir(os.path.join(self.os_path, self.path.lstrip("/"))) if not "." in x]
        
    def items(self):
        keys = self.keys()
        return [(k, self[k]) for k in keys]
    
    def __contains__(self, key):
        return (key in self.keys())
    
    def __getitem__(self, path):
        """ get an item based only on its path.
        Can assume that next-to-last segment is a group (dataset is lowest level)
        """
        if path.startswith("/"):
            # absolute path
            full_path = path
        else: 
            # relative
            full_path = os.path.join(self.path, path)

        os_path = os.path.join(self.os_path, full_path.lstrip("/"))
        if os.path.exists(os_path):
            #print os_path, full_path
            if os.path.isdir(os_path):
                # it's a group
                return Group(self, full_path)
            elif os.path.exists(os_path + ".link"):
                # it's a link
                return FieldLink(self, full_path)
            else:
                # it's a field
                return FieldFile(self, full_path)
        else:
            # the item doesn't exist
            raise KeyError(path)
    
    def add_field(self, path, **kw):
        FieldFile(self, path, **kw)
        
    def add_group(self, path, nxclass, attrs={}):
        Group(self, path, nxclass, attrs)

class File(Node):
    def __init__(self, filename, mode="r", timestamp=None, creator=None, compression=zipfile.ZIP_DEFLATED, attrs={}, **kw):
        fn = tempfile.mkdtemp()
        self.os_path = fn
        Node.__init__(self, parent_node=None, path="/")        
        self.attrs = JSONBackedDict(os.path.join(self.os_path, self._attrs_filename))
        self.filename = filename
        self.mode = mode
        self.compression = compression
        file_exists = os.path.exists(filename)
        if file_exists and (mode == "a" or mode == "r"):
             zipfile.ZipFile(filename).extractall(self.os_path)
        
        if mode == "a" or mode == "w":
            #os.mkdir(os.path.join(self.os_path, self.path.lstrip("/")))
            if timestamp is None:
                timestr = iso8601.now()
            else:
                # If given a time string, check that it is valid
                try:
                    timestamp = iso8601.parse_date(timestamp)
                except TypeError:
                    pass
                timestr = iso8601.format_date(timestamp)
            attrs['NX_class'] = 'NXroot'
            attrs['file_name'] = filename
            attrs['file_time'] = timestr
            attrs['NeXus_version'] = __version__
            if creator is not None:
                attrs['creator'] = creator       
        self.attrs.update(attrs)
        self.attrs._write()

    def __repr__(self):
        return "<HDZIP file \"%s\" (mode %s)>" % (self.filename, self.mode)
           
    def close(self):
        # there seems to be only one read-only mode
        if self.mode != "r":
            self.writezip()
        shutil.rmtree(self.os_path)
        
    def writezip(self):
        make_zipfile_withlinks(self.filename, os.path.join(self.os_path, self.path.lstrip("/")), self.compression)
        
    
class Group(Node):
    def __init__(self, node, path, nxclass="NXCollection", attrs={}):
        Node.__init__(self, parent_node=node, path=path)
        if path.startswith("/"):
            # absolute path
            self.path = path
        else: 
            # relative
            self.path = os.path.join(node.path, path)
        
        self.os_path = node.os_path
        preexisting = os.path.exists(os.path.join(self.os_path, self.path.lstrip("/")))
        
        if not preexisting:
            os.mkdir(os.path.join(self.os_path, self.path.lstrip("/")))
            attrs['NX_class'] = nxclass.encode('UTF-8')
        
        self.attrs = JSONBackedDict(os.path.join(self.os_path, self.path.lstrip("/"), self._attrs_filename))
        self.attrs.update(attrs)
        self.attrs._write()
        
    def __repr__(self):
        return "<HDZIP group \"" + self.path + "\">"
    

class FieldFile(object):
    _formats = {
        'S': '%s',
        'f': '%.8g',
        'i': '%d',
        'u': '%d',
        'b': '%d'}
        
    _attrs_suffix = ".attrs"
        
    def __init__(self, node, path, **kw):
        """
        Create a data object.
        
        Returns the data set created, or None if the data is empty.

        :Parameters:

        *node* : File object
            Handle to a File-like object.  This could be a file or a group.

        *path* : string
            Path to the data.  This could be a full path from the root
            of the file, or it can be relative to a group.  Path components
            are separated by '/'.

        *data* : array or string
            If the data is known in advance, then the value can be given on
            creation. Otherwise, use *shape* to give the initial storage
            size and *maxshape* to give the maximum size.

        *units* : string
            Units to display with data.  Required for numeric data.

        *label* : string
            Axis label if data is numeric.  Default for field dataset_name
            is "Dataset name (units)".

        *attrs* : dict
            Additional attributes to be added to the dataset.


        :Storage options:

        *dtype* : numpy.dtype
            Specify the storage type for the data.  The set of datatypes is
            limited only by the HDF-5 format, and its h5py interface.  Usually
            it will be 'int32' or 'float32', though others are possible.
            Data will default to *data.dtype* if *data* is specified, otherwise
            it will default to 'float32'.

        *shape* : [int, ...]
            Specify the initial shape of the storage and fill it with zeros.
            Defaults to [1, ...], or to the shape of the data if *data* is
            specified.

        *maxshape* : [int, ...]
            Maximum size for each dimension in the dataset.  If any dimension
            is None, then the dataset is resizable in that dimension.
            For a 2-D detector of size (Nx,Ny) with Nt time of flight channels
            use *maxshape=[Nx,Ny,Nt]*.  If the data is to be a series of
            measurements, then add an additional empty dimension at the front,
            giving *maxshape=[None,Nx,Ny,Nt]*.  If *maxshape* is not provided,
            then use *shape*.

        *chunks* : [int, ...]
            Storage block size on disk, which is also the basic compression
            size.  By default *chunks* is set from maxshape, with the
            first unspecified dimension set such that the chunk size is
            greater than nexus.CHUNK_SIZE. :func:`make_chunks` is used
            to determine the default value.

        *compression* : 'none|gzip|szip|lzf' or int
            Dataset compression style.  If not specified, then compression
            defaults to 'szip' for large datasets, otherwise it defaults to
            'none'. Datasets are considered large if each frame in maxshape
            is bigger than CHUNK_SIZE.  Eventmode data, with its small frame
            size but large number of frames, will need to set compression
            explicitly.  If compression is an integer, then use gzip compression
            with that compression level.

        *compression_opts* : ('ec|nn', int)
            szip compression options.

        *shuffle* : boolean
            Reorder the bytes before applying 'gzip' or 'hzf' compression.

        *fletcher32* : boolean
            Enable error detection of the dataset.

        :Returns:

        *dataset* : file-backed data object
            Reference to the created dataset.
        """
        self.root_node = node.root_node
        self.os_path = node.os_path
        if not path.startswith("/"):
            # relative path:
            path = os.path.join(node.path, path)
        self.path = path
            
        preexisting = os.path.exists(os.path.join(self.os_path, self.path.lstrip("/")))
            
        self.attrs_path = self.path + self._attrs_suffix
        self.attrs = JSONBackedDict(os.path.join(self.os_path, self.attrs_path.lstrip("/")))
        
        
        
        if preexisting:
            pass
        else:
            data = kw.pop('data', numpy.array([]))
            attrs = kw.pop('attrs', {})
            attrs['description'] = kw.setdefault('description', None)
            attrs['dtype'] = kw.setdefault('dtype', None)
            attrs['units'] = kw.setdefault('units', None)
            attrs['label'] = kw.setdefault('label', None)
            attrs['binary'] = kw.setdefault('binary', False)
            attrs['byteorder'] = sys.byteorder
            if attrs['dtype'] is None:
                raise TypeError("dtype missing when creating %s" % (path,))
            self.attrs.clear()
            self.attrs.update(attrs)
            self.attrs._write()
            if data is not None:
                if numpy.isscalar(data): data = [data]
                data = numpy.asarray(data, dtype=attrs['dtype'])        
                self.value = data
    
    def __repr__(self):
        return "<HDZIP field \"%s\" %s \"%s\">" % (self.name, str(self.attrs['shape']), self.attrs['dtype'])
    
    def __getitem__(self, slice_def):
        return self.value.__getitem__(slice_def)
        
    def __setitem__(self, slice_def, newvalue):
        intermediate = self.value
        intermediate[slice_def] = newvalue
        self.value = intermediate 
    
    # promote a few attrs items to python object attributes:
    @property
    def shape(self):
        return self.attrs.get('shape', None)
    
    @property
    def dtype(self):
        return self.attrs.get('dtype', None)
    
    @property
    def name(self):
        return self.path
          
          
    @property
    def parent(self):
        return self.root_node[os.path.dirname(self.name)]
                
    @property
    def value(self):
        attrs = self.attrs
        target = os.path.join(self.os_path, self.path.lstrip("/"))
        with builtin_open(target, 'rb') as infile:
            if attrs.get('binary', False) == True:
                d = numpy.fromfile(infile, dtype=attrs['format'])
            else:
                d = numpy.loadtxt(infile, dtype=numpy.dtype(str(attrs['format'])))
        if 'shape' in attrs:
            d = d.reshape(attrs['shape'])
        return d              
    
    @value.setter
    def value(self, data):
        attrs = self.attrs
        if hasattr(data, 'shape'): attrs['shape'] = data.shape
        elif hasattr(data, '__len__'): attrs['shape'] = [data.__len__()]
        if hasattr(data, 'dtype'): 
            formatstr = '<' if attrs['byteorder'] == 'little' else '>'
            formatstr += data.dtype.char
            formatstr += "%d" % (data.dtype.itemsize * 8,)
            attrs['format'] = formatstr            
            attrs['dtype'] = data.dtype.name
        
        self._write_data(data, 'w')
            
    def _write_data(self, data, mode='w'):
        target = os.path.join(self.os_path, self.path.lstrip("/"))
        if self.attrs.get('binary', False) == True:
            with builtin_open(target, mode + "b") as outfile:                           
                data.tofile(outfile)
        else:            
            with builtin_open(target, mode) as outfile:       
                numpy.savetxt(outfile, data, delimiter='\t', fmt=self._formats[data.dtype.kind])
                
    def append(self, data, coerce_dtype=True):
        # add to the data...
        # can only append along the first axis, e.g. if shape is (3,4)
        # it becomes (4,4), if it is (3,4,5) it becomes (4,4,5)
        attrs = self.attrs
        if (list(data.shape) != list(attrs.get('shape', [])[1:])):
            raise Exception("invalid shape to append")
        if data.dtype != attrs['dtype']:
            if coerce_dtype == False:
                raise Exception("dtypes do not match, and coerce is set to False")
            else:
                data = data.astype(attrs['dtype'])
                                
        new_shape = list(attrs['shape'])
        new_shape[0] += 1
        attrs['shape'] = new_shape
        self._write_data(data, mode='a')
        
    def extend(self, data, coerce_dtype=True):
        attrs = self.attrs
        if (list(data.shape[1:]) != list(attrs.get('shape', [])[1:])):
            raise Exception("invalid shape to append")
        if data.dtype != attrs['dtype']:
            if coerce_dtype == False:
                raise Exception("dtypes do not match, and coerce is set to False")
            else:
                data = data.astype(attrs['dtype'])
                
        new_shape = list(attrs['shape'])
        new_shape[0] += data.shape[0]
        attrs['shape'] = new_shape
        self._write_data(data, "a")

class FieldLink(FieldFile):
    def __init__(self, node, path, target_path=None, **kw):
        if not path.startswith("/"):
            path = os.path.join(node.path, path)
        self.orig_path = path
        self.os_path = node.os_path
        orig_attrs_path = path + ".link"
        self.orig_attrs = JSONBackedDict(os.path.join(self.os_path, orig_attrs_path.lstrip("/")))
                
        if 'target' in self.orig_attrs:
            target_path = self.orig_attrs['target']
        else:
            self.orig_attrs['target'] = target_path
        
        FieldFile.__init__(self, node, target_path, **kw)
        preexisting = os.path.exists(os.path.join(self.os_path, self.orig_path.lstrip("/")))
        if preexisting:
            pass
        else:
            builtin_open(os.path.join(self.os_path, self.orig_path.lstrip("/")), "w").write("soft link: see .link file for target")
            
        self.attrs = StaticDictWrapper(self.attrs, self.orig_attrs)
      
    @property
    def name(self):
        return self.orig_path
        
import collections
from itertools import chain

class StaticDictWrapper(collections.MutableMapping):
    def __init__(self, wrapped_dict, static_dict):
        self.wrapped_dict = wrapped_dict
        self.static_dict = static_dict
    
    def copy(self):
        output = dict()
        output.update(self.wrapped_dict)
        output.update(self.static_dict)
        return output
        
    def __repr__(self):
        return self.copy().__repr__()
    
    def __getitem__(self, key):
        if key in self.static_dict:
            return self.static_dict[key]
        else:
            return self.wrapped_dict[key]
            
    def __setitem__(self, key, value):
        if key in self.static_dict:
            raise KeyError("static key: can't write")
        else:
            self.wrapped_dict[key] = value
            
    def __delitem__(self, key):
        if key in self.static_dict:
            raise KeyError("static key: can't write")
        else:
            del self.wrapped_dict[key]
            
    def __iter__(self):
        return chain(iter(self.static_dict), iter(self.wrapped_dict))
        
    def __len__(self):
        return len(self.static_dict) + len(self.wrapped_dict)
            

def write_item(zipOut, relroot, root, permissions=0755):
    """ check if a path points to a link, or a file, or a directory,
    and take appropriate action in the zip archive """
    # zipinfo.external_attr = 0644 << 16L # permissions -r-wr--r--
    # zipinfo.external_attr = 0755 << 16L # permissions -rwxr-xr-x
    # zipinfo.external_attr = 0777 << 16L # permissions -rwxrwxrwx
    # e.g. zipInfo.external_attr = 2716663808L for 0755 permissions + link type
    relpath = os.path.relpath(root, relroot)
    
    if os.path.islink(root):
        zipInfo = zipfile.ZipInfo(relpath)
        zipInfo.create_system = 3
        # long type of hex val of '0xA1ED0000L',
        # say, symlink attr magic...
        zipInfo.external_attr = permissions << 16L       
        zipInfo.external_attr |= 0120000 << 16L # symlink file type        
        zipOut.writestr(zipInfo, os.readlink(root))
        return
    else:
        zipOut.write(root, relpath)


def isLink(full_path):
    return os.path.exists(full_path + '.link')

def link(node, link):
    try:
        FieldLink(node, link, node.path)
    except:
        annotate_exception("when linking %s to %s"%(link, node.name))
        raise

def update_hard_links(*args, **kw):
    pass
    
def annotate_exception(msg, exc=None):
    """
    Add an annotation to the current exception, which can then be forwarded
    to the caller using a bare "raise" statement to reraise the annotated
    exception.
    """
    if not exc: exc = sys.exc_info()[1]
        
    args = exc.args
    if not args:
        arg0 = msg
    else:
        arg0 = " ".join((args[0],msg))
    exc.args = tuple([arg0] + list(args[1:]))
        
def make_zipfile_withlinks(output_filename, source_dir, compression=zipfile.ZIP_DEFLATED):
    relroot = os.path.abspath(source_dir)
    try: 
        zipped = zipfile.ZipFile(output_filename, "w", compression)
        for root, dirs, files in os.walk(source_dir):
            # add directory (needed for empty dirs)
            for d in dirs:
                dirname = os.path.join(root, d)
                write_item(zipped, relroot, dirname)
            for f in files:
                filename = os.path.join(root, f)
                write_item(zipped, relroot, filename)
    finally:
        zipped.close()
                            

#compatibility with h5nexus:
group = Group
field = FieldFile 
open = File

def extend(node, data):
    node.extend(data)
    
def append(node, data):
    node.append(data)
    
"""
if os.path.islink(fullPath):
    # http://www.mail-archive.com/python-list@python.org/msg34223.html
    zipInfo = zipfile.ZipInfo(archiveRoot)
    zipInfo.create_system = 3
    # long type of hex val of '0xA1ED0000L',
    # say, symlink attr magic...
    zipinfo.external_attr = 0644 << 16L # permissions -r-wr--r--
    # or zipinfo.external_attr = 0755 << 16L # permissions -rwxr-xr-x
    # or zipinfo.external_attr = 0777 << 16L # permissions -rwxrwxrwx
    # zipInfo.external_attr = 2716663808L # for 0755 permissions
    zipinfo.external_attr |= 0120000 << 16L # symlink file type
    zipOut.writestr(zipInfo, os.readlink(fullPath))
else:
    zipOut.write(fullPath, archiveRoot, zipfile.ZIP_DEFLATED)
"""                    
