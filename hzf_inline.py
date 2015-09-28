import os, sys
import zipfile, tempfile, shutil
from json_backed_dict import JSONBackedDict
import numpy
import iso8601

DEFAULT_ENDIANNESS = '<' if (sys.byteorder == 'little') else '>'
__version__ = "0.0.1"

builtin_open = __builtins__['open']

class Node(object):
    _attrs_filename = ".attrs"
    _fields_filename = ".fields"
    
    def __init__(self, parent_node, path="/", nxclass="NXCollection", attrs={}):
        self.root_node = self if parent_node is None else parent_node.root_node
        if path.startswith("/"):
            # absolute path
            self.path = path
        else: 
            # relative
            self.path = os.path.join(parent_node.path, path)
        self.os_path = parent_node.os_path
        preexisting = os.path.exists(os.path.join(self.os_path, self.path.lstrip("/")))        
        if not preexisting:
            os.mkdir(os.path.join(self.os_path, self.path.lstrip("/")))
            attrs['NX_class'] = nxclass.encode('UTF-8')
        #print "making things: ", os.path.join(self.os_path, self.path.lstrip("/"))
        self.attrs = JSONBackedDict(os.path.join(self.os_path, self.path.lstrip("/"), self._attrs_filename))
        self.attrs.update(attrs)
        self.fields = JSONBackedDict(os.path.join(self.os_path, self.path.lstrip("/"), self._fields_filename))
    
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
        thisdir = os.path.join(self.os_path, self.path.lstrip("/"))
        subgroups = [x for x in os.listdir(thisdir) if os.path.isdir(os.path.join(thisdir, x))]
        self.fields._read() # need to get most up to date value from disk
        field_keys = self.fields.keys()
        subgroups.extend(field_keys)
        return subgroups
        
    def items(self):
        keys = self.keys()
        return [(k, self[k]) for k in keys]
    
    def __contains__(self, key):
        return (key in self.keys())
        
    def __delitem__(self, path):
        if not path.startswith("/"):
            path = os.path.join(self.path, path)
        os_path = os.path.join(self.os_path, path.lstrip("/"))
        parent_path = os.path.dirname(path)
        parent_os_path = os.path.join(self.os_path, parent_path.lstrip("/"))
        field_name = os.path.basename(path)
        print "deleting:", field_name, parent_path, os_path, parent_os_path
        if os.path.exists(os_path) and os.path.isdir(os_path):
            # it's a group: remove the whole directory
            shutil.rmtree(os_path)
        elif os.path.exists(parent_os_path) and os.path.isdir(parent_os_path):
            parent_group = Group(self, parent_path)
            print "deleting field: ", parent_group.fields
            if field_name in parent_group.fields:
                del parent_group.fields[field_name]
                if os.path.exists(os.path.join(parent_os_path, field_name + ".dat")):
                    os.remove(os.path.join(parent_os_path, field_name + ".dat"))
            else:
                raise KeyError(field_name)
        else:
            raise KeyError(field_name)
           
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
        parent_path = os.path.dirname(full_path)
        parent_os_path = os.path.join(self.os_path, parent_path.lstrip("/"))
        field_name = os.path.basename(full_path)
        if os.path.exists(os_path) and os.path.isdir(os_path):
            return Group(self, full_path)
        elif os.path.exists(parent_os_path):
            parent_group = Group(self, parent_path)
            if field_name in parent_group.fields:
                if 'target' in parent_group.fields[field_name]:
                    return FieldLink(self, path)
                else:
                    return Field(self, path)
            else:
                raise KeyError(path)
        else:
            # the item doesn't exist
            raise KeyError(path)
    
    def add_field(self, path, **kw):
        Field(self, path, **kw)
        
    def add_group(self, path, nxclass, attrs={}):
        Group(self, path, nxclass, attrs)

class File(Node):
    def __init__(self, filename, mode="r", timestamp=None, creator=None, compression=zipfile.ZIP_DEFLATED, attrs={}, **kw):
        fn = tempfile.mkdtemp()
        self.os_path = fn
        self.root_node = self
        self.filename = filename
        self.mode = mode
        self.compression = compression
        file_exists = os.path.exists(filename)
        if file_exists and (mode == "a" or mode == "r"):
             zipfile.ZipFile(filename).extractall(self.os_path)
        Node.__init__(self, parent_node=self, path="/")        
               
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

    def flush(self):
        # might make this do writezip someday.
        pass
        
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
        Node.__init__(self, parent_node=node, path=path, nxclass=nxclass, attrs=attrs)
        
    def __repr__(self):
        return "<HDZIP group \"" + self.path + "\">"
    
class Field(object):
    _formats = {
        'S': '%s',
        'f': '%.8g',
        'i': '%d',
        'u': '%d',
        'b': '%d'}
    
    def __init__(self, node, path, **kw):
        self.root_node = node.root_node
        self.os_path = node.os_path
        if not path.startswith("/"):
            # relative path:
            path = os.path.join(node.path, path)
        self.path = path
        self.basename = os.path.basename(self.path)
        group_path = os.path.dirname(self.path)
        preexisting = self.basename in self.parent.fields
        print self.basename, group_path
  
        if preexisting:
            self.attrs = self.parent.fields[self.basename]['attrs']
        else:
            self.parent.fields[self.basename] = {"attrs": {}}
            data = kw.pop('data', numpy.array([]))
            attrs = kw.pop('attrs', {})
            attrs['description'] = kw.setdefault('description', None)
            attrs['dtype'] = kw.setdefault('dtype', None)
            attrs['units'] = kw.setdefault('units', None)
            attrs['inline'] = kw.setdefault('inline', True)
            attrs['label'] = kw.setdefault('label', None)
            attrs['binary'] = kw.setdefault('binary', False)
            attrs['byteorder'] = sys.byteorder
            if attrs['dtype'] is None:
                raise TypeError("dtype missing when creating %s" % (path,))
            self.attrs = self.parent.fields[self.basename]['attrs']
            self.attrs.update(attrs)
            if data is not None:
                if numpy.isscalar(data): data = [data]
                data = numpy.asarray(data, dtype=attrs['dtype'])        
                self.value = data
    
    def __repr__(self):
        return "<HDZIP field \"%s\" %s \"%s\">" % (self.basename, str(self.attrs['shape']), self.attrs['dtype'])
    
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
        return self.root_node[os.path.dirname(self.path)]
                    
    @property
    def value(self):
        field = self.parent.fields[self.basename]
        if self.attrs.get('inline', False) == True:
            return field['value']
        else: 
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
        field = self.parent.fields[self.basename]
        if hasattr(data, 'shape'): field['attrs']['shape'] = list(data.shape)
        elif hasattr(data, '__len__'): field['attrs']['shape'] = [data.__len__()]
        if hasattr(data, 'dtype'): 
            formatstr = '<' if attrs['byteorder'] == 'little' else '>'
            formatstr += data.dtype.char
            formatstr += "%d" % (data.dtype.itemsize,)
            field['attrs']['format'] = formatstr            
            field['attrs']['dtype'] = data.dtype.name
        
        #print self.attrs, self.parent.fields, "inline?"
        if field['attrs'].get('inline', False) == True:
            print hasattr(data, 'tolist'), data, str(field['attrs']['dtype'])
            if hasattr(data, 'tolist'): data = data.tolist()
            field['value'] = data
        else:
            field['value'] = {"file": self.path + ".dat"}
            self._write_data(data, 'w')
            
    def _write_data(self, data, mode='w'):
        target = os.path.join(self.os_path, self.path.lstrip("/") + ".dat")
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
        field = self.parent.fields[self.basename]
        if (list(data.shape) != list(field['attrs'].get('shape', [])[1:])):
            raise Exception("invalid shape to append")
        if data.dtype != str(field['attrs']['dtype']):
            if coerce_dtype == False:
                raise Exception("dtypes do not match, and coerce is set to False")
            else:
                data = data.astype(str(field['attrs']['dtype']))
                                
        new_shape = list(field['attrs']['shape'])
        new_shape[0] += 1
        field['attrs']['shape'] = new_shape
        if field['attrs'].get('inline', False) == True:
            # can only overwrite whole data if inline
            self.value = self.value.append(data.tolist())
        else:
            self._write_data(data, mode='a')
        
    def extend(self, data, coerce_dtype=True):
        field = self.parent.fields[self.basename]
        if (list(data.shape[1:]) != list(field['attrs'].get('shape', [])[1:])):
            raise Exception("invalid shape to append")
        if data.dtype != str(field['attrs']['dtype']):
            if coerce_dtype == False:
                raise Exception("dtypes do not match, and coerce is set to False")
            else:
                data = data.astype(str(field['attrs']['dtype']))
                
        new_shape = list(field['attrs']['shape'])
        new_shape[0] += data.shape[0]
        field['attrs']['shape'] = new_shape
        if field['attrs'].get('inline', False) == True:
            # can only overwrite whole data if inline
            intermediate = self.value
            intermediate.extend(data)
            self.value = self.value.extend(data.tolist())
        else:
            self._write_data(data, mode='a')
        
class FieldLink(Field):
    def __init__(self, node, path, target_path=None, **kw):
        if not path.startswith("/"):
            path = os.path.join(node.path, path)
        self.orig_path = path
        
        Field.__init__(self, node, target_path, **kw)
      
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
field = Field
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
