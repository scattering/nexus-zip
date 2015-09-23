import os, sys
import zipfile, tempfile, shutil
import json
from collections import OrderedDict
import numpy
import iso8601

DEFAULT_ENDIANNESS = '<' if (sys.byteorder == 'little') else '>'
__version__ = "0.0.1"


class WithAttrs(object):
    """ File, Group, etc. inherit from here to get access to the attrs 
    property, which is backed by .attrs.json in the filesystem """
    _ATTRS_FNAME = ".attrs"
    @property
    def attrs(self):
        """ file-backed attributes dict """
        return json.loads(open(os.path.join(self.os_path, self.path.lstrip("/"), self._ATTRS_FNAME)).read())

    @attrs.setter
    def attrs(self, value):
        open(os.path.join(self.os_path, self.path.lstrip("/"), self._ATTRS_FNAME), 'w').write(json.dumps(value))

    @attrs.deleter
    def attrs(self):
        """ you can't do this """
        raise NotImplementedError

class WithFields(object):
    """ File, Group, etc. inherit from here to get optional fields property, 
    which is backed by fields.json in the filesystem when fields are present """
    _FIELDS_FNAME = ".fields_attrs"
    @property
    def fields(self):
        """ file-backed attributes dict """
        fpath = os.path.join(self.os_path, self.path.lstrip("/"), self._FIELDS_FNAME)
        fields_out = {}
        if os.path.exists(fpath):
            fields_out = json.loads(open(fpath, 'r').read())
        return fields_out

    @fields.setter
    def fields(self, value):
        open(os.path.join(self.os_path, self.path.lstrip("/"), self._FIELDS_FNAME), 'w').write(json.dumps(value))

    @fields.deleter
    def fields(self):
        """ you can't do this """
        raise NotImplementedError
        
class WithLinks(object):
    """ File, Group, etc. inherit from here to get any defined links, 
    which is backed by links.json in the filesystem when links are present """
    _LINKS_FNAME = "links.json"
    @property
    def links(self):
        """ file-backed attributes dict """
        lpath = os.path.join(self.os_path, self.path.lstrip("/"), self._LINKS_FNAME)
        links_out = {}
        if os.path.exists(lpath):
            links_out = json.loads(open(lpath, 'r').read())
        return links_out

    @links.setter
    def links(self, value):
        open(os.path.join(self.os_path, self.path.lstrip("/"), self._LINKS_FNAME), 'w').write(json.dumps(value))

    @links.deleter
    def links(self):
        """ you can't do this """
        raise NotImplementedError

class Node(WithAttrs,WithFields,WithLinks):
    def __init__(self, parent_node=None, path="/", nxclass=None, attrs={}):
        self.parent_node = parent_node
        self.root_node = self if parent_node is None else parent_node.root_node
        if path.startswith("/"):
            # absolute path
            self.path = path
        else: 
            # relative
            self.path = os.path.join(parent_node.path, path)
            
    @property
    def groups(self):
        groupnames = [x for x in os.listdir(os.path.join(self.os_path, self.path.lstrip("/"))) if os.path.isdir(os.path.join(self.os_path, self.path.lstrip("/"), x))] 
        return dict([(gn, Group(self, gn)) for gn in groupnames])
    
    def __repr__(self):
        return "<HDZIP group \"" + self.path + "\">"
    
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
        #print os_path, full_path
        if os.path.isdir(os_path):
            return Group(self, path)
        else:
            field_name = os.path.basename(full_path)
            group_path = os.path.dirname(full_path)
            os_path = os.path.join(self.os_path, group_path.lstrip("/"))
            #print os.path.isdir(os_path), os_path, group_path
            if os.path.isdir(os_path):
                g = Group(self, group_path)
                #return g.fields[field_name]        
                return FieldFile(g, full_path)
    
    def add_field(self, path, **kw):
        FieldFile(self, path, **kw)
        
    def add_group(self, path, nxclass, attrs={}):
        Group(self, path, nxclass, attrs)

class File(Node):
    def __init__(self, filename, mode, timestamp=None, creator=None, compression=zipfile.ZIP_DEFLATED, attrs={}, **kw):
        Node.__init__(self, parent_node=None, path="/")
        fn = tempfile.mkdtemp()
        # os.close(fd) # to be opened by name
        self.os_path = fn
        self.filename = filename
        self.mode = mode
        self.compression = compression
        file_exists = os.path.exists(filename)
        if file_exists and (mode == "a" or mode == "r"):
             zipfile.ZipFile(filename).extractall(self.os_path)
        
        preexisting = os.path.exists(os.path.join(self.os_path, self.path.lstrip("/")))
        
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
        self.attrs = attrs
    
    def __repr__(self):
        return "<HDZIP file \"%s\" (mode %s)>" % (self.filename, self.mode)
           
    def close(self):
        self.writezip()
        shutil.rmtree(self.os_path)
        
    def writezip(self):
        make_zipfile_withlinks(self.filename, os.path.join(self.os_path, self.path.lstrip("/")), self.compression)
        
    
class Group(Node):
    def __init__(self, node, path, nxclass=None, attrs={}):
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
            self.attrs = attrs

class Field(object):
    _formats = {
        'S': '%s',
        'f': '%.8g',
        'i': '%d',
        'u': '%d' }
        
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

        self.parent_node = node
        self.root_node = node.root_node
        self.os_path = node.os_path
        if path.startswith("/"):
            # absolute path
            self.path = path
        else: 
            # relative
            self.path = os.path.join(node.path, path)
        
        self.name = os.path.basename(self.path)
        group_path = os.path.dirname(self.path)
        preexisting = self.name in self.parent_node.fields
        print "preexisting?", preexisting
        #json.loads(open(os.path.join(full_path, "fields.json"))))
        
        if preexisting:
            self.attrs = self.parent_node.fields[self.name]
        else:       
            attrs = kw.pop('attrs', {})
            attrs['data'] = kw.setdefault('data', {})
            attrs['dtype'] = kw.setdefault('dtype', None)
            attrs['units'] = kw.setdefault('units', None)
            attrs['label'] = kw.setdefault('label', None)
            attrs['shape'] = kw.setdefault('shape', None)
            attrs['inline'] = kw.setdefault('inline', False)
            attrs['binary'] = kw.setdefault('binary', False)
            attrs['byteorder'] = sys.byteorder
            if data is not None:
                self.set_data(data, attrs)
    
    @property
    def value(self):
        field = self.parent_node.fields[self.name]
        if self.attrs.get('inline', False) == True:
            return field['value']
        else:
            target = os.path.join(self.os_path, (field['target']).lstrip("/"))
            print target
            if self.attrs.get('binary', False) == True:
                datastring = open(target, 'rb').read()
                d = numpy.fromstring(datastring, dtype=field['format'])
            else:
                datastring = open(target, 'r').read()
                d = numpy.loadtxt(target, dtype=field['dtype'])
            if 'shape' in field:
                d.reshape(field['shape'])
            return d
                
                
                
    
    def set_data(self, data, attrs=None):
        if attrs is None:
            attrs = self.parent_node.fields[self.name]
        if hasattr(data, 'shape'): attrs['shape'] = data.shape
        if hasattr(data, 'dtype'): 
            formatstr = '<' if attrs['byteorder'] == 'little' else '>'
            formatstr += data.dtype.char
            formatstr += "%d" % (data.dtype.itemsize * 8,)
            attrs['format'] = formatstr
            
        if self.attrs.get('inline', False) == True:            
            if hasattr(data, 'tolist'): data = data.tolist()
            attrs['value'] = data
        else:
            if self.attrs.get('binary', False) == True:
                full_path = os.path.join(self.path + '.bin')
                open(os.path.join(self.os_path, full_path.lstrip("/")), 'w').write(data.tostring())
            else:
                full_path = os.path.join(self.path + '.dat')
                numpy.savetxt(os.path.join(self.os_path, full_path.lstrip("/")), data, delimiter='\t', fmt=self._formats[data.dtype.kind])
            attrs['target'] = full_path
            attrs['dtype'] = data.dtype.name
            attrs['shape'] = data.shape
        parent_fields = self.parent_node.fields
        parent_fields[self.name] = attrs
        self.parent_node.fields = parent_fields
        print self.parent_node.fields

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

class FieldFile(object):
    _formats = {
        'S': '%s',
        'f': '%.8g',
        'i': '%d',
        'u': '%d' }
        
    _data_separator = "=== data ===\n"
        
    def __init__(self, node, path, **kw):
        self.parent_node = node
        self.root_node = node.root_node
        self.os_path = node.os_path
        if path.startswith("/"):
            # absolute path
            self.path = path
        else: 
            # relative
            self.path = os.path.join(node.path, path)
        
        self.name = os.path.basename(self.path)
        group_path = os.path.dirname(self.path)
        preexisting = os.path.exists(os.path.join(self.os_path, self.path.lstrip("/")))
        #print "preexisting?", preexisting
        
        if preexisting:
            pass
        else:
            data = kw.pop('data', None)
            attrs = kw.pop('attrs', {})
            attrs['units'] = kw.setdefault('units', None)
            attrs['label'] = kw.setdefault('label', None)
            attrs['inline'] = kw.setdefault('inline', False)
            attrs['binary'] = kw.setdefault('binary', False)
            attrs['byteorder'] = sys.byteorder
            self.attrs = attrs
            if data is not None:
                self.value = data
                
    @property
    def attrs(self):
        """ file-backed attributes dict """
        json_text = ""
        with open(os.path.join(self.os_path, self.path.lstrip("/")), "r") as infile:
            newline = infile.readline();
            while newline != self._data_separator and newline != "":
                json_text += newline
                newline = infile.readline()
                
        return json.loads(json_text)

    @attrs.setter
    def attrs(self, value):
        fd_out, fd_out_name = tempfile.mkstemp(dir=self.os_path)
        fd_in_name = os.path.join(self.os_path, self.path.lstrip("/"))
        with os.fdopen(fd_out, "w") as outfile:
            outfile.write(json.dumps(value))
            outfile.write("\n" + self._data_separator)
            if os.path.exists(fd_in_name):
                with open(fd_in_name, "r") as infile:
                    newline = infile.readline()
                    while newline != self._data_separator and newline != "":
                        newline = infile.readline()
                    # efficiently copy the data from the old file to the new...
                    shutil.copyfileobj(infile, outfile)
        # then rename the temporary file to the backing file name...
        shutil.move(fd_out_name, fd_in_name)
                
    @property
    def value(self):
        attrs = self.attrs
        if attrs.get('inline', False) == True:
            return attrs.get('value', None)
        else:
            target = os.path.join(self.os_path, self.path.lstrip("/"))
            with open(target, 'rb') as infile:
                # skip the attrs
                newline = infile.readline()
                while newline != self._data_separator and newline != "":
                    newline = infile.readline()
                if self.attrs.get('binary', False) == True:
                    d = numpy.fromfile(infile, dtype=attrs['format'])
                else:
                    d = numpy.loadtxt(infile, dtype=attrs['dtype'])
            if 'shape' in attrs:
                d.reshape(attrs['shape'])
            return d              
    
    @value.setter
    def value(self, data, attrs=None):
        if attrs is None:
            attrs = self.attrs
        if hasattr(data, 'shape'): attrs['shape'] = data.shape
        if hasattr(data, 'dtype'): 
            formatstr = '<' if attrs['byteorder'] == 'little' else '>'
            formatstr += data.dtype.char
            formatstr += "%d" % (data.dtype.itemsize * 8,)
            attrs['format'] = formatstr
            attrs['dtype'] = data.dtype.name
            attrs['shape'] = data.shape
            
        if attrs.get('inline', False) == True:            
            if hasattr(data, 'tolist'): data = data.tolist()
            attrs['value'] = data
            self.attrs = attrs
        else:
            target = os.path.join(self.os_path, self.path.lstrip("/"))
            with open(target, 'wb') as outfile:
                outfile.write(json.dumps(attrs))
                outfile.write("\n" + self._data_separator)
                if attrs.get('binary', False) == True:
                    data.tofile(outfile)
                    #open(target, 'wb').write(data.tostring())
                else:
                    numpy.savetxt(outfile, data, delimiter='\t', fmt=self._formats[data.dtype.kind])
                    
    def append(self, data, dtype=None):
        # add to the data...
        # can only append along the last axis, e.g. if shape is (3,4)
        # it becomes (3,5), if it is (3,4,5) it becomes (3,4,6)
        attrs = self.attrs
        if data.shape != attrs.get('shape', []).slice(None, -1):
            raise Exception("invalid shape to append")
        if dtype is None:
            dtype = attrs['dtype']
        attrs['shape'][-1] += 1
        self.attrs = attrs
        #... unfinished.
        
        
def make_zipfile_withlinks(output_filename, source_dir, compression=zipfile.ZIP_DEFLATED):
    relroot = os.path.abspath(source_dir)
    with zipfile.ZipFile(output_filename, "w", compression) as zipped:
        for root, dirs, files in os.walk(source_dir):
            # add directory (needed for empty dirs)
            for d in dirs:
                dirname = os.path.join(root, d)
                write_item(zipped, relroot, dirname)
            for f in files:
                filename = os.path.join(root, f)
                write_item(zipped, relroot, filename)         
                            
    
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
