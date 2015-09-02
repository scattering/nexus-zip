import os, sys
import zipfile, tempfile, shutil
import json
from collections import OrderedDict
import numpy
import iso8601

DEFAULT_ENDIANNESS = '<' if (sys.byteorder == 'little') else '>'
__version__ = "4.2.1"


class WithAttrs(object):
    """ File, Group, etc. inherit from here to get access to the attrs 
    property, which is backed by .attrs.json in the filesystem """
    _ATTRS_FNAME = ".attrs.json"
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
    _FIELDS_FNAME = "fields.json"
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
        return "<HDF5 ZIP group \"" + self.path + "\">"
    
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
        print os_path, full_path
        if os.path.isdir(os_path):
            return Group(self, path)
        else:
            field_name = os.path.basename(full_path)
            group_path = os.path.dirname(full_path)
            os_path = os.path.join(self.os_path, group_path)
            if os.path.isdir(os_path):
                g = Group(self, group_path)
                return g.fields[field_name]        
    
    def add_field(self, path, **kw):
        Dataset(self, path, **kw)
        
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
        
    def close(self):
        self.writezip()
        shutil.rmtree(self.os_path)
        
    def writezip(self):
        make_root_zipfile(self.filename, os.path.join(self.os_path, self.path.lstrip("/")), self.compression)
        
    
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

class Dataset(object):
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
        
        field_name = os.path.basename(self.path)
        group_path = os.path.dirname(self.path)
        preexisting = field_name in self.parent_node.fields
        print "preexisting?", preexisting
        #json.loads(open(os.path.join(full_path, "fields.json"))))
        
        if preexisting:
            pass
        else:       
            data = kw.pop('data', [])
            dtype = kw.pop('dtype', None)
            shape = kw.pop('shape', None)
            units = kw.pop('units', None)
            label = kw.pop('label', None)
            inline = kw.pop('inline', False)
            binary = kw.pop('binary', False)
            attrs = kw.pop('attrs', {})
      
            self.inline = inline
            self.binary = binary
       
            #os.mkdir(os.path.join(node.os_path, self.path))
            attrs['dtype'] = dtype
            attrs['units'] = units
            attrs['label'] = label
            attrs['shape'] = shape
            attrs['byteorder'] = sys.byteorder
            if data is not None:
                self.set_data(data, attrs)
    
    @property
    def value(self):
        field = self.root_node.fields[self.path]
        if self.inline:
            return field['value']
        else:
            target = field['target']
            if self.binary:
                datastring = open(target, 'rb').read()
                d = numpy.fromstring(datastring, dtype=field['format'])
            else:
                datastring = open(target, 'r').read()
                d = numpy.loadtxt(target, fmt=field['format'])
            if 'shape' in field:
                d.reshape(field['shape'])
            return d
                
                
                
    
    def set_data(self, data, attrs=None):
        if attrs is None:
            attrs = self.parent_node.fields[self.path]
        if hasattr(data, 'shape'): attrs['shape'] = data.shape
        if hasattr(data, 'dtype'): 
            formatstr = '<' if attrs['byteorder'] == 'little' else '>'
            formatstr += data.dtype.char
            formatstr += "%d" % (data.dtype.itemsize * 8,)
            attrs['format'] = formatstr
            
        if self.inline:            
            if hasattr(data, 'tolist'): data = data.tolist()
            attrs['value'] = data
        else:
            if self.binary:
                full_path = os.path.join(self.path + '.bin')
                open(os.path.join(self.os_path, full_path.lstrip("/")), 'w').write(data.tostring())
            else:
                full_path = os.path.join(self.path + '.dat')
                numpy.savetxt(os.path.join(self.os_path, full_path.lstrip("/")), data, delimiter='\t', fmt=self._formats[data.dtype.kind])
            attrs['target'] = full_path
            attrs['dtype'] = data.dtype.name
            attrs['shape'] = data.shape
        parent_fields = self.parent_node.fields
        parent_fields[self.path] = attrs
        self.parent_node.fields = parent_fields
        print self.parent_node.fields

def make_zipfile(output_filename, source_dir, compression=zipfile.ZIP_DEFLATED):
    relroot = os.path.abspath(os.path.join(source_dir, os.pardir))
    print relroot
    with zipfile.ZipFile(output_filename, "w", compression) as zipped:
        for root, dirs, files in os.walk(source_dir):
            # add directory (needed for empty dirs)
            zipped.write(root, os.path.relpath(root, relroot))
            for file in files:
                filename = os.path.join(root, file)
                if os.path.isfile(filename): # regular files only
                    arcname = os.path.join(os.path.relpath(root, relroot), file)
                    zipped.write(filename, arcname)
                    
def make_root_zipfile(output_filename, source_dir, compression=zipfile.ZIP_DEFLATED):
    relroot = os.path.abspath(source_dir)
    with zipfile.ZipFile(output_filename, "w", compression) as zipped:
        for root, dirs, files in os.walk(source_dir):
            # add directory (needed for empty dirs)
            relpath = os.path.relpath(root, relroot)
            if not os.path.samefile(root, relroot):
                zipped.write(root, os.path.relpath(root, relroot))
            for file in files:
                filename = os.path.join(root, file)
                if os.path.isfile(filename): # regular files only
                    arcname = os.path.join(os.path.relpath(root, relroot), file)
                    zipped.write(filename, arcname)                    
    
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
