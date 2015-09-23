import sys
import h5py
import os
import numpy
import zipfile
import tempfile
import simplejson
from collections import OrderedDict
import copy

DEBUG = False

#file_in = sys.argv[1]
#file_out = file_in.replace('.nxs', '.nxz') + '.zip'

def make_dir(path):
    os.mkdir(path)

join_path = os.path.join

def make_metadata(obj, path=''):
    metadata = {}
    for key in obj.keys():
        new_path = join_path(path, key)
        newitem = OrderedDict(obj[key].attrs)
        for k,v in newitem.items(): 
            newitem[k] = numpy.asscalar(v) if isinstance(v, numpy.generic) else v
        if isinstance(obj[key], h5py.Group):
            newitem['members'] = make_metadata(obj[key], new_path)
        else:
            fname = join_path(path, key+'.dat')
            #if max(obj[key].shape) <= 1:
            #    newitem['value'] = obj[key].value.tolist()
        if DEBUG:
            print key
            _ = simplejson.dumps(metadata)
        metadata[key] = newitem
    return metadata

def to_zipfile(obj, zipfile, path=''):
    summary = OrderedDict()
    for key in obj.keys():
        val = obj[key]
        new_path = join_path(path, key)
        if isinstance(val, h5py.Group):
            to_zipfile(val, zipfile, new_path)
        else:
            fname = join_path(path, key+'.dat')
            if 'target' in val.attrs and val.attrs['target'] != join_path('/', path, key):
                print val.attrs['target'], join_path('/', path, key)
                summary[key] = OrderedDict([['target', val.attrs['target']]]) #, ['shape', (obj[val.attrs['target']]).shape]])
            elif numpy.product(val.shape) <= 1:
                summary[key] = val.value.tolist()
            else:
                value = obj[key].value
                formats = {
                    'S': '%s', 
                    'f': '%.8g',
                    'i': '%d',
                    'u': '%d' }
                if value.dtype.kind in formats:
                    fd, fn = tempfile.mkstemp()
                    os.close(fd) # to be opened by name
                    if DEBUG: print fname, value.dtype.kind
                    if len(value.shape) > 2:
                        with open(fn, 'w') as f:
                            simplejson.dump(value.tolist(), f)
                    else:
                        numpy.savetxt(fn, value, delimiter='\t', fmt=formats[value.dtype.kind])
                    zipfile.write(fn, fname)
                    os.remove(fn)
                    summary[key] = OrderedDict([['target', join_path('/', fname)], ['shape',  obj[key].shape]])
                else:
                    print "unknown type of array: ", fname, value.dtype
    zipfile.writestr(os.path.join(path, 'fields.json'), simplejson.dumps(summary, indent='  '))

def to_zip(hdfname, zipname='data.zip'):
    obj = h5py.File(hdfname)
    z = zipfile.ZipFile(zipname, 'w', compression=zipfile.ZIP_DEFLATED)
    to_zipfile(obj, z)
    metadata = make_metadata(obj)
    fd, fn = tempfile.mkstemp()
    os.close(fd) # to be opened by name
    with open(fn, 'w') as f:
        simplejson.dump(metadata, f, indent='  ')
        #simplejson.dump(metadata, f)
    z.write(fn, '.metadata')
    os.remove(fn)
    z.close()

if __name__ == '__main__':
    import sys
    file_in = sys.argv[1]
    file_out = file_in.replace('.nxs', '.nxz') + '.zip'
    to_zip(file_in, file_out)

