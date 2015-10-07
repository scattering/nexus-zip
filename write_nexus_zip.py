"""
Data writer for the NeXus file format.
"""
from __future__ import print_function

__all__ = ["Writer"]

import sys
import os
import json
import bisect

from os.path import basename

import numpy

import zipfile

from . import hzf as h5nexus
from . import util
from . import iso8601
from . import quack
from . import writer
from .writer import Writer as BaseWriter

# For recovery mode (i.e., when run from nicereplay), remember the first
# time the entry was created in the session; subsequent writes to the same
# entry should be appended rather than recreated.
RECOVERED_SET = set()

# Hardcoded info in configuration
SAMPLE_GROUP = "sample"  # Must match name in NeXus mapping

RESERVED_ATTRS = set(('mode','name','units','type','label','value','shape'))

@quack.implements(BaseWriter)
class Writer(BaseWriter):
    """
    Writer for the NeXus file format.
    """
    def __init__(self, ext=".nxs", zipped=False):
        self.ext = ext
        self.zipped = zipped
        self.active_scan = None
        self.active_scan_handle = None
        self.scans = {}
        self.tmp_paths = {}
        self.reported_paths = set()

    def configure(self, state):
        # Keep the complete history of each sensor for the duration of the
        # trajectory so that each entry can have the complete data.  The
        # temperature log for an individual entry will only contain data from
        # the start of the entry to the end of the final point, not from the
        # start of the trajectory to the end of the trajectory.
        state.keep_all_sensor_logs = True

    def open_scan(self, state):
        #if self.active_scan:
        #    self.active_scan_handle.close(state,self.zipped)
            
        try:
            basename,entryname = state.scan.split(':',2)
        except:
            basename,entryname = state.scan,"entry"
        path = os.path.join(state.datadir, basename+self.ext)

        # Create path to file.  Note that basename may contain path
        # separators, so we can't just check for state.datadir
        parent = os.path.dirname(path)
        if not os.path.exists(parent):
            try:
                os.makedirs(parent)
            except:
                exc = IOError("Could not create directory %r"%parent)
                exc.__cause__ = None
                raise exc
    
        # Delete existing files the first time they are seen during replay
        if "--recovery" in sys.argv and path not in RECOVERED_SET and os.path.exists(path):
            os.remove(path)
            RECOVERED_SET.add(path)
            
        self.active_scan = path, entryname
        if not self.active_scan in self.scans:
            new_scan = Scan(path, entryname, state, self.tmp_paths.get(path, None))
            self.scans[self.active_scan] = new_scan
            self.tmp_paths[path] = new_scan.h5file.os_path
        self.active_scan_handle = self.scans[self.active_scan]

        if self.zipped:
            path = "%s.zip" % (path)         
                     
        if path not in self.reported_paths:
            self.reported_paths.add(path)
            if not self.zipped:
                 util.report_file_writing(True, path, state.data)           
                
        return self.active_scan

    def close_scan(self, state, scan):        
        #self.reload_scan(state, scan)
        if self.active_scan == scan:
            self.active_scan_handle.close(state,self.zipped)
        self.active_scan = None

    def end_count(self, state, scan):
        #print "end count",point
        ##All points in one entry
        ##point = self.point; self.point += 1
        ##Force counts to change in simulated data
        ##state.record['data'] = dict((k,v+point) for k,v in state.record['data'].items())
        self.reload_scan(state, scan)
        self.active_scan_handle.end_count(state)

    def update_events(self, state, scan):
        self.reload_scan(state, scan)
        self.active_scan_handle.update_events(state)

    def add_note(self, state, scan):
        self.reload_scan(state, scan)
        self.active_scan_handle.add_note(state)

    def reload_scan(self, state, scan):
        if self.active_scan == scan: return
        self.active_scan = scan
        path,entryname = scan
        self.active_scan_handle = self.scans[self.active_scan]

    def end(self, state):
        print ("processing end", self.reported_paths)
        for scan in self.scans:
            self.scans[scan].close(state, self.zipped)
        for path in self.reported_paths:
            util.report_file_writing(False, path, state.data)
        self.reported_paths.clear()
        self.tmp_paths.clear()
        

class Scan(object):
    """
    Internal object representing a scan in the nexus writer.
    """
    def end_count(self, state):
        # Cache fields that are stored in ms but need to be seconds
        if self.time_fields is None: 
            self.time_fields = state.time_fields()
        
        # generate sensor statistics from sensor data
        sensor_data = {}
        for s in self.sensor_list:
            # Get logs for each sensor during the point, and compute statistics
            data = state.sensor_logs.get(s, [])
            stats = _sensor_stats(data, state.data[s])
            for k,v in stats.items():
                sensor_data[s+"."+k] = v

        # store the fields
        #print self.das,"ending counts",point
        links_to_update = []
        for source,dataset in sorted(self.fields.items()):
            # Normal data is in state.data, but sensor summary statistics must
            # be retrieved from sensor_data.
            #print "source",source,fields.get(source,"not available")
            try: value = state.data[source]
            except KeyError: value = sensor_data.get(source, None)
            # Make sure times are delta seconds
            if source in self.time_fields and value is not None:
                value = 0.001*(value - self.start)
            dataset.store(value, self.point, links_to_update)
        h5nexus.update_hard_links(self.das.parent, links_to_update)

        # Store environment data collected while the point was being measured
        self._write_sensor_logs(state)
        # Remember when the last point ended; this must be after write_sensor_logs
        # in order for the logic to work on reloaded entries.
        self.end = state.record['time']  
        self.collection_time += state.data.get('counter.liveTime',0)
        self._update_timestamps()

        self.point += 1
        if self.point == 1:
            self.render_entry(state)

        self._write_error_log(state, self.point, 'error',
                              state.current_errors-state.config_errors)
        self._write_error_log(state, self.point, 'warning',
                              state.current_warnings-state.config_warnings)


        # Flush buffers after every point is written
        #print "---- flush"
        self.h5file.flush()

    def update_events(self, state):
        pass

    def add_note(self, state):
        if "notes" not in self.das:
            h5nexus.group(self.das, "notes", 'NXcollection')
        path = 'notes/note%d'%self.point
        if path in self.das:
            for ext in 'abcdefghijklmnopqrstuvwxyz':
                if path+ext not in self.das: break
            else:
                raise RuntimeError("More than 27 notes for one point not supported")
            path = path + ext
        h5nexus.group(self.das, path, 'NXnote')
        h5nexus.field(self.das[path], 'date', data=state.timestamp, dtype='|S')
        h5nexus.field(self.das[path], 'type', data=state.record['mimetype'], dtype='|S')
        h5nexus.field(self.das[path], 'description',  dtype='|S',
                      data=state.record['description'])
        if state.record['mimetype'] == 'application/json':
            data = json.dumps(state.record['mimedata'])
        else:
            data = state.record['mimedata']
        h5nexus.field(self.das[path], 'data', data=data, dtype='|S')
        h5nexus.field(self.das[path], 'point', data=self.point, units="", dtype='int32')
        
    def close(self, state, zipped):
        # Note: sensor values after the last point are not required
        # self._write_sensor_readings()
        
        if self.h5file is not None:
            
            if zipped:
                file_path = self.h5file.filename           
            self.h5file.close()
            self.h5file = None         
            
            if zipped:
                try:
                    self.zip_file(file_path)
                    os.remove(file_path)
                    util.report_file_writing(True, "%s.zip" % (file_path), state.data)
                except Exception as e:
                    writer.warn(e)
                    
            # Eventually release the handles we are holding within the file
            del self.fields
            del self.das
    
    def zip_file(self,file_path):
        
        zf = zipfile.ZipFile("%s.zip" % (file_path), "w",zipfile.ZIP_DEFLATED)
        zf.write(file_path,basename(file_path))
        zf.close()
    
    def __init__(self, path, entry_name, state, tmp_path=None):
        # Things to remember between calls

        # all scan data goes to the DAS_logs, so remember where it is
        # location->value map for default values stored at every point
        self.fields = {}
        self.sensor_list = {} # device.field: index
        self.time_fields = None
        self.scanning = True #'Scan' in record
        self.entry_name = entry_name
        
        #print "working on",path
        self.h5file = h5nexus.open(path, mode='a', creator='NICE data writer', os_path = tmp_path)
        #print self.h5file.keys()
        
        if self.entry_name in self.h5file:
            #print "> reloading",path,entry_name
            self.reload_entry(state)
        else:
            #print "> creating",path,entry_name
            self.create_entry(state)

    def create_entry(self, state):
        """
        Create a new entry for the scan.
        """
        self.start = state.record['time']
        self.end = self.start
        self.collection_time = 0.
        self.point = 0
        
        entry = h5nexus.group(self.h5file, self.entry_name, 'NXentry')        
        h5nexus.field(entry,'start_time', dtype='|S',
                      data=iso8601.format_date(self.start*0.001, precision=3),
                      label='measurement start time')
        h5nexus.field(entry,'program_name', dtype='|S',
                      data=state.data.get('trajectory.program',''),
                      label="program",
                      attrs={'version': state.data.get('trajectory.version',''),
                             'configuration': state.data.get('trajectory.command','')})
        self.das = h5nexus.group(entry, 'DAS_logs', 'NXcollection')
        self._update_timestamps()
        
        for k,v in state.devices.items():
            self.create_device(state, k, v)
        self._write_error_log(state, 0, 'error', state.config_errors)
        self._write_error_log(state, 0, 'warning', state.config_warnings)

    def reload_entry(self, state):
        """
        Reload an already existing entry for the scan
        so that new points can be appended.
        
        There is no check that the new instrument configuration matches the 
        stored configuration; new devices will not be written.
        """
        entry = self.h5file[self.entry_name]
        self.das = entry['DAS_logs']
        self.start = int(1000*iso8601.seconds_since_epoch(util.bytes_to_str(entry['start_time'].value[0])))
        self.end = int(1000*iso8601.seconds_since_epoch(util.bytes_to_str(entry['end_time'].value[0])))
        self.collection_time = entry['collection_time'].value[0]
        self._update_timestamps()
        for k,v in state.devices.items():
            self.reload_device(state, k, v)
        self.point = self.das['counter/startTime'].shape[0]
        
    def render_entry(self, state):
        #print "creating instrument"
        links = build_static_tree(self.das, self.das.parent,
                           state.nexus['entry$NXentry'], state.data)
        make_links(self.das.parent, links)
        self.create_nxdata(state)

    def create_device(self, state, name, device):
        """
        Create a device field within the DAS collection.

        See :ref:`positioner-devices` for details.
        """
        #print "create device",name
        attrs = {'description': device['description']}
        if 'primary' in device:
            attrs['primary'] = device['primary']
        
        h5nexus.group(self.das, name, 'NXcollection', attrs=attrs)
        

        # Create a slot for each of the subfields; turn errors into warnings,
        # but don't stop creating the rest of the file
        for field_name,field in device['fields'].items():
            try:
                self.create_field(state, name, field_name, field)
            except:
                writer.warn("error while creating %s/%s"%(name,field_name), trace=True)
            
    def create_field(self, state, device_name, field_name, field):
        source = "%s.%s"%(device_name,field_name)
        target = "%s/%s"%(device_name,field_name)
        Ftype = field.get('type','float32')
        Funits = util.ascii_units(field.get('units',''))
        if Ftype.endswith("[]"): Ftype = Ftype[:-2]
        Flabel = field.get('label', _label(device_name, field_name, Funits))
        if not Funits and 'label' not in field: Flabel=None
        Fmode = field.get('mode','configure')
        # Look for additional attributes such as description
        Fattrs = dict((k,self._resolve_attr_links(state, v)) 
                      for k,v in field.items() if k not in RESERVED_ATTRS)
        # Remove optional attributes
        Fattrs = dict((k,v) for k,v in Fattrs.items() if v is not None)

        if Ftype == 'time': 
            # Remember this is a time field so that we can compute delta
            # from the start of scan before storing and set the units to
            # seconds, not milliseconds (and not blank, which is what the
            # stream currently claims).  Precision on the time field is ms
            Funits = 's'
            Fattrs['error'] = 0.001
            Fattrs['start'] = iso8601.format_date(self.start*0.001, precision=3)

        def _convert_string_list(s):
            #return util.str_to_bytes('\n'.join(s))
            return [util.str_to_bytes(si) for si in s]
        if Fmode == 'configure':
            Fdata = state.data[".".join((device_name,field_name))]
            #print "configure",target,Ftype,Fdata
            if Ftype.startswith('map'):
                h5nexus.group(self.das,target,"NXcollection")
                kdata,vdata = Fdata
                # Hack to support configuration maps.
                has_subkeys = (Ftype == "map<string,map<string,string>>")
                if has_subkeys:
                    ktype,stype,vtype = 'string','string','string'
                    kdata = _convert_string_list(kdata)
                    vdata = [_convert_string_list(vi) for vi in vdata]
                    maps = [dict((k,v) for k,v in zip(M[::2],M[1::2]))
                            for M in vdata]
                    sdata = list(sorted(set.union(*(set(M.keys()) for M in maps))))
                    vdata = [[M.get(k,b'') for k in sdata] for M in maps]
                else:
                    ktype,vtype = Ftype[4:-1].split(',')
                    sdata,stype = [],'string'
                    if ktype == 'string': kdata = _convert_string_list(kdata)
                    if vtype == 'string': vdata = _convert_string_list(vdata)

                #print("kdata",kdata)
                #print("vdata",vdata)

                ktype,stype,vtype = [_nicetype_to_dtype(t) for t in (ktype,stype,vtype)]
                h5nexus.field(self.das,target+"/key", data=kdata,
                              units='', label=Flabel+' key', dtype=ktype)
                if has_subkeys:
                    h5nexus.field(self.das,target+"/key2", data=sdata,
                                  units='', label=Flabel+' key 2', dtype=stype)
                h5nexus.field(self.das,target+"/value", data=vdata,
                              units='', label=Flabel+' value', dtype=vtype)
            elif Ftype == 'json':
                self._save_json(target, Fdata, Flabel)

            else:
                # Note: if item is large, we may want to allow compression                        
                dtype=('|S' if Ftype.startswith('string') else Ftype)
                if dtype == '|S':
                    if isinstance(Fdata,list):
                        Fdata = _convert_string_list(Fdata)
                    else:
                        Fdata = util.str_to_bytes(Fdata)
                h5nexus.field(self.das, target, data=Fdata, units=Funits,
                              label=Flabel, attrs=Fattrs, dtype=dtype)
        elif Fmode == 'log':
            self.create_log(device=device_name, sensor=field_name, units=Funits,
                            label=Flabel, dtype=Ftype, attrs=Fattrs)
        elif Fmode in ('state','counts'):
            if Fmode == 'counts':
                Fattrs['signal'] = 1
                # Note: for area detector counts, we could peek into the
                # nexus map for the detector which is linked to this device
                # and find the x_offset/y_offset for the node.  Axes needs
                # to be set as ":".join([scan_axis(),"x_offset","y_offset"])
                # and x_offset/y_offset need to be linked into the NXdata group.
                Fattrs['axes'] = self.scan_axis(state)
            #print "create dataset",target,Funits,Flabel,Ftype
            data = Dataset(root=self.das,
                           path=target,
                           units=Funits,
                           label=Flabel,
                           dtype=Ftype,
                           attrs=Fattrs,
                           scanning=self.scanning)
            self.fields[source] = data
        else:
            raise ValueError("Expected mode for %r to be devices|log|state, not %r"
                             % (source, Fmode))

    def create_nxdata(self, state):
        """
        Create NXdata class in entry.
        """
        nxdata = h5nexus.group(self.das.parent, 'data', 'NXdata')

        # identify columns
        counters = [".".join((deviceID,'counts'))
                    for deviceID,device in state.devices.items()
                    if device['type'] == 'logical_counter']
        nodes = state.data['trajectory.scannedVariables']
        nodes.extend(state.primary_sensors())
        nodes.extend(counters)
        
        # add all columns to the NXdata block, exclude duplicates
        for nodeID in sorted(set(nodes)):
            deviceID, fieldID = nodeID.split('.', 1)
            source = "/".join((deviceID,fieldID))
            if state.devices[deviceID].get('primary',None) == fieldID:
                target = "/".join((nxdata.name,deviceID))
            else:
                target = "/".join((nxdata.name,nodeID.replace('.','_')))
            #print("link",source,target)
            h5nexus.link(self.das[source], target)

    def scan_axis(self, state):
        try: return self._cached_scan_axis
        except AttributeError: pass
        try:
            self._cached_scan_axis = self._guess_scan_axis(state)
        except Exception as exc:
            writer.warn("%s (%s) while identifying scan axis"%
                        (exc.__class__.__name__, str(exc)))
            self._cached_scan_axis = 'counts.startTime'
        return self._cached_scan_axis
            
    def _guess_scan_axis(self, state):
        scanID = state.data.get('trajectory.defaultXAxisPlotNode', '')
        if not scanID:
            scanVars = state.data.get('trajectory.scannedVariables', '')
            if scanVars:
                scanID = scanVars[0]
            else:
                controlVars = state.data.get('trajectory.controlVariables', '')
                if controlVars:
                    scanID = controlVars[0]
                else:
                    scanID = 'counts.startTime'
        deviceID,fieldID = scanID.split('.',1)
        if deviceID == 'counts':
            scanID = fieldID
        elif state.devices[deviceID].get('primary',None) == fieldID:
            scanID = deviceID
        else:
            scanID = scanID.replace('.','_')
        return scanID
        
    def normalization_node(self, state):
        """
        Normalization data required to make a sensible plot.  This will usually
        be monitor counts, but it could be time or even another detector,
        depending on what is in the trajectory.
        
        Note: normalization_node is not currently written to the file, but this
        function outlines the logic that the reduction application could use
        to compute the default reduced data.
        """
        try: return self._cached_normalization_node
        except AttributeError: pass
        try:
            self._cached_normalization_axis = self._guess_normalization_node(state)
        except Exception as exc:
            writer.warn("%s (%s) while identifying normalization channel"%
                        (exc.__class__.__name__, str(exc)))
            self._cached_normalization_node = 'counter.liveMonitor'
        return self._cached_normalization_node

    def _guess_normalization_node(self, state):
        normID = state.data.get('trajectory.defaultYAxisNormalizationNode', '')
        normChannel = state.data.get('trajectory.defaultYAxisNormalizationChannel', '')
        if not normID:
            countAgainst = state.data['counter.countAgainst']
            normID = 'counter.liveTime' if countAgainst == 'TIME' else 'counter.liveMonitor'
        deviceID,fieldID = normID.split('.', 1)
        if fieldID == 'counts':
            size = 1
            for di in state.data[deviceID+".dimension"]:
                size *= di
            if size != 1:
                if normChannel == '': normChannel = 0
                normSource = normID
                normID = deviceID+".normChannel"
                # Need to create normID from normSource[normChannel] when
                # done writing trajectory
                self._norm_channel_slice = normID,normSource,normChannel
        return normID

    def _get_slice(self, node, channel):
        if len(node.shape) > 1 and channel is not None and channel >= 0:
            # create a new dataset corresponding to the desired array slice of the y node data
            if len(node.shape) == 2:
                data = node[:, channel]
            elif len(node.shape) == 3:
                data = node[:, channel%node.shape[1], channel/node.shape[1]]
            else:
                raise NotImplementedError('Unsupported number of array dimensions, can\'t translate indices.')
        else:
            data = node[:]
        return data
                
    
    def _save_json(self, target, Fdata, Flabel):
        if isinstance(Fdata, list):
            if all(isinstance(v, int) for v in Fdata):
                h5nexus.field(self.das, target, units="",
                              data = numpy.asarray(Fdata, 'int32'),
                              label = Flabel, dtype = 'int32')
            elif all(isinstance(v, (float,int)) for v in Fdata):
                h5nexus.field(self.das, target, units="",
                              data = numpy.asarray(Fdata, 'float32'),
                              label = Flabel, dtype = 'float32')
            elif all(_isstr(v) for v in Fdata):
                h5nexus.field(self.das, target,
                              data = numpy.asarray(Fdata, '|S'),
                              label = Flabel, dtype = '|S')
            else:
                h5nexus.field(self.das, target, 
                              data = json.dumps(Fdata), 
                              label=Flabel, dtype="|S")
        elif isinstance(Fdata, float):
            h5nexus.field(self.das, target, units="", 
                          data=Fdata, label=Flabel, dtype='float32')
        elif isinstance(Fdata, int):
            h5nexus.field(self.das, target, units="",
                          data=Fdata, label=Flabel, dtype='int32')
        elif _isstr(Fdata):
            h5nexus.field(self.das, target, 
                          data=Fdata, label=Flabel, dtype='|S')
        else:
            h5nexus.field(self.das, target, 
                          data=json.dumps(Fdata), label=Flabel, dtype='|S')

    def reload_device(self, state, name, device):
        for Fname, F in device['fields'].items():
            source = "%s.%s"%(name,Fname)
            target = "%s/%s"%(name,Fname)
            Fmode = F.get('mode','configure')
            
            if Fmode == 'log':
                self.reload_log(device=name, sensor=Fname)
            elif Fmode in ('state','counts'):
                self.fields[source] = Dataset(self.das, target)
        
    def _resolve_attr_links(self, state, attr):
        """
        If the SDS attribute is a link into the DAS, return the DAS value, otherwise
        simply return the attr value.
        """
        if _isstr(attr):
            if attr.startswith('->?'):
                return state.data.get(attr[3:], None)
            elif attr.startswith('->'):
                return state.data.get(attr[2:], "")
        return attr

    def create_log(self,device,sensor,units,label,dtype,attrs):
        """
        Create a sensor group in the DAS logs.
        """
        self.sensor_list[device+"."+sensor] = -1 # indicate new entry
        #print "create_log",device,sensor,units,label,dtype,attrs
        # Create the DAS entry for the sensor
        logpath = "/".join((device,sensor))
        log = h5nexus.group(self.das, logpath, 'NXlog')

        # Need a time field and a value field
        h5nexus.field(log, 'time', maxshape=[None], units='s', dtype='float32',
                    attrs={'start':iso8601.format_date(self.start*0.001, precision=3)},
                    label=_label(label, 'measurement time', 's'))
        h5nexus.field(log, 'value', maxshape=[None], units=units, dtype=dtype,
                    attrs=attrs, label=label)
        for _Sfunction,Sfield,Slabel in _SENSOR_STATS:
            # Map Temp.Sensor1.avg to Temp/Sensor1/average_value, etc.
            source = ".".join((device,sensor,Sfield))
            target = "/".join((device,sensor,Sfield))
            self.fields[source] = Dataset(root=self.das,
                                          path=target,
                                          dtype='float32',
                                          units=units,
                                          label=_label(label,Slabel,units),
                                          scanning=self.scanning)

    def reload_log(self,device,sensor):
        # Index is 0 
        self.sensor_list[device+"."+sensor] = -2 # indicate reloaded entry
        for _Sfunction,Sfield,_Slabel in _SENSOR_STATS:
            source = ".".join((device,sensor,Sfield))
            target = "/".join((device,sensor,Sfield))
            self.fields[source] = Dataset(self.das, target)
        

    def _write_error_log(self, state, point, level, messages):
        if not messages: return
        messages = dict(messages)
        #print "errors",self.point,messages
        if "error_log" not in self.das:
            h5nexus.group(self.das, "error_log", 'NXcollection')
        path = 'error_log/%s%03d'%(level,self.point)
        h5nexus.group(self.das, path, 'NXnote')
        h5nexus.field(self.das[path], 'date', data=state.timestamp, dtype='|S')
        h5nexus.field(self.das[path], 'type', data='application/json', dtype='|S')
        h5nexus.field(self.das[path], 'description', data='%s messages from DAS devices'%level, dtype='|S')
        h5nexus.field(self.das[path], 'data', data=json.dumps(messages), dtype='|S')
        h5nexus.field(self.das[path], 'point', data=point, units="", dtype='int32')

    def _write_sensor_logs(self, state):
        """
        Write any new sensor values to the active sensors.
        
        Note: this must happen before the "end" attribute is updated so that
        it can include any points since the last counts ended.
        """
        #print "=============",state.record['time']-self.start,self.start,self.end-self.start
        for sensor, index in self.sensor_list.items():
            target = sensor.replace('.','/')
            data = state.all_sensor_logs.get(sensor,None)
            #print sensor, data
            #print "updating",sensor,"from",index,"to",(len(data) if data else 0)
            if data and len(data) > index:
                # Update index for next round.  Do this before checking if
                # this is the first update (i.e., index == -1) so that we
                # can short circuit with continue
                self.sensor_list[sensor] = len(data)
                if index == -1: # start entry
                    # if this is the first update, lookup the start time of the
                    # entry in the logs, and include the first value before it.
                    time,_,_,_ = zip(*data)
                    index = bisect.bisect_right(time, self.start) - 1
                    if index < 0: index = 0
                    #print "first lookup",[t-self.start for t in time],index
                    if index >= len(data): continue
                elif index == -2: # reload entry
                    # on reload, start recording the logs at the first log
                    # after the end of the last point
                    time,_,_,_ = zip(*data)
                    index = bisect.bisect_right(time, self.end)
                    #print "reload lookup",self.end-self.start,[t-self.start for t in time],index
                    if index >= len(data): continue

                # grab data since last index and convert to arrays
                time,value,_validity,_msg = zip(*data[index:])
                value = numpy.asarray(value)
                time = 0.001*(numpy.asarray(time) - self.start)
                # Add the arrays to the end of the log field
                #print "+++",sensor,value,time
                h5nexus.extend(self.das[target+"/time"], time)
                h5nexus.extend(self.das[target+"/value"], value)

    def _update_timestamps(self):
        """
        Write end time, duration and collection time after each point.
        """
        end_time_str = iso8601.format_date(self.end*0.001, precision=3)
        if 'end_time' in self.das.parent:
            self.das.parent['end_time'][:] = util.str_to_bytes(end_time_str)
            self.das.parent['duration'][0] = (self.end-self.start)*0.001
            self.das.parent['collection_time'][0] = self.collection_time
        else:
            h5nexus.field(self.das.parent,'end_time',
                          data=end_time_str, dtype='|S',
                          label='measurement end time')
            h5nexus.field(self.das.parent,'duration',
                          data=[(self.end-self.start)*0.001],
                          units='s', dtype='float32',
                          label='total measurement duration')
            h5nexus.field(self.das.parent,'collection_time',
                          data=[self.collection_time],
                          units='s', dtype='float32',
                          label='total time detectors were active')


def _nicetype_to_dtype(nicetype, attrs=None):
    if nicetype == "enum":
        size = max(len(s) for s in attrs['options'].split('|'))
        dtype = "|S%d"%size
    elif nicetype.startswith("string"):
        dtype = "|S"
    #elif nicetype == "string[]":
    #    dtype = "|S"
    elif nicetype == "time":
        dtype = "float32"
    elif nicetype == "bool":
        dtype = "uint8"
    elif nicetype == "json":
        dtype = "object"
    elif nicetype.endswith("[]"):
        dtype = nicetype[:-2]
    else:
        dtype = nicetype
    return dtype

class Dataset(object):
    """
    Dataset hides the complexity of creating the right kind of field
    in the NeXus file.

    *root* : H5 group

        Base group (e.g., file['entry/DAS_logs']) from which data path
        is specified.

    *path* : string

        Target location (e.g., 'A3/set') relative to root.  The field
        isn't created until valid data is available to write.

    *units* : string

        Units on the data, if available.

    *label* : string

        Label to use on data axis when field is plotted.  This is usually
        "field (units)" or "field subfield (units)".

    *dtype* : numpy.dtype

        Type of the data column. The default type is 'float32'.  Strings
        are represented by '|S#' for a fixed length set of strings that
        can be stored in a list, or simply as 'string'.

    *default* : value

        Default value for the field.

    *scanning* : boolean

        True if this is a scanned measurement, false otherwise.  For
        scanned measurements, vector and array fields are represented
        by [Np x Ni x Nj ... ] where Np is the number of points in the
        scan and (Ni, Nj, ...) is the shape of each point and scalars
        are of size [Np].  For sit-and-count measurements, the Np
        dimension is not included on vectors and arrays, and scalars are
        of size [1].

    To write data to the field, use the following::

        field.store(value)

    This will only create and write the field if a valid value is
    stored.  The value will be stored as a scalar field unless it is
    seen to change during the scan, in which case the scalar field
    will be replaced by a compressed extensible field, with the initial
    field value repeated once for all points already stored in the scan.
    """
    def __init__(self, root, path, units=None, label=None, dtype=None,
                 default=None, attrs={}, scanning=True):
        self.root = root
        self.path = path

        if path in root:
            self._load(root[path])
            self.dtype = _nicetype_to_dtype(self.dtype, attrs)
            self._assign_default(default)
            return
        
        self.units = util.ascii_units(units)
        try:
            self.dtype = _nicetype_to_dtype(dtype, attrs)
        except:
            h5nexus.annotate_exception("when saving %r as type %s"%(path,str(dtype)))
            raise
            
        #print "type",self.path,self.dtype, dtype
        self.label = label
        self.scanning = scanning
        self.attrs = attrs
        self._assign_default(default)

    def _assign_default(self, default):
        # Setting default before calling _load
        if default is None:
            # Set default to 0 for numbers and "" for string
            if self.dtype.startswith('|S'): # string
                default = [""]
            else:
                default = [0]
        elif numpy.isscalar(default):
            default = [default]
        self.default = numpy.asarray(default, dtype=self.dtype)

    def store(self, original, point, links_to_update):
        """
        Store a new point in the data set. The field is initially stored
        as a scalar, but is converted to a vector the first time a new
        value is encountered.  Since hardlinks will need to be updated
        when the scalar is converted to a vector, the new vector field
        will be appended to *links_to_update*.
        """
                
        # On the first point (point==0), this creates a scalar field.  On 
        # subsequent points, if the new value is identical to all previous 
        # values, then the field remains scalar.  The first new value causes 
        # the old field to be destroyed and replace by a new vector field.  
        # Subsequent values are always stored to the vector field, even if 
        # they are the same as the prior value.
        
        #echo = (self.path == "sampleThetaMotor/desiredSoftPosition")
        #if echo: print "storing",self.path,point,value
        # Note: conversion could fail for user variables
        try:
            value = numpy.asarray([original], dtype = self.dtype)
        except:
            value = self.default
            writer.warn("could not interpret value for %r as %s: %s"
                        % (self.path,str(self.dtype),original))

        #print "point",point; raise KeyboardInterrupt
        #print "storing",point,self.path,self.dtype,value
        if point == 0:
            # Set type for JSON field to type of the first value.
            if self.dtype == "object":
                try: value = numpy.asarray([original])
                except: pass
                if value.dtype == "object":
                    #print value.dtype, str(value.dtype), type(value), type(value[0])
                    writer.warn("json value is not a basic type for %r: %s"
                                % (self.path, value[0]))
                    self.default = str(value)
                    self.dtype = "|S"
                else:
                    self.dtype = str(value.dtype)
                    self.default = value
                if self.dtype.startswith('|S'):
                    self.default = util.str_to_bytes(self.default)
                    
            #import sys; print >>sys.stderr,"units",self.units
            #import sys; print >>sys.stderr,"label",self.label
            #import sys; print >>sys.stderr,"attrs",self.attrs
            #import sys; print >>sys.stderr,"value",value

            #print "init field",self.path
            h5nexus.field(self.root, self.path, data=value,
                          units=self.units, dtype=self.dtype,
                          label=self.label, attrs=self.attrs)
            #print self.root[self.path]
            # _first starts out as the initial value, or the default
            #print "first point",self.path,value
            self._first = value.copy()
            #print("first",self.path,self._first,self._first.shape)
        elif self._first is None:
            # _first is only None if we have already converted field to
            # an extensible record.
            #print "append to",self.path,id(self)
            try: h5nexus.extend(self.root[self.path], value)
            except Exception as exc:
                # If there was an error writing the value, write the default instead
                writer.warn(str(exc))
                h5nexus.extend(self.root[self.path], self.default)
        elif self._first.shape != value.shape:
            writer.warn("incompatible data in column %r: %s and %s"
                        %(self.path,self._first,value[0]))
        elif not util.equal_nan(self._first, value).all():
            # Value is not last value so we are turning a scalar into
            # a vector; be sure to repeat the scalar once for each point
            # that has already past before appending the current value.
            # Points are numbered from 0, so point is 1 for the second
            # point.
            #print "creating",self.root,self.path,"at point",point
            #print self.root,self.path,value.shape
            #print "extending",self.path,"from",self._first,"with",value,"at",point
            try:
                data = numpy.concatenate([self._first]*point+[value],axis=0)
            except:
                # This is the first non-equal point and it failed, so just
                # warn and pretend that it is still equal, and don't extend
                # the field.
                writer.warn("incompatible data in column %r: %s and %s"
                            %(self.path,self._first,value[0]))
                return
                
            # Note: assert_array_equal compares NaN as equal
            #if echo: print "extending",self.path,self._first, "with",value
            #print numpy.isnan(self._first), numpy.isnan(value)
            #print "deleting",self.path,self.root[self.path]
            try:
                del self.root[self.path]
            except:
                h5nexus.annotate_exception('when deleting %r'%self.path)
                raise                

            maxshape = list(data.shape)
            maxshape[0] = None
            new_node = h5nexus.field(self.root, self.path, data=data,
                          compression=9, maxshape=maxshape,
                          units=self.units, dtype=self.dtype,
                          label=self.label, attrs=self.attrs)
            #print "vector",self.path,self.value
            links_to_update.append(new_node)

            # Set the single to None to indicate that we now have a
            # vector rather than a scalar.
            self._first = None
        else:
            pass
            #if echo: print "not changed", self.path, self._first, value
    
    def _load(self, node):
        """
        Reload the dataset from the file, and prepare to append.
        """
        self.dtype = str(node.dtype)
        self.label = None
        self.units = None
        self.attrs = {}
        for k,v in node.attrs.items():
            if k == 'units': self.units = v
            elif k == 'long_name': self.label = v
            else: self.attrs[k] = v
        if node.value.shape[0] == 1:
            self._first = node.value
        else:
            self._first = None
        #if node.name.endswith('trajectoryData/i'):
        #    print "Loading",node.name, self._first
        

def _label(a,b,units):
    """
    Create a label like 'A3 setpoint (degrees)'
    """
    #print "label a=%r,b=%r,%r"%(a,b,units)
    if a: a = " ".join(a.split("_")).capitalize()
    if b: b = " ".join(b.split("_"))
    if units:
        return ' '.join(v for v in (a,b,'(%s)'%units) if v)
    else:
        return ' '.join(v for v in (a,b) if v)
def _sensor_stats(sensor_data, prior):
    """
    Convert a set of sensor data for a point into statistics for that sensor.
    """
    # Keep only the good values
    values = [vi for _time,vi,status,_msg in sensor_data if status==0]
    # If no good values, use the last value
    if len(values) == 0:
        values = [prior]
    return dict((field,fn(values)) for fn,field,_ in _SENSOR_STATS)
# Table of function, nexus name, and statistic label for sensor stats
_SENSOR_STATS = [
    (numpy.mean,'average_value','mean'),
    (numpy.std,'average_value_error','1-sigma variation'),
    (numpy.min,'minimum_value','minimum'),
    (numpy.median,'maximum_value','maximum'),
    (numpy.max,'median_value','median'),
    ]


def build_static_tree(das, path, config, data):
    """
    create group returning links to be made at the end
    """
    #print "creating group",path,config
    if not config: return []
    links = []
    for k,v in config.items():
        #print "devices",k,v
        if k.endswith("$NXlink"):
            target_path = v
            link_path = "/".join((path.name,k[:-7]))
            links.append((target_path, link_path))
        elif k.endswith("$DASlink"):
            das_path = v.replace('.','/')
            if das_path in das:
                target = das[das_path]
                link_path = "/".join((path.name,k[:-8]))
                links.append((target.name,link_path))
        elif "$NX" in k:
            # If group is empty don't create it
            if v is not None:
                # subgroup name$NXclass
                name, nxclass = k.split('$')
                #print "creating",name,path
                p = h5nexus.group(path, name, nxclass)
                links += build_static_tree(das, p, v, data)
        else:
            try:
                build_static_field(path, k, v)
            except:
                writer.warn("error while creating %s"%path.name, trace=True)                
    return links

def build_static_field(path, name, conf):
    #print "make field",path,name,conf
    dtype = conf.get('type',None)
    value = conf.get('value', None)
    attrs = conf.get('attrs', {})
    if attrs is None: attrs = {}
    if 'units' in attrs:
        units = attrs['units'].get('value', '')
        if units is None: units = ""
    else:
        units = ""
    if 'long_name' in attrs:
        label = attrs['long_name'].get('value','')
    else:
        try: deviceID,nodeID = name.split('.', 1)
        except ValueError: deviceID,nodeID = name, ''
        label = _label(deviceID, nodeID, units)
    if value is None:
        raise KeyError("missing value for %r at %r"%(name,path))
    if dtype is None:
        dtype = '|S' if _isstr(value) else 'float32'
    else:
        if "[" in dtype:
            dtype = dtype.split("[")[0]
        dtype = {'NX_CHAR':'|S','NX_FLOAT32':'float32','NX_INT32':'int32'}[dtype]

    if dtype == '|S':
         value = util.str_to_bytes(value)
    attrs = dict((k,v['value']) for k,v in attrs.items()
                 if k not in set(('units','long_name')))
    h5nexus.field(path, name, data=value, units=units, dtype=dtype,
                label=label, attrs=attrs)

def make_links(entry, links):
    for target_path,link_path in links:        
        #print "linking",sourcepath,target
        try:
            target = entry[target_path]
        except KeyError:
            # ignore missing links
            continue
        h5nexus.link(target,link_path)

def _isstr(s): return isinstance(s, str)
