# nexus-zip
An implementation of the nexus hierarchical data format in a zipfile (rather than HDF or XML)

For the original format specification, see http://www.nexusformat.org/

## NeXuS now
- Historically, two container formats have been supported for NeXuS: XML and HDF
- The XML container is deprecated officially, but there seems to be active work on supporting it
- The HDF (hierarchical data format) was first supported at version 4.0 and currently the default NeXuS container is HDF5

## HDF container
- The HDF container is a good match for the NeXuS requirements
- Standard NeXuS namespace is linked to instrument device names with HDF soft links and hard links (like a file system)
- Data is stored in the HDF tree (also like a file system), where groups function as folders and datasets are like files. -  Attributes can be added to files and groups easily
- Fast data write/read for very large data sets (NASA)

### HDF limitations
- A cross-platform viewer (in JAVA) is available but not easy to find, and must be installed on each computer where it is used
- No viewer for browser is available
- The size of a file for a scan with many devices (typical of NICE instrument setup) is large, ~100x larger than ICP format

## Proposed: new zip container
- Structure of HDF is like folders with data files in them
- Why not use folders with data files in them instead?
- Zip utilities are available for every platform, including web browsers
- They are installed by default on most OS, so users can just open the file to inspect with no special software.

### NeXuS-zip implementation
- Attributes are added to folders and files  through a JSON-format “.attrs” file
- e.g. folder entry/DAS_logs has attributes stored in entry/DAS_logs/.attrs (a text file)
- dataset entry/DAS_logs/counter/counts has attrs in entry/DAS_logs/counter/counts.attrs
- Datasets can be text (column-format) for 2 dimensions or less, or binary for >2 dims.
- Format and type of data is written in .attrs

### new web capabilities enabled
- Browser can open NeXuS-zip, so live data can just be a regular data file updated often
- Viewer like HDFView can be built in the browser, for inspecting all devices for a dataset
- Seamless web-based data reduction can pull data from public archive for preview, then use
