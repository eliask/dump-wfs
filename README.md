# dump-wfs
A Python program for dumping all features from a given layer in a WFS server as GeoJSON

Dependencies:
- Python 3.6+
- shapely
- gdal (osgeo)

Usage:
```sh
# Dump all features from layer <layerName>
python dump_wfs.py https://some-wfs-server layerName

# Dump all features from <layerName2> matching the given server-side filter expression
python dump_wfs.py https://another-wfs-server layerName2 'FOO in (1,2,3) AND BAR in (4,5,6)
```
