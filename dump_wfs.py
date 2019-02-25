import asyncio
import sys
import time
import shapely.wkb
import traceback
import json
from osgeo import ogr, gdal


# Set the driver (optional)
wfs_drv = ogr.GetDriverByName('WFS')

# Speeds up querying WFS capabilities for services with a lot of layers
gdal.SetConfigOption('OGR_WFS_LOAD_MULTIPLE_LAYER_DEFN', 'NO')
# Set config for paging. Works on WFS 2.0 services and WFS 1.0 and 1.1 with some other services.
gdal.SetConfigOption('OGR_WFS_PAGING_ALLOWED', 'YES')
# NB: specifying a value that's greater than server max value will silently fail..
gdal.SetConfigOption('OGR_WFS_PAGE_SIZE', '1000')


try:
    url = sys.argv[1]
    layer_name = sys.argv[2]
    server_attribute_filter = sys.argv[3] if sys.argv[3:] else None
except IndexError:
    print(f'Usage: {sys.argv[0]} <WFS URL> <layer name> [server-side attribute filter expression]', file=sys.stderr)
    print(file=sys.stderr)
    print('NB: Use an empty layer name ("") to list the layers on the server and exit.', file=sys.stderr)
    print('NB: Output will be printed to stdout as NDJSON', file=sys.stderr)
    sys.exit(1)


print('WFS:', url, file=sys.stderr)
print('Layer:', layer_name or '<EMPTY>', file=sys.stderr)
if server_attribute_filter:
    print(f'Filtering attributes server-side: {server_attribute_filter}', file=sys.stderr)

wfs_ds = wfs_drv.Open('WFS:' + url)
assert wfs_ds, f'ERROR: can not open WFS datasource: {url}'


async def get_features():
    layer = wfs_ds.GetLayerByName(layer_name)
    layer.GetDescription()
    srs = layer.GetSpatialRef()
    name = layer.GetName()

    # NB: This is 4..5 times slower than if we just used client-side filtering.
    # Only use it if bandwidth costs more than gold.
    if server_attribute_filter:
        layer.SetAttributeFilter(server_attribute_filter)

    # NB: GetFeatureCount() is a very heavy function when (server-side) SetAttributeFilter is used.
    if server_attribute_filter:
        print(
            'Features: ??? (asking server would probably take too long with server-side filtering)',
            file=sys.stderr
        )
        num_features = 1e6 # An arbitrary number
    else:
        num_features = layer.GetFeatureCount()
        print('Features:', format(num_features, ",d"), file=sys.stderr)

    print('SR:', srs.ExportToWkt(), file=sys.stderr)

    # Iterate over features
    n_features = 0
    backoff = 0
    last_time = start_time = time.time()
    while True:
        try:
            feat = layer.GetNextFeature()
            # Encountered this earlier. Got a None wayyy too early.
            if feat is None:
                raise Exception(f'Out of data? backoff:{backoff}')
            else:
                # Success: reset backoff
                backoff=0

        except Exception as ex:
            traceback.print_exc(file=sys.stderr)
            if backoff >= 3:
                print(f'Maximum retries exceeded (backoff:{backoff}). Exiting :/', file=sys.stderr)
                break

            print(f'Trying again in {2**backoff} seconds. Backoff:{backoff}...', file=sys.stderr)
            time.sleep(2**backoff)
            backoff += 1
            continue

        # Not really triggered now...
        if feat is None:
            print('Looks like this is the last feature. Exiting.', file=sys.stderr)
            break

        if n_features == 0:
            defns = [feat.GetFieldDefnRef(i) for i in range(feat.GetFieldCount())]
            names = [d.GetName() for d in defns]
            gdefns = [feat.GetGeomFieldDefnRef(i) for i in range(feat.GetGeomFieldCount())]
            gnames = [d.GetName() for d in gdefns]

            print('First feature:', [
                (d.GetName(), d.GetTypeName(), feat.GetField(i))
                for i,d in enumerate(defns)
            ], file=sys.stderr)
            for g in gdefns:
                print(g, g.GetSpatialRef().ExportToWkt(), file=sys.stderr)

        attrs = {k: feat.GetField(i) for i,k in enumerate(names)}
        g_attrs = {k: feat.GetGeomFieldRef(i) for i,k in enumerate(gnames)}
        yield feat, attrs, g_attrs

        n_features += 1
        if n_features % 10_000 == 0:
            t=time.time()
            print(
                'Number of features seen so far:',
                format(n_features, ',d'),
                f'(~{round(100*n_features/num_features,1)} %)',
                f'time:{round(t-start_time,1)} sec',
                file=sys.stderr
            )
            # print(f'Time Total: {t-start_time} Last 10k:{t-last_time)}', file=sys.stderr)
            last_time=t

    print('Finished.', file=sys.stderr)
    print('Final number of features seen:', format(n_features, ',d'), file=sys.stderr)


async def print_features_geojson():
    async for feat, attributes, geom_attributes in get_features():
        # print('Feature:', format(feat.GetFID(), ',d'), file=sys.stderr)

        # NB! We cannot export directly to WKB -> Shapely,
        # because Shapely does not understand CURVEPOLYGON among other things.
        for name, geom in geom_attributes.items():
            if geom is None:
                print('ERROR: Skipping empty geom:', name, feat, attributes, file=sys.stderr)
                continue
            lin_geom = geom.GetLinearGeometry()
            wkb = lin_geom.ExportToWkb()
            shgeom_source_crs = shapely.wkb.loads(wkb)
            geo_dict = shapely.geometry.mapping(shgeom_source_crs)
            geo_dict['properties'] = attributes
            json.dump(geo_dict, sys.stdout, ensure_ascii=False, separators=(',',':'))
            print()


if not layer_name:
    print('Printing server layer list and exiting.')
    num_layers = wfs_ds.GetLayerCount()
    print('Layer count:', num_layers)
    for i in range(num_layers):
        layer = wfs_ds.GetLayerByIndex(i)
        layer.GetDescription()
        srs = layer.GetSpatialRef()
        name = layer.GetName()
        print(f'Layer {i}: {name}')
    sys.exit(0)


if sys.version_info < (3,7):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(print_features_geojson())
    loop.close()
else:
    asyncio.run(print_features_geojson())
