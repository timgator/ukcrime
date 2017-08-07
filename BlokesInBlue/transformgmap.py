# -*- coding: utf-8 -*-
"""
Created on Sat Apr 16 21:34:23 2016

@author: joshuakaplan
"""
import pyproj
import shapefile
from collections import OrderedDict
from bokeh.models.glyphs import Patches, Line, Circle
from bokeh.models import (
    GMapPlot, Range1d, ColumnDataSource, LinearAxis, GeoJSONDataSource,
    HoverTool, PanTool, WheelZoomTool, BoxSelectTool, ResetTool, PreviewSaveTool,
    GMapOptions,
    NumeralTickFormatter, PrintfTickFormatter)
from bokeh.plotting import figure, show, output_file

sf = shapefile.Reader("/Users/Ahn/Desktop/ukcrime/Shapefile/england_msoa_2011_sgen_clipped.shp") 

#http://gis.stackexchange.com/questions/168310/how-to-convert-projected-coordinates-to-geographic-coordinates-without-arcgis
#https://karlhennermann.wordpress.com/2015/02/16/how-to-make-lsoa-and-msoa-boundaries-from-uk-data-service-align-properly-in-arcgis/
shapes = sf.shapes()
records = sf.records()
fields = sf.fields

def transform(epsg_in, epsg_out, x_in, y_in):

    # define source and destination coordinate systems based on the ESPG code
    srcProj = pyproj.Proj(init='epsg:%i' % int(epsg_in), preserve_units=True)
    dstProj = pyproj.Proj(init='epsg:%i' % int(epsg_out), preserve_units=True)

    # perform transformation
    x_out,y_out = pyproj.transform(srcProj, dstProj, x_in, y_in)
    return x_out,y_out

data = dict([])
for i in range(len(shapes)):
    temp = dict()
    lats = list()        
    longs=list()
    for j in range(len(shapes[i].points)):
        x = shapes[i].points[j][0]
        y = shapes[i].points[j][1]
        lats.append(transform(epsg_in=27700,epsg_out=4326,x_in=x,y_in=y)[1])
        longs.append(transform(epsg_in=27700,epsg_out=4326,x_in=x,y_in=y)[0])
        name = records[i][0]
    temp['name']=name
    temp['lats']=lats
    temp['longs']=longs
    data[i] = temp



colors = ["#F1EEF6", "#D4B9DA", "#C994C7", "#DF65B0", "#DD1C77", "#980043"]

msoa_names =[msoa["name"] for msoa in data.values()]
msoa_lats = [msoa["lats"] for msoa in data.values()]
msoa_longs = [msoa["longs"] for msoa in data.values()]
col = colors*1200

source = ColumnDataSource(data=dict(
    y=msoa_lats,
    x=msoa_longs,
    color=col[:len(set(msoa_names))+1],
    name=msoa_names
))


p = GMapPlot(title="MSOA", plot_width=1200, plot_height=800, x_range = Range1d(), y_range = Range1d(), map_options = GMapOptions(lat=51.5074, lng=0.1278, zoom=10))

p.map_options.map_type = "terrain"

patch = Patches(xs="x", ys="y", fill_color="color", fill_alpha=0.7, line_color="black", line_width=0.5)

patches_glyph = p.add_glyph(source, patch)

p.add_tools(PanTool(), WheelZoomTool(), BoxSelectTool(), HoverTool(), ResetTool(), PreviewSaveTool())

hover = p.select(dict(type=HoverTool))
hover.point_policy = "follow_mouse"
hover.tooltips = OrderedDict([
    ("Name", "@name"),
    ("(Lat, Long)", "($y, $x)"),
])

output_file("MSOAGMap.html", title="MSOA GMap test", mode="cdn")
show(p)
