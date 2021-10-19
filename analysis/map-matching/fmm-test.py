# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 02:53:44 2021

@author: WylieTimmerman

attempted to run through the example provided with fmm, but just gave ups
"""
# NOTE: This is python 2.7 only, apparently
# may be better to dump files to CSV and then bring back to parquet later?
# 

# % Environment Setup

from fmm import Network,NetworkGraph,FastMapMatch,FastMapMatchConfig,UBODT
from fmm import UBODTGenAlgorithm

#### Load network and graph

network = Network("../data/edges.shp")
print "Nodes {} edges {}".format(network.get_node_count(),network.get_edge_count())
graph = NetworkGraph(network)

#### Precompute UBODT file
from fmm import UBODTGenAlgorithm
ubodt_gen = UBODTGenAlgorithm(network,graph)
status = ubodt_gen.generate_ubodt("../data/ubodt.txt", 4, binary=False, use_omp=True)
print status

#### Load UBODT data
ubodt = UBODT.read_ubodt_csv("../data/ubodt.txt")

#### Create FMM model
model = FastMapMatch(network,graph,ubodt)

#### Define FMM config
k = 4
radius = 0.4
gps_error = 0.5
fmm_config = FastMapMatchConfig(k,radius,gps_error)

#### Run map matching
# TODO: convert to WKT, add timestamp?
# TODO: does match_wkt() take timestamp?
wkt = "LINESTRING(0.200812146892656 2.14088983050848,1.44262005649717 2.14879943502825,3.06408898305084 2.16066384180791,3.06408898305084 2.7103813559322,3.70872175141242 2.97930790960452,4.11606638418078 2.62337570621469)"
result = model.match_wkt(wkt, fmm_config)

print "Matched path: ", list(result.cpath)
print "Matched edge for each point: ", list(result.opath)
print "Matched edge index ",list(result.indices)
print "Matched geometry: ",result.mgeom.export_wkt()
print "Matched point ", result.pgeom.export_wkt()

#### Match trajectories in a GPS file

from fmm import GPSConfig,ResultConfig
input_config = GPSConfig()
input_config.file = "../data/trips.csv"
input_config.id = "id"

print input_config.to_string()

result_config = ResultConfig()
result_config.file = "../data/mr.txt"
result_config.output_config.write_opath = True
print result_config.to_string()

status = model.match_gps_file(input_config, result_config, fmm_config)



