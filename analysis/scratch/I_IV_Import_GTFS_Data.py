# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 12:35:10 2020

@author: abibeka
"""

#0.0 Housekeeping. Clear variable space
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()

#1 Import Libraries
########################################################################################
import pandas as pd, os, numpy as np, pyproj, sys, zipfile, glob
from geopy.distance import geodesic
from collections import defaultdict

if os.getlogin() == "WylieTimmerman":
    # Working Paths
    path_working = r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL")
    path_sp = r"C:\OD\Foursquare ITP\Foursquare ITP SharePoint Site - Shared Documents\WMATA Queue Jump Analysis"
    
    # Source data
    # path_source_data = os.path.join(path_sp,r"Client Shared Folder\data\00-raw\102019 sample")
    path_source_data = r"C:\Downloads\Vehicles 0-2999\Vehicles 0-2999"
    GTFS_Dir = os.path.join(path_sp,r"Client Shared Folder\data\00-raw\wmata-2019-05-18 dl20200205gtfs")
    ZippedFilesLoc = path_source_data
    # Processed data
    
    # Processed data
    path_processed_data = os.path.join(path_sp,r"Client Shared Folder\data\02-processed")
    
else:
    breakpoint() #I broke your paths, sorry!
    # Working Paths
    path_working = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(path_working) 
    
    # Source data
    path_source_data = r"C:\Downloads\Vehicles 0-2999\Vehicles 0-2999"
    GTFS_Dir = "./google_transit"
    ZippedFilesLoc = './October 2019 Rawnav/Vehicles 0-2999'
    
    # Processed data
    path_processed_data = os.path.join("somethinggoeshere")


import rawnavparser as rp

#2 Read the GTFS data
########################################################################################

StopsDat= pd.read_csv(os.path.join(GTFS_Dir,"stops.txt"))
StopTimeDat = pd.read_csv(os.path.join(GTFS_Dir,"stop_times.txt"))
TripsDat =pd.read_csv(os.path.join(GTFS_Dir,"trips.txt"))
StopsDat= StopsDat[['stop_id','stop_name','stop_lat','stop_lon']]
StopTimeDat = StopTimeDat[['trip_id','arrival_time','departure_time','stop_id','stop_sequence','pickup_type','drop_off_type']]
TripsDat = TripsDat[['route_id','service_id','trip_id','trip_headsign','direction_id']]
# with pd.option_context('display.max_rows', 5, 'display.max_columns', 10):
#     display(StopTimeDat)

#3 Analyze the Trip Start and End
########################################################################################
#trip_id is a unique identifier irrespective of the route
TripsDat.trip_id = TripsDat.trip_id.astype(str)
StopTimeDat.trip_id = StopTimeDat.trip_id.astype(str)
# 0: travel in one direction; 1 travel in opposite direction
TripSumDa = TripsDat.groupby(['route_id','direction_id','trip_headsign']).count().reset_index()
Merdat = TripsDat.merge(StopTimeDat,on="trip_id",how='inner')
Merdat = Merdat.merge(StopsDat,on= "stop_id")

#Get the 1st stops
#########################################
Mask_1stStop = (Merdat.stop_sequence ==1)
FirstStopDat = Merdat[Mask_1stStop]
FirstStopDat.rename(columns = {'stop_id':'first_stopId','stop_name':"first_stopNm",'stop_lat':'first_sLat',
                               'stop_lon':'first_sLon'
                               },inplace=True)
FirstStopDat = FirstStopDat[['route_id','direction_id','trip_headsign',
                             'first_stopId',"first_stopNm",'first_sLat',
                             'first_sLon','arrival_time','departure_time']]
def to_set(x):
    return set(x)
CheckFirstStop = FirstStopDat.groupby(['route_id','direction_id','trip_headsign']).agg({'first_stopId':to_set,'first_stopNm':to_set})
CheckFirstStop1 = FirstStopDat.groupby(['route_id','direction_id','trip_headsign',\
                                        'first_stopId','first_stopNm'])\
                                        .agg({'first_sLat':['count','first'],'first_sLon':'first'
                                             ,'arrival_time':to_set,'departure_time':to_set})

#Get Last stops per trip
#########################################
TempDa = Merdat.groupby('trip_id')['stop_sequence'].max().reset_index()
LastStopDat = TempDa.merge(Merdat, on=['trip_id','stop_sequence'],how='left')
LastStopDat.rename(columns = {'stop_id':'last_stopId','stop_name':"last_stopNm",'stop_lat':'last_sLat',
                               'stop_lon':'last_sLon'},inplace=True)
LastStopDat = LastStopDat[['route_id','direction_id','trip_headsign',
                           'last_stopId',"last_stopNm",'last_sLat','last_sLon','arrival_time','departure_time']]
CheckLastStop = LastStopDat.groupby(['route_id','direction_id','trip_headsign']).agg({'last_stopId':to_set,'last_stopNm':to_set})

CheckLastStop1 = LastStopDat.groupby(['route_id','direction_id','trip_headsign',
                                      'last_stopId','last_stopNm'])\
                                    .agg({'last_sLat':['count','first'],
                                          'last_sLon':'first' ,'arrival_time':to_set,'departure_time':to_set})
First_Last_Stop = CheckFirstStop.merge(CheckLastStop,left_index=True,right_index=True,how='left')
#Check stops data
#########################################
OutFi = os.path.join(path_processed_data,'First_Last_Stop.xlsx')
writer = pd.ExcelWriter(OutFi)
First_Last_Stop.to_excel(writer,'First_Last_Stop')
CheckFirstStop1.to_excel(writer,'First_Stop')
CheckLastStop1.to_excel(writer,'Last_Stop')
writer.save()


#4 Get first stops and lasts stops by route---ignore direction
########################################################################################
dropCols = ['direction_id','trip_headsign','arrival_time','departure_time']
FirstStopDat1 =FirstStopDat.drop(columns=dropCols)        
FirstStopDat1 = FirstStopDat1.groupby(['route_id','first_stopId','first_stopNm']).first()
LastStopDat1 =LastStopDat.drop(columns=dropCols)        
LastStopDat1 = LastStopDat1.groupby(['route_id','last_stopId','last_stopNm']).first()
FirstStopDat1.to_csv(os.path.join(path_processed_data,'FirstStopGTFS.csv'))
LastStopDat1.to_csv(os.path.join(path_processed_data,'LastStopGTFS.csv'))


#5 Only Read RawNav Files for selected routes
########################################################################################


TripInventory = pd.read_excel(os.path.join(path_processed_data,'TripSummaries_Veh0_2999.xlsx'),\
                              'summary_data', \
                              converters = {'Tag':str})
TripInventory.loc[:,'route_id'] = TripInventory.Tag.str[0:2].str.upper()
TripInventory.columns
AnalysisRoutes = ['79','X2','X9']
MaskAnalysisRoutes = TripInventory.route_id.isin(AnalysisRoutes)
TripInventory = TripInventory[MaskAnalysisRoutes]

if len(TripInventory) == 0:
    raise Exception ("No Analysis Routes found in TripInventory")

TripInventory1 = TripInventory.groupby(['route_id','FileNm'])['BusID'].count().reset_index()

RawNavDataDict= defaultdict(lambda:  {})

for idx, row in TripInventory1.iterrows():
    RawNavDataDict[row['route_id']][row['FileNm']] = pd.DataFrame()

FirstTagDict = {}

for FileNm_int, EmptyDf in RawNavDataDict['79'].items():
    ZipFolder = os.path.join(path_source_data,"rawnav" + str(FileNm_int).rjust(11,'0') + '.txt.zip')
    ZipFile1 = os.path.basename(ZipFolder.split('.zip')[0]) 
    FistTagLnNum, FirstTagLine, StartTimeLn,HasData,HasCorrectBusID = rp.FindFirstTagLine_ZipFile(ZipFolder, ZipFile1)
    zf = zipfile.ZipFile(ZipFolder)
    RawNavDataDict['79'][FileNm_int] = pd.read_csv(zf.open(ZipFile1),skiprows = FistTagLnNum, header =None)
    FirstTagDict[FileNm_int] = {'FistTagLnNum':FistTagLnNum,'FirstTagLine':FirstTagLine,'StartTimeLn':StartTimeLn}


# os.getcwd()
# Analyze Route 79
FirstStopDat1 = FirstStopDat1.reset_index()[['route_id','first_sLat','first_sLon']]
LastStopDat1 = LastStopDat1.reset_index()[['route_id','last_sLat','last_sLon']]

ProcessedDataDict= defaultdict(lambda:  {})
Debug= False
ColumnNmMap = {0:'Lat',1:'Long',2:'Heading',3:'DoorState',4:'VehState',5:'OdomtFt',6:'SecPastSt',7:'SatCnt',
                   8:'StopWindow',9:'Blank',10:'LatRaw',11:'LongRaw'}

rte_id = '79'
for key,TestData in RawNavDataDict[rte_id].items():
    FirstTag = FirstTagDict[key]['FirstTagLine']
    FirstTag = [0] + FirstTag
    #3 data Cleaning
    #****************************************************************************************************************
    
    #3.1 Remove "APC" and "CAL" Labels
    #****************************************************************************************************************
    Data = TestData if Debug else ""
    TestData = rp.RemoveCAL_APC_Tags(TestData)
    #3.2 Get the Rows with Tags
    #****************************************************************************************************************
    TestData.reset_index(inplace=True); TestData.rename(columns = {"index":"IndexLoc"},inplace=True)
    TagsData = TestData[~TestData.apply(rp.check_valid_data_entry, axis=1)]
    TripTags,EndOfRoute1 = rp.GetTagInfo(TagsData,FirstTag)
    #Remove rows with tags and rows that have no value in the 3rd column
    # Might need to look back at the 3rd column
    RemoveRows = np.append(EndOfRoute1.IndexTripEnd.values, TripTags.IndexTripTags.values)
    RemoveRows = np.setdiff1d(RemoveRows,np.array([0])) #1st row should not be deleted. 
    #1st tag would at position 0 but it doesn't affect the data.
    TestData = TestData[~TestData.IndexLoc.isin(RemoveRows)]
    TestData=  TestData[TestData.apply(rp.check_valid_data_entry, axis=1)]
    #check if 1st and 2nd column only has lat long 
    try:
        TestData.loc[:,[0,1]] = TestData.loc[:,[0,1]].applymap(lambda x: float(x))#It would not work we All Tags are not removed from the data
    except(ValueError): print('All Tags are not removed from the data')
    TestData.drop(columns=[7,8,9,10,11],inplace=True)
    # Get Trip Summary 
    #****************************************************************************************************************
    Data = TestData if Debug else ""
    # Add start and end info to the data. Get Trip start and end data from tags and raw data. 
    TestData1, TripSumData,EndTimeFeetDat = rp.AddTripStartEndTags(TestData,TripTags, EndOfRoute1)  
    TripSumData = TripSumData.groupby('IndexTripTags')['TripStartTime'].first().reset_index()
    TestData1.rename(columns=ColumnNmMap,inplace=True)    
    TestData1.loc[:,'route_id'] = TestData1.Tag.str[:2].str.upper()
    TestData2 = TestData1[TestData1.route_id==rte_id]
    FirstRawNavRow = TestData2.groupby(['IndexTripTags'])['IndexLoc'].min().reset_index()
    FirstRawNavRow = FirstRawNavRow.merge(TestData2,on = ['IndexTripTags','IndexLoc'],how='left')
    
    LastRawNavRow = TestData2.groupby(['IndexTripTags'])['IndexLoc'].max().reset_index()
    LastRawNavRow = LastRawNavRow.merge(TestData2,on = ['IndexTripTags','IndexLoc'],how='left')
    
    
    FirstStopLatLong_RawNav = pd.DataFrame()
    LastStopLatLong_RawNav = pd.DataFrame()

    #****************************************************************************************************************
    GTFS_1stStop = FirstStopDat1[FirstStopDat1.route_id==rte_id]
    GTFS_lastStop = LastStopDat1[LastStopDat1.route_id==rte_id]

    #****************************************************************************************************************
    for idx, row in FirstRawNavRow.iterrows():
        RawNavLat1,RawNavLong1 = row[['Lat','Long']]
        GTFS_1stStop.loc[:,'Dist_from_1st_RawNav_Pt'] = GTFS_1stStop.apply(lambda row:rp.GetDistanceLatLong_ft(row['first_sLat'],row['first_sLon'],RawNavLat1,RawNavLong1) ,axis=1)
        MaskClosestStop = GTFS_1stStop.Dist_from_1st_RawNav_Pt== min(GTFS_1stStop.Dist_from_1st_RawNav_Pt )
        TempLat = GTFS_1stStop[MaskClosestStop][['first_sLat','first_sLon']].values[0][0]
        TempLong = GTFS_1stStop[MaskClosestStop][['first_sLat','first_sLon']].values[0][1]
        TempDa2 = pd.DataFrame({'IndexTripTags':[row['IndexTripTags']],'first_sLat':[TempLat],'first_sLon':[TempLong]})    
        FirstStopLatLong_RawNav =pd.concat([FirstStopLatLong_RawNav,TempDa2])
         
    for idx, row in LastRawNavRow.iterrows():
        RawNavLat1,RawNavLong1 = row[['Lat','Long']]
        GTFS_lastStop.loc[:,'Dist_from_last_RawNav_Pt'] = GTFS_lastStop.apply(lambda row:rp.GetDistanceLatLong_ft(row['last_sLat'],row['last_sLon'],RawNavLat1,RawNavLong1) ,axis=1)
        MaskClosestStop = GTFS_lastStop.Dist_from_last_RawNav_Pt== min(GTFS_lastStop.Dist_from_last_RawNav_Pt )
        TempLat = GTFS_lastStop[MaskClosestStop][['last_sLat','last_sLon']].values[0][0]
        TempLong = GTFS_lastStop[MaskClosestStop][['last_sLat','last_sLon']].values[0][1]
        TempDa2 = pd.DataFrame({'IndexTripTags':[row['IndexTripTags']],'last_sLat':[TempLat],'last_sLon':[TempLong]})    
        LastStopLatLong_RawNav =pd.concat([LastStopLatLong_RawNav,TempDa2])
    
    TestData2 = TestData2.merge(FirstStopLatLong_RawNav,on='IndexTripTags',how='left')
    TestData2.loc[:,'Dist_from_GTFS1stStop'] = TestData2.apply(lambda row:rp.GetDistanceLatLong_ft(row['first_sLat'],row['first_sLon'],row['Lat'],row['Long']) ,axis=1)
    TestData2 = TestData2.merge(LastStopLatLong_RawNav,on='IndexTripTags',how='left')
    TestData2.loc[:,'Dist_from_GTFSlastStop'] = TestData2.apply(lambda row:rp.GetDistanceLatLong_ft(row['last_sLat'],row['last_sLon'],row['Lat'],row['Long']) ,axis=1)
    MinDat = TestData2.groupby(['IndexTripTags'])['Dist_from_GTFS1stStop','Dist_from_GTFSlastStop'].idxmin().reset_index()
    MinDat.rename(columns = {'Dist_from_GTFS1stStop':'LowerBound','Dist_from_GTFSlastStop':"UpperBound"},inplace=True)
    MinDat.loc[:,'LowerBound'] = TestData2.loc[MinDat.loc[:,'LowerBound'],'IndexLoc'].values
    MinDat.loc[:,'UpperBound'] = TestData2.loc[MinDat.loc[:,'UpperBound'],'IndexLoc'].values

    TestData2 = TestData2.merge(MinDat,on='IndexTripTags',how='left')
    MaskGTFS_Trimming = (TestData2.IndexLoc>=TestData2.LowerBound) & (TestData2.IndexLoc<=TestData2.UpperBound)
    TestData2 = TestData2[MaskGTFS_Trimming]
    TestData2 = TestData2[['Lat','Long','Heading','OdomtFt','SecPastSt',
                           'IndexTripTags','Tag','Dist_from_GTFS1stStop','Dist_from_GTFSlastStop']]
    
    Map1 = lambda x: max(x)-min(x)
    SumDat1 =TestData2.groupby('IndexTripTags').agg({'OdomtFt':['min','max',Map1],
                                                     'SecPastSt':['min','max',Map1],
                                                     'Lat':['first','last'],
                                                     'Long':['first','last'],
                                                     'Dist_from_GTFS1stStop':['first','last'],
                                                     'Dist_from_GTFSlastStop':['last']})
    SumDat1.columns = ['OdomtFt_start_GTFS','OdomtFt_end_GTFS','Trip_Dist_Mi_GTFS',
                       'SecPastSt_start_GTFS','SecPastSt_end_GTFS','Trip_Dur_Sec_GTFS',
                       'Lat_start_GTFS','Lat_end_GTFS','Long_start_GTFS','Long_end_GTFS',
                       'Dist_from_GTFS1stStop_start_ft','Dist_from_GTFS1stStop_end_mi',
                       'Dist_from_GTFSlastStop_end_ft']
    SumDat1.loc[:,['Trip_Dist_Mi_GTFS','Dist_from_GTFS1stStop_end_mi']] =SumDat1.loc[:,['Trip_Dist_Mi_GTFS','Dist_from_GTFS1stStop_end_mi']]/5280
    SumDat1.loc[:,'Trip_Speed_mph_GTFS'] =round(3600* SumDat1.Trip_Dist_Mi_GTFS/SumDat1.Trip_Dur_Sec_GTFS,2)
    SumDat1 = SumDat1.merge(TripSumData,on='IndexTripTags',how='left')
    SumDat1.loc[:,'FileNm'] = key
    ProcessedDataDict[rte_id][key] =  SumDat1
    
FinSumDat = pd.concat(ProcessedDataDict[rte_id].values())
FinSumDat =FinSumDat.merge(TripInventory, on =['FileNm','TripStartTime'],how='left')
FinSumDat.to_excel(os.path.join(path_processed_data,'Route79_TrimSum.xlsx'))

    
    
# X---Misc Subset data for Route 79
# Rewrite later !!!
########################################################################################

TripsDat = TripsDat[TripsDat.route_id=="79"]
TripsDat.trip_id = TripsDat.trip_id.astype(str)
StopTimeDat.trip_id = StopTimeDat.trip_id.astype(str)


TripsDat.trip_headsign.value_counts()
TripsDat.groupby(['trip_headsign','direction_id']).count()
# 0: travel in one direction; 1 travel in opposite direction

Merdat = TripsDat.merge(StopTimeDat,on="trip_id",how='inner')
NumStops = Merdat.groupby(['trip_id','direction_id'])['stop_id'].count()
NumStops.describe()


Merdat = Merdat.merge(StopsDat,on= "stop_id")

#Make Sure all trips have same 1st stop
FirstStopDat = Merdat[Merdat.stop_sequence==1].reset_index()
assert(np.var(FirstStopDat.stop_sequence)==0)
assert(FirstStopDat.groupby(['route_id','direction_id'])['stop_id'].var().sum()==0)
FirstStopDat = FirstStopDat.groupby(['route_id','direction_id']).first()
#Make sure all trips have same end stop
LastStopDat = Merdat.groupby('trip_id')['stop_sequence'].max().reset_index()
LastStopDat = LastStopDat.merge(Merdat, on=['trip_id','stop_sequence'],how='left')
# WT: FYI, errored here
assert(np.var(LastStopDat.stop_sequence)==0)
assert(LastStopDat.groupby(['route_id','direction_id'])['stop_id'].var().sum()==0)
LastStopDat = LastStopDat.groupby(['route_id','direction_id']).first()
#The above assertions would start failing for routes like S9; routes with long and short routes
FirstStopDat.columns
FirstStopDat.rename(columns = {'stop_id':'first_stopId','stop_name':"first_stopNm",'stop_lat':'first_sLat',
                               'stop_lon':'first_sLon',
                               'trip_headsign':'end_stop_desc'},inplace=True)
FirstStopDat = FirstStopDat[['first_stopId',"first_stopNm",'first_sLat','first_sLon','end_stop_desc']]
LastStopDat.rename(columns = {'stop_id':'last_stopId','stop_name':"last_stopNm",'stop_lat':'last_sLat',
                               'stop_lon':'last_sLon'},inplace=True)
LastStopDat = LastStopDat[['last_stopId',"last_stopNm",'last_sLat','last_sLon']]

FinStopDat= pd.merge(FirstStopDat,LastStopDat,left_index=True,right_index=True)
FinStopDat.reset_index(inplace=True)

geodesic = pyproj.Geod(ellps='WGS84')
fwd_azimuth,back_azimuth,distance = geodesic.inv(FinStopDat.first_sLat[0], FinStopDat.first_sLon[0], 
                                                 FinStopDat.last_sLat[0], FinStopDat.last_sLon[0])
fwd_azimuth,back_azimuth,distance = geodesic.inv(FinStopDat.first_sLat[1], FinStopDat.first_sLon[1], 
                                                 FinStopDat.last_sLat[1], FinStopDat.last_sLon[1])
#Need to find a way to determine direction of the route
# direction_id : 0,1 might not always mean outbound and inbound



FinStopDat.to_csv(os.path.join(path_processed_data,"StopDetails.csv"),index=False)


