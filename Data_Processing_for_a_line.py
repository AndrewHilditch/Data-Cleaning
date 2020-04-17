import pandas as pd
from pandas import DataFrame
import random as rm
import numpy as np
import time
import networkx as nx
import pickle
import datetime
from datetime import timedelta
import scipy.stats
import matplotlib.pyplot as plt

#Process raw timetable data into a more manageable dataframe
def Process_raw_timetable(Timetablepath,Timetablefilename):
    #Read in timetable data
    data_folder = Timetablepath
    filename = data_folder+ Timetablefilename
    data = pd.read_csv(filename, header=None, names=['tag'])
    data['Msg_type'] = data['tag'].str.slice(0,2)
    data = data[data['Msg_type']!='AA'].reset_index(drop=True)
    #Process basic schedule data type
    BSdata= data[data['Msg_type']=='BS'].copy()
    BSdata['TransactionType'] = BSdata['tag'].str.slice(2,3)
    BSdata['TrainUID'] = BSdata['tag'].str.slice(3,9)
    BSdata['Datefrom'] = BSdata['tag'].str.slice(9,15)
    BSdata['Dateto'] = BSdata['tag'].str.slice(15,21)
    BSdata['Daysrun'] = BSdata['tag'].str.slice(21,28)
    BSdata['Bankhol'] = BSdata['tag'].str.slice(28,29)
    BSdata['Trainstatus'] = BSdata['tag'].str.slice(29,30)
    BSdata['Traincategory'] = BSdata['tag'].str.slice(30,32)
    BSdata['Trainidentity'] = BSdata['tag'].str.slice(32,36)
    BSdata['Headcode'] = BSdata['tag'].str.slice(36,40)
    BSdata['Servicecode'] = BSdata['tag'].str.slice(41,49)
    BSdata['Speed'] = BSdata['tag'].str.slice(57,60)
    BSdata['Operatechar'] = BSdata['tag'].str.slice(60,66)
    del BSdata['tag']
    BSdata=BSdata.reset_index(drop=True)
    #Process train origin data type
    LOdata= data[data['Msg_type']=='LO'].copy()
    LOdata['Location'] = LOdata['tag'].str.slice(2,10)
    LOdata['SchedArr'] = LOdata['tag'].str.slice(50,51)
    LOdata['SchedDept'] = LOdata['tag'].str.slice(10,15)
    LOdata['SchedPass'] = LOdata['tag'].str.slice(50,51)
    LOdata['PubArr'] = LOdata['tag'].str.slice(50,51)
    LOdata['PubDept'] = LOdata['tag'].str.slice(15,19)
    LOdata['Platform'] = LOdata['tag'].str.slice(19,22)
    LOdata['Line'] = LOdata['tag'].str.slice(22,25)
    LOdata['Path'] = LOdata['tag'].str.slice(50,51)
    LOdata['Activity'] = LOdata['tag'].str.slice(29,41)
    LOdata['Engallow'] = LOdata['tag'].str.slice(25,27)
    LOdata['Pathallow'] = LOdata['tag'].str.slice(27,29)
    LOdata['Perfallow'] = LOdata['tag'].str.slice(41,43)
    del LOdata['tag']
    #Process intermediate train movements data type
    LIdata= data[data['Msg_type']=='LI'].copy()
    LIdata['Location'] = LIdata['tag'].str.slice(2,10)
    LIdata['SchedArr'] = LIdata['tag'].str.slice(10,15)
    LIdata['SchedDept'] = LIdata['tag'].str.slice(15,20)
    LIdata['SchedPass'] = LIdata['tag'].str.slice(20,25)
    LIdata['PubArr'] = LIdata['tag'].str.slice(25,29)
    LIdata['PubDept'] = LIdata['tag'].str.slice(29,33)
    LIdata['Platform'] = LIdata['tag'].str.slice(33,36)
    LIdata['Line'] = LIdata['tag'].str.slice(36,39)
    LIdata['Path'] = LIdata['tag'].str.slice(39,42)
    LIdata['Activity'] = LIdata['tag'].str.slice(40,54)
    LIdata['Engallow'] = LIdata['tag'].str.slice(54,56)
    LIdata['Pathallow'] = LIdata['tag'].str.slice(56,58)
    LIdata['Perfallow'] = LIdata['tag'].str.slice(58,60)
    del LIdata['tag']
    #Process train terminal movements data type
    LTdata= data[data['Msg_type']=='LT'].copy()
    LTdata['Location'] = LTdata['tag'].str.slice(2,10)
    LTdata['SchedArr'] = LTdata['tag'].str.slice(10,15)
    LTdata['SchedDept'] = LTdata['tag'].str.slice(50,51)
    LTdata['SchedPass'] = LTdata['tag'].str.slice(50,51)
    LTdata['PubArr'] = LTdata['tag'].str.slice(15,19)
    LTdata['PubDept'] = LTdata['tag'].str.slice(50,51)
    LTdata['Platform'] = LTdata['tag'].str.slice(19,22)
    LTdata['Line'] = LTdata['tag'].str.slice(50,51)
    LTdata['Path'] = LTdata['tag'].str.slice(22,25)
    LTdata['Activity'] = LTdata['tag'].str.slice(25,37)
    LTdata['Engallow'] = LTdata['tag'].str.slice(50,51)
    LTdata['Pathallow'] = LTdata['tag'].str.slice(50,51)
    LTdata['Perfallow'] = LTdata['tag'].str.slice(50,51)
    del LTdata['tag']
    #Recombine these message types to get to the timetable
    Timetabledata=LOdata.append(LIdata)
    Timetabledata=Timetabledata.append(LTdata)
    Timetabledata=Timetabledata.sort_index()
    Timetabledata=Timetabledata.reset_index()
    del Timetabledata['index']
    #Add additional information from the basic schedule messages
    k=0
    Timetabledata['TrainUID']=Timetabledata['Location']
    Timetabledata['Headcode']=Timetabledata['Location']
    Timetabledata['Daysrun']=Timetabledata['Location']
    Timetabledata['Datesrun']=Timetabledata['Location']
    for i in range(0,len(Timetabledata)):
        Timetabledata.at[i,'TrainUID']=BSdata['TrainUID'][k]
        Timetabledata.at[i,'Headcode']=BSdata['Headcode'][k]
        Timetabledata.at[i,'Daysrun']=BSdata['Daysrun'][k]
        Timetabledata.at[i,'Datesrun']=BSdata['Datefrom'][k]+BSdata['Dateto'][k]
        if Timetabledata['Msg_type'][i]=='LT':
            k=k+1 
Timetabledata['Date']=pd.to_datetime('20'+Timetabledata['Datesrun'].str.slice(0,6), format='%Y%m%d')
    del Timetabledata['Datesrun']
    del Timetabledata['Daysrun']
    return Timetabledata
