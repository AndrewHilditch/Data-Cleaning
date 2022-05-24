import pandas as pd
from pandas import DataFrame
import numpy as np
import networkx as nx
import pickle
import datetime
from datetime import timedelta


#A function to clean and process berth data
def Berth_data_processing(Dayinput,Berthpath,Berthfilename,Areacode):
    #Read in the data
    data_folder = Berthpath
    filename = data_folder+Berthfilename
    Berthdata=pd.read_csv(filename,names=['tag'])
    #Split the data into dataframe columns
    Berthdata['UTC']=Berthdata['tag'].str.slice(15,21)
    Berthdata['tag']=Berthdata['tag'].str.slice(61,77)
    Berthdata['Area']=Berthdata['tag'].str.slice(0,2)
    Berthdata['msg_type']=Berthdata['tag'].str.slice(2,4)
    Berthdata['From'] = Berthdata['tag'].str.slice(4,8)
    Berthdata['To'] = Berthdata['tag'].str.slice(8,12)
    Berthdata['Headcode'] = Berthdata['tag'].str.slice(12,16)
    del Berthdata['tag']
    #Converts the UTC column into a datetime object 
    Berthdata['RealUTC']=pd.to_datetime(Dayinput+Berthdata['UTC'],format='%Y%m%d%H%M%S')
    for i in range(0,len(Berthdata)):
        if int(Berthdata['UTC'][i])<40000:
            Berthdata.at[i,'RealUTC']=Berthdata['RealUTC'][i]+timedelta(days=1)
    #Filter the data futher selecting message type with optional area selection and optional removal of all trains except passenger transports
    Berthdata=Berthdata[Berthdata['Area']==Areacode]
    #Remove all message types except berth movements
    Train=Berthdata[Berthdata['msg_type']=='CA']
    #Remove all train except passenger transports
    #pattern =r'[1-2][A-Z][0-9][0-9]'
    #Train=Train[Train['Headcode'].str.match(pattern)].reset_index(drop=True)
    
    #Example code for using pickles to save dataframes
    
    #pickle_out=open("filename.pickle","wb")
    #pickle.dump(Train, pickle_out)
    
    return Train
    
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
    BSdata['STP']=BSdata['tag'].str.slice(79,80)
    del BSdata['tag']
    BSdata=BSdata.reset_index()
    #Stores the STP type C messages for later
    STPC=BSdata[BSdata['STP']=='C'].reset_index(drop=True)
    for i in range(0,len(BSdata)-1):
        if BSdata['index'][i]==BSdata['index'][i+1]-1:
            BSdata=BSdata.drop([i])
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
        Timetabledata.at[i,'Headcode']=BSdata['Trainidentity'][k]
        Timetabledata.at[i,'Daysrun']=BSdata['Daysrun'][k]
        Timetabledata.at[i,'Datesrun']=BSdata['Datefrom'][k]+BSdata['Dateto'][k]
        Timetabledata.at[i,'STP']=BSdata['STP'][k]
        if Timetabledata['Msg_type'][i]=='LT':
            k=k+1
    Timetabledata['Datefrom']=pd.to_datetime('20'+Timetabledata['Datesrun'].str.slice(0,6),format='%Y%m%d')
    Timetabledata['Dateto']=pd.to_datetime('20'+Timetabledata['Datesrun'].str.slice(6,12),format='%Y%m%d')
    del Timetabledata['Datesrun']
    #Outputs the timetable data for the day as well as the cancellation messages
    return Timetabledata,STPC
    
#A function to read in a pickle data object 
def Pickle_read(pathtopickle):
    #Read in the pickle
    filename=pathtopickle
    pickle_in=open(filename,"rb")
    Data=pickle.load(pickle_in)
    return Data
    
#Read and process the STANOX-to-berth data effectively turning it into
# TIPLOC-to-berth data
def STANOXberth_processing(STANOXberthpath,STANOXberthfilename,TIPLOCSTANOXpath,TIPLOCSTANOXfilename):
    #Read in the data
    data_folder = STANOXberthpath
    filename = data_folder+STANOXberthfilename
    STANOXberth=pd.read_csv(filename)
    #Also requires the STANOX-TIPLOC data
    data_folder = TIPLOCSTANOXpath
    filename = data_folder+TIPLOCSTANOXfilename
    STANOXTIPLOC=pd.read_csv(filename)
    #Append TIPLOCs to the STANOXberth dataframe
    STANberth=STANOXberth.copy()
    STANberth['TIPLOC']=str(STANberth['STANOX'])
    for i in range(0,len(STANberth['STANOX'])):
        Interim=STANOXTIPLOC[STANOXTIPLOC['STANOX']==str(STANberth['STANOX'][i])].reset_index()
        if len(Interim)>0:
            STANberth.at[i,'TIPLOC']=Interim['TIPLOC'][0]
        else:
            STANberth.at[i,'TIPLOC']='NaN'
    
    #Removes all ofsets except the standard 'B' type offset
    STANberth=STANberth[STANberth['STEPTYPE']=='B'].reset_index(drop=True)
    return STANberth
    
#A function that adds the timetable to the berth movements
def Berth_movements_appender(Berthpickle,Timetablepickle,STANberthpickle,Areacode):
    #Timetable=Further_timetable_processing(Timetablepickle,STANberthpickle,Areacode)
    Timetable=Pickle_read(Timetablepickle)
    Train=Pickle_read(Berthpickle)
    Train['TIPLOC']=Train['Area']
    Train['Offset']=Train['Area']
    Train['Sched']=Train['Area']
    Headcode=Train['Headcode'].unique()
    Headcode2=Timetable['Headcode'].unique()
    Df=pd.DataFrame(columns=Train.columns)
    #Create a set of headcodes that are present in both the timetable
    #and the berth movements
    Headmerge=np.empty(0)
    k=0
    for i in range(0,len(Headcode)):
        for j in range(0,len(Headcode2)):
            if Headcode[i]==Headcode2[j]:
                k=k+1
                Headmerge=np.append(Headmerge,Headcode[i])
    #Iterates through all the headcodes and connects the berth movements
    # to the timetable
    for l in range(0,len(Headmerge)):
        PrepTrain=Train[Train['Headcode']==Headmerge[l]].reset_index(drop=True)
        PrepSched=Timetable[Timetable['Headcode']==Headmerge[l]].reset_index(drop=True)
        for i in range(0,len(PrepTrain['TIPLOC'])):
            for j in range(0,len(PrepSched['Location'])):
                for k in range(0,len(PrepSched['Berth'][j])):
                    if PrepTrain['From'][i]==PrepSched['Berth'][j][k][0] and PrepTrain['To'][i]==PrepSched['Berth'][j][k][1]:
                        PrepTrain.at[i,'TIPLOC']=PrepSched['Location'][j]
                        PrepTrain.at[i,'Offset']=int(PrepSched['Offset'][j][k])
                        PrepTrain.at[i,'Sched']=PrepSched['Time'][j]
        Df=Df.append(PrepTrain)
    Df=Df.reset_index(drop=True)
    Df['Delay']=Df['Area']
    for i in range(0,len(Df['Offset'])):
        if Df['Offset'][i]=='UR' or Df['Offset'][i]=='U2':
            Df.at[i,'Offset']=0
    Df['NewUTC']=Df['RealUTC']
    for i in range(0,len(Df['NewUTC'])):
        Df.at[i,'AdjUTC']=Df['NewUTC'][i]+timedelta(seconds=int(Df['Offset'][i]))
        if Df['Sched'][i]=='UR' or Df['Sched'][i]=='U2':
            Df.at[i,'Sched']=Df['AdjUTC'][i]
        Df.at[i,'Delay']=Df['AdjUTC'][i]-Df['Sched'][i]
    del Df['RealUTC']
    del Df['msg_type']
    del Df['UTC']
    return Df
    
#Calculates a measure of how delayed a train is
def Delay_calculation(Berthpickle,Timetablepickle,STANberthpickle,Areacode):
    Df=Berth_movements_appender(Berthpickle,Timetablepickle,STANberthpickle,Areacode)
    Df['Delay']=Df['Area']
    for i in range(0,len(Df['Offset'])):
        if Df['Offset'][i]=='UR':
            Df.at[i,'Offset']=0
    Df['NewUTC']=Df['RealUTC']
    for i in range(0,len(Df['NewUTC'])):
        Df.at[i,'AdjUTC']=Df['NewUTC'][i]+timedelta(seconds=Df['Offset'][i])
        if Df['Sched'][i]=='UR':
            Df.at[i,'Sched']=Df['AdjUTC'][i]
        Df.at[i,'Delay']=Df['AdjUTC'][i]-Df['Sched'][i]
    del Df['RealUTC']
    del Df['msg_type']
    del Df['UTC']
    return Df

