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
    

