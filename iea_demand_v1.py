"""
Created on Mon Oct  7 10:29:21 2019
@author: harshit.mahajan
"""
# This script analysis the IEA monthly data 
import pandas as pd
from datetime import datetime, date

# Import the OECD Demand File 
path = 'XXXX'

#Save the raw data to oecddata dataframe 
oecddata = pd.read_csv(path)
oecddata.country.unique()

#Get the list of all countries/regions in OECD Demand File
oecdCountryList = ['AUSTRALI', 'BELGIUM', 'CANADA', 'CHILE', 'DENMARK','ESTONIA', 'FINLAND', 'FRANCE', 'GERMANY', 
                   'ITALY', 'JAPAN', 'KOREA', 'LATVIA','LUXEMBOU', 'MEXICO', 'NETHLAND', 'NZ', 'NORWAY', 'SLOVAKIA',
                   'SLOVENIA', 'PORTUGAL', 'SPAIN', 'SWEDEN', 'SWITLAND', 'TURKEY','UK', 'USA', 'CZECH', 'HUNGARY', 'POLAND', 'US50', 'USTER',
                   'OECDTOT', 'BENELUX', 'EUROOTH', 'SCANDIN', 'OECDEUR', 'EURO4',
                   'OECDG9', 'OECDAME', 'OECDAOC', 'LITHUANIA']

# Create a sublist of major countries in OECD
oecdRefinedCountryList = ['AUSTRALI', 'CANADA', 'FRANCE', 'GERMANY', 'ITALY', 'JAPAN', 'KOREA', 'MEXICO', 'NETHLAND', 'NZ', 'NORWAY', 'UK', 'USA', 'US50']
oecdBlocks = ['OECDEUR', 'EURO4','OECDG9', 'OECDAME', 'OECDAOC']

# Import the Non-OECD Demand File 
# Only quarterly and annual data is available
pathNon = 'XXXX'

#Save the raw data to nonoecddata dataframe 
nonoecddata= pd.read_csv(pathNon)
nonoecddata.country.unique()

#Get the list of all countries/regions in Non OECD Demand File
nonoecdCountryList = ['ARMENIA', 'AZERBAIJAN', 'BELARUS', 'GEORGIA', 'KAZAKHSTAN','KYRGYZSTAN', 'LITHUANIA', 'MOLDOVA', 'RUSSIA', 'TAJIKISTAN',
       'TURKMENIST', 'UKRAINE', 'UZBEKISTAN', 'FORMERUSSR', 'ALBANIA','BULGARIA', 'CYPRUS', 'GIBRALTAR', 'MALTA', 'ROMANIA','BOSNIAHERZ', 'CROATIA', 
       'FYROM', 'MONTENEGRO', 'KOSOVO', 'SERBIA','FORMERYUGO', 'OMRNEUR', 'CHINA', 'BANGLADESH', 'BRUNEI','CAMBODIA', 'HONGKONG', 'INDIA', 'INDONESIA', 'MALAYSIA',
       'MONGOLIA', 'MYANMAR', 'NEPAL', 'KOREADPR', 'PAKISTAN','PHILIPPINE', 'SINGAPORE', 'SRILANKA', 'TAIPEI', 'THAILAND','VIETNAM', 'OTHERASIA', 'OMROTHASIA', 'ARGENTINA', 'BOLIVIA',
       'BRAZIL', 'COLOMBIA', 'COSTARICA', 'CUBA', 'CURACAO', 'DOMINICANR','ELSALVADOR', 'ECUADOR', 'GUATEMALA', 'HAITI', 'HONDURAS','JAMAICA', 'NICARAGUA', 'PANAMA', 'PARAGUAY', 'PERU', 
       'SURINAME','TRINIDAD', 'URUGUAY', 'VENEZUELA', 'OTHERLATIN', 'OMRLAM','BAHRAIN', 'IRAN', 'IRAQ', 'JORDAN', 'KUWAIT', 'LEBANON', 'OMAN','QATAR', 'SAUDIARABI', 'SYRIA', 
       'UAE', 'YEMEN', 'MIDDLEEAST','ALGERIA', 'ANGOLA', 'BENIN', 'CAMEROON', 'CONGO', 'EGYPT','ETHIOPIA', 'GABON', 'GHANA', 'COTEIVOIRE', 'KENYA', 'LIBYA','MOROCCO', 'MOZAMBIQUE', 
       'NIGERIA', 'SENEGAL', 'SOUTHAFRIC','SSUDAN', 'SUDAN', 'TANZANIA', 'TUNISIA', 'CONGOREP', 'ZAMBIA','ZIMBABWE', 'OTHERAFRIC', 'AFRICA', 'NONOECD']

# Create a sublist of major countries in OECD
nonoecdRefinedCountryList = ['RUSSIA','CHINA', 'INDIA','ARGENTINA','BRAZIL', 'COLOMBIA']

oecdCountryDemand  = pd.DataFrame()
oecdBlockDemand = pd.DataFrame()
nonoecdCountryDemand = pd.DataFrame()
oecdDemand = pd.DataFrame()
nonoecdDemand = pd.DataFrame() 

frequencyList = ['Y','Q']

for frequency in frequencyList:
    
    print(frequency)
    #Subseting the data to get the OECD Total 
    oecdDemand = pd.concat([oecdDemand, oecddata[(oecddata['country'] == 'OECDTOT' ) & (oecddata['frequency_id'] == frequency ) & (oecddata['product'] == 'TOTPRODS')][['vintage_id','country','product','metric_id', 'frequency_id','date_id',
                                                                                                'date_conv','value_id','unit_id']]])
    #Loop through refiend country list and put it in a dataframe  
    for country in oecdRefinedCountryList :
        print(country)
        oecdCountryDemand = pd.concat([oecdCountryDemand, oecddata[(oecddata['country'] == country) & (oecddata['frequency_id'] == frequency ) & (oecddata['product'] == 'TOTPRODS') & (oecddata['date_id']> '2017-01-01')][['vintage_id','country',
                                   'product','metric_id', 'frequency_id','date_id','date_conv','value_id','unit_id']]])

    #Loop through each OECD block in the list and put it in a dataframe  
    for country in oecdBlocks:
        print(country)
        oecdBlockDemand = pd.concat([oecdBlockDemand, oecddata[(oecddata['country'] == country) & (oecddata['frequency_id'] == frequency ) & (oecddata['product'] == 'TOTPRODS') & (oecddata['date_id']> '2017-01-01')][['vintage_id','country',
                                   'product','metric_id', 'frequency_id','date_id','date_conv','value_id','unit_id']]])        

    #Subseting the data to get the OECD Total 
    nonoecdDemand = pd.concat([nonoecdDemand, nonoecddata[(nonoecddata['country'] == 'NONOECD' ) & (nonoecddata['frequency_id'] == frequency ) & (nonoecddata['product'] == 'Total')][['vintage_id','country',
                                                                                              'product','metric_id', 'frequency_id','date_id','date_conv','value_id','unit_id']]])
    #Loop through refiend country list and put it in a dataframe  
    for country in nonoecdRefinedCountryList :
        print(country)
        nonoecdCountryDemand = pd.concat([nonoecdCountryDemand , nonoecddata[(nonoecddata['country'] == country) & (nonoecddata['frequency_id'] == frequency ) & 
                                        (nonoecddata['product'] == 'Total') & (nonoecddata['date_id']> '2017-01-01')][['vintage_id','country','product',
                                        'metric_id', 'frequency_id','date_id','date_conv','value_id','unit_id']]])
    


totalDemand = pd.concat([oecdDemand, nonoecdDemand])
totalDemand.columns
totalDemand = totalDemand.groupby(['vintage_id', 'date_conv', 'frequency_id'])['value_id'].sum()
totalDemand = totalDemand.reset_index(level = totalDemand.index.names)

year_selection= ['2020', '2021']

totalDemandOecd_year = oecdDemand[(oecdDemand['frequency_id'] == 'Y') & (oecdDemand['date_conv'] == year_selection)]
totalDemandNonoecd_year = nonoecdDemand[(nonoecdDemand['frequency_id'] == 'Y') & (nonoecdDemand['date_conv'] == year_selection)]
totalDemandbyBlock = pd.concat([totalDemandOecd_year,totalDemandNonoecd_year ])

filePath = 'XXXX'

#Save the Excel output
newfile = filePath + 'IEA_Demand_Analysis_' + datetime.now().strftime ('%Y%m%d-%H%M%S') + '.xlsx'        

#write to excel 
dfs = {'oecdDemand':oecdDemand,'oecdCountryDemand': oecdCountryDemand, 'oecdBlockDemand':oecdBlockDemand, 'nonoecdDemand':nonoecdDemand, 
       'nonoecdCountryDemand': nonoecdCountryDemand , 'oecdDemand': oecdDemand, 'nonoecdDemand': nonoecdDemand,'totalDemand': totalDemand, 'totalDemandbyBlock': totalDemandbyBlock}

writer = pd.ExcelWriter(newfile, engine = 'xlsxwriter')

for sheetname in dfs.keys():
    dfs[sheetname].to_excel(writer, sheet_name=sheetname, index = False)
writer.save()    
