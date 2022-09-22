import pandas as pd
import sys
import flat_table as ft
import os
import numpy as np
import time
import json
from apexMatchVariables import matchXPlayed, dfMatchX, matchXSelect, dfPlayerDataMatchX, dfTeamDataMatchX

if os.path.exists('C:/ApexData/tokenSecret.json') and os.path.exists('C:/ApexData/APISecret.json'):
    print('Retrieving API details from json')
    with open('C:/ApexData/tokenSecret.json', 'r') as tokenFile:
        tokens = json.load(tokenFile)
    with open('C:/ApexData/APISecret.json', 'r') as APIFile:
        apiURL = json.load(APIFile)
else:
    with open('C:/ApexData/tokenSecret.json', 'w') as jsonFile:
        tokens = ['01234567-89abcdef0123456789abcd',
            '01234567-89abcdef0123456789abcd',
            '01234567-89abcdef0123456789abcd',
            '01234567-89abcdef0123456789abcd',
            '01234567-89abcdef0123456789abcd',
            '01234567-89abcdef0123456789abcd']
        json.dump((tokens), jsonFile)
    with open('C:/ApexData/APISecret.json', 'w') as jsonFile:
        apiURL = 'https://apex.api/details/here'
        json.dump((apiURL), jsonFile)
    print('Please open C:/ApexData and fill in the tokenSecret and APISecret files with your details.')
    print('You can have up to 24 match secret tokens!')
    time.sleep(1)
    input("Press ENTER to close this window...")
    sys.exit()

tokenCount = len(tokens)

def main():
    pd.options.mode.chained_assignment = None  # default='warn'

    matchDate = input("Enter the date of matches in YYYY-MM-DD format: ")

    #print('Thank you for using my Apex Legends custom match data processor!')
    #print('For more apps made by me, check out my website: https://bombersapps.com')

    if os.path.exists('C:/ApexData'):
        print('Data will be sent to C:/ApexData')
    else:
        os.mkdir('C:/ApexData')
        print('Data will be sent to C:/ApexData')

    matchNumber = 0
    df = pd.DataFrame()

    for i in tokens:
        matchNumber += 1
        print('Downloading data from match ' + str(matchNumber))
        data = pd.read_json(apiURL+i)
        dfItem = pd.DataFrame.from_records(data)
        dfItemFlat = ft.normalize(dfItem)
        dfItemFlat.index.name = 'global_index'
        dfItemFlat.columns = dfItemFlat.columns.str.replace('[.]','_',regex=True)
        dfItemFlat = dfItemFlat[dfItemFlat.data_rosters_rosterPlayers_teamName == dfItemFlat.data_rosters_rosterName]
        dfItemFlat.drop('index', axis=1, inplace=True)
        dfItemFlat.drop('token_timestamp', axis=1, inplace=True)
        dfItemFlat = dfItemFlat[(dfItemFlat['data_time_date'].str.startswith(matchDate))]
        df = pd.concat([df,dfItemFlat])

    for i in range(tokenCount):
        if tokens[i] in df['token'].values:
            matchXPlayed[i] = True

    if matchXPlayed[0] == False & matchXPlayed[1] == False & matchXPlayed[2] == False & matchXPlayed[3] == False & matchXPlayed[4] == False & matchXPlayed[5] == False & matchXPlayed[6] == False & matchXPlayed[7] == False & matchXPlayed[8] == False & matchXPlayed[9] == False & matchXPlayed[10] == False & matchXPlayed[11] == False & matchXPlayed[12] == False & matchXPlayed[13] == False & matchXPlayed[14] == False & matchXPlayed[15] == False & matchXPlayed[16] == False & matchXPlayed[17] == False & matchXPlayed[18] == False & matchXPlayed[19] == False & matchXPlayed[20] == False & matchXPlayed[21] == False & matchXPlayed[22] == False & matchXPlayed[23] == False:
        print('No matches have been played on '+matchDate)
        time.sleep(1)
        input("Press ENTER to close this window...")
        sys.exit()

    for i in range(tokenCount):
        if matchXPlayed[i] == True:
            print('Match '+str(i+1)+' valide for '+matchDate)

    print('Processing match data')
    dfMatchData = df.filter([
        'token',
        'match_start',
        'map_name',
        'aim_assist_allowed',
        'data_time_timezone',
        'data_time_timezone_type',
        'data_time_date'
        ], axis=1)
    dfMatchData['Match'] = df.apply(matchNumberer, axis=1)
    dfMatchData.rename(columns={
        'token': 'Token',
        'match_start': 'StartTime',
        'map_name': 'MapName',
        'aim_assist_allowed': 'AimHackOn',
        'data_time_timezone': 'TimeZone',
        'data_time_timezone_type': 'TimeZoneType',
        'data_time_date': 'Date'
        }, inplace=True)
    dfMatchData = dfMatchData[[
        'Match',
        'Date',
        'MapName',
        'AimHackOn',
        'TimeZone',
        'TimeZoneType',
        'Token',
        'StartTime'
        ]]
    dfMatchFilter = dfMatchData.groupby(['Token','StartTime']).count()
    dfMatchFilter = dfMatchFilter[dfMatchFilter['Match'] > 20]
    dfMatchFilter.drop(['MapName','AimHackOn','TimeZone','TimeZoneType','Date'], axis=1, inplace=True)
    dfMatchFilter.reset_index(inplace=True)
    col_list = dfMatchFilter.StartTime.values.tolist()
    dfMatchData = dfMatchData[dfMatchData.StartTime.isin(col_list)]
    dfMatchData.reset_index(inplace=True)
    dfMatchData.drop_duplicates(inplace=True, ignore_index=True)
    dfMatchData.drop('global_index', axis=1, inplace=True)
    dfMatchData.drop_duplicates(inplace=True)
    dfMatchDataStarts = dfMatchData.filter({
        'Match',
        'Date'
    })
    dfMatch0 = pd.DataFrame(columns=['Date', 'Match'], data=[[np.nan,'No Match']])

    for i in range(tokenCount):
        dfMatchX[i] = dfMatchDataStarts[dfMatchDataStarts['Match']==('Match '+str(i+1))]
    
    os.system('cls')

    for i in range(tokenCount):
        if len(dfMatchX[i]) > 0:
            dfMatchX[i] = pd.concat([dfMatchX[i],dfMatch0])
            dfMatchX[i].reset_index(drop=True, inplace=True)
            dfMatchX[i].index.rename('Match Index', inplace=True)
            print(dfMatchX[i])
            matchXSelect[i] = input('Select which match index you want to use for match '+str(i+1)+': [0] ') or '0'
            matchXSelect[i] = (dfMatchX[i].iat[int(matchXSelect[i]),0])
            os.system('cls')

    print('Enter the date of matches in YYYY-MM-DD format: 2022-09-11\r\nData will be sent to C:/ApexData\r\nDownloading data from match 1\r\nDownloading data from match 2\r\nDownloading data from match 3\r\nDownloading data from match 4\r\nDownloading data from match 5\r\nDownloading data from match 6\r\nMatch 1 valid for 2022-09-11\r\nMatch 2 valid for 2022-09-11\r\nMatch 3 valid for 2022-09-11\r\nMatch 4 valid for 2022-09-11\r\nMatch 5 valid for 2022-09-11\r\nMatch 6 valid for 2022-09-11\r\nProcessing match data')

    validMatchDates = []

    for i in range(tokenCount):
        validMatchDates.append(matchXSelect[i])

    dfMatchData = dfMatchData.loc[dfMatchData.Date.isin(validMatchDates)]
    validMatchStarts = dfMatchData.StartTime.values.tolist()

    print('Processing player data')
    dfPlayerData = df.filter([
        'token',
        'match_start',
        'data_rosters_rosterPlayers_playerName',
        'data_rosters_rosterPlayers_teamName',
        'data_rosters_rosterPlayers_characterName',
        'data_rosters_rosterPlayers_teamNum',
        'data_rosters_rosterPlayers_shots',
        'data_rosters_rosterPlayers_hits',
        'data_rosters_rosterPlayers_headshots',
        'data_rosters_rosterPlayers_damageDealt',
        'data_rosters_rosterPlayers_knockdowns',
        'data_rosters_rosterPlayers_kills',
        'data_rosters_rosterPlayers_assists',
        'data_rosters_rosterPlayers_respawnsGiven',
        'data_rosters_rosterPlayers_survivalTime',
        'data_rosters_rosterPlacement',
        'data_time_date'
        ], axis=1)
    dfPlayerData.rename(columns={
        'token': 'Token',
        'match_start': 'StartTime',
        'data_rosters_rosterPlayers_playerName': 'PlayerName',
        'data_rosters_rosterPlayers_teamName': 'TeamName',
        'data_rosters_rosterPlayers_characterName': 'Legend',
        'data_rosters_rosterPlayers_teamNum': 'TeamID',
        'data_rosters_rosterPlayers_shots': 'Shots',
        'data_rosters_rosterPlayers_hits': 'Hits',
        'data_rosters_rosterPlayers_headshots': 'Headshots',
        'data_rosters_rosterPlayers_damageDealt': 'Damage',
        'data_rosters_rosterPlayers_knockdowns': 'Knocks',
        'data_rosters_rosterPlayers_kills': 'Kills',
        'data_rosters_rosterPlayers_assists': 'Assists',
        'data_rosters_rosterPlayers_respawnsGiven': 'Respawns',
        'data_rosters_rosterPlayers_survivalTime': 'SurvivalTime',
        'data_rosters_rosterPlacement': 'TeamPlacement',
        }, inplace=True)
    dfPlayerData['Match'] = df.apply(matchNumberer, axis=1)
    dfPlayerDataTotal = dfPlayerData
    dfPlayerDataTotal['PlacementScore'] = df.apply(placementScore, axis=1)
    dfPlayerData['Hit%'] = dfPlayerData['Shots'].where(dfPlayerData['Shots'] > 0, 1)
    dfPlayerData['Hit%'] = dfPlayerData['Hits']/dfPlayerData['Hit%']*100
    dfPlayerData['Headshot%'] = dfPlayerData['Shots'].where(dfPlayerData['Shots'] > 0, 1)
    dfPlayerData['Headshot%'] = dfPlayerData['Headshots']/dfPlayerData['Headshot%']*100
    dfPlayerData = dfPlayerData[[
        'Match',
        'PlayerName',
        'Shots',
        'Hits',
        'Hit%',
        'Headshots',
        'Headshot%',
        'Damage',
        'Knocks',
        'Kills',
        'Assists',
        'Respawns',
        'SurvivalTime',
        'TeamName',
        'TeamPlacement',
        'TeamID',
        'Legend',
        'Token',
        'StartTime'
        ]]
    dfPlayerData = dfPlayerData[dfPlayerData.StartTime.isin(validMatchStarts)]
    dfPlayerData.reset_index(inplace=True)
    dfPlayerData.drop_duplicates(inplace=True, ignore_index=True)
    dfPlayerData.drop('global_index', axis=1, inplace=True)
    dfPlayerData.drop_duplicates(inplace=True)

    for i in range(tokenCount):
        dfPlayerDataMatchX[i] = dfPlayerData[dfPlayerData['Match']==('Match ' + str(i+1))]
        dfPlayerDataMatchX[i].sort_values(by=['TeamPlacement'], inplace=True)

    dfPlayerDataTotal['Score'] = dfPlayerDataTotal['PlacementScore']/3 + dfPlayerDataTotal['Kills'] + dfPlayerDataTotal['Assists']/2
    dfPlayerDataTotal = dfPlayerDataTotal[[
        'PlayerName',
        'Shots',
        'Hits',
        'Headshots',
        'Damage',
        'Knocks',
        'Kills',
        'Assists',
        'Score'
        ]]
    dfPlayerDataTotal = dfPlayerDataTotal.groupby(['PlayerName']).sum()
    dfPlayerDataTotal.reset_index(inplace=True)
    dfPlayerDataTotal['Hit%'] = dfPlayerDataTotal['Shots'].where(dfPlayerDataTotal['Shots'] > 0, 1)
    dfPlayerDataTotal['Hit%'] = dfPlayerDataTotal['Hits']/dfPlayerDataTotal['Hit%']*100
    dfPlayerDataTotal['Headshot%'] = dfPlayerDataTotal['Shots'].where(dfPlayerDataTotal['Shots'] > 0, 1)
    dfPlayerDataTotal['Headshot%'] = dfPlayerDataTotal['Headshots']/dfPlayerDataTotal['Headshot%']*100
    dfPlayerDataTotal = dfPlayerDataTotal[[
        'PlayerName',
        'Shots',
        'Hits',
        'Hit%',
        'Headshots',
        'Headshot%',
        'Damage',
        'Knocks',
        'Kills',
        'Assists',
        'Score'
        ]]
    dfPlayerDataMVP = dfPlayerDataTotal.copy()
    dfPlayerDataMVP.sort_values(by=['Score'], ascending=False, inplace=True)
    dfPlayerDataMVP.reset_index(drop=True, inplace=True)
    dfPlayerDataHS = dfPlayerDataTotal.copy()
    dfPlayerDataHS.sort_values(by=['Headshot%'], ascending=False, inplace=True)
    dfPlayerDataHS.reset_index(drop=True, inplace=True)
    dfPlayerDataAccuracy = dfPlayerDataTotal.copy()
    dfPlayerDataAccuracy.sort_values(by=['Hit%'], ascending=False, inplace=True)
    dfPlayerDataAccuracy.reset_index(drop=True, inplace=True)

    playerMatchList = []
    for i in range(tokenCount):
        playerMatchList.append(dfPlayerDataMatchX[i])

    print('Processing team data')
    dfTeamData = df.filter([
        'token',
        'match_start',
        'data_rosters_rosterName',
        'data_rosters_rosterPlacement',
        'data_rosters_rosterDmg',
        'data_rosters_rosterKills',
        'data_rosters_rosterAssists',
        'data_time_date'
        ], axis=1)
    dfTeamData.rename(columns={
        'token': 'Token',
        'match_start': 'StartTime',
        'data_rosters_rosterName': 'Name',
        'data_rosters_rosterPlacement': 'Placement',
        'data_rosters_rosterDmg': 'Damage',
        'data_rosters_rosterKills': 'Kills',
        'data_rosters_rosterAssists': 'Assists'
        }, inplace=True)
    dfTeamData['Match'] = df.apply(matchNumberer, axis=1)
    dfTeamData['PlacementScore'] = df.apply(placementScore, axis=1)
    dfTeamData['Score'] = dfTeamData['PlacementScore'] + dfTeamData['Kills']
    dfTeamData = dfTeamData[[
        'Match',
        'Name',
        'Placement',
        'PlacementScore',
        'Damage',
        'Kills',
        'Assists',
        'Score',
        'Token',
        'StartTime'
        ]]
    dfTeamData = dfTeamData[dfTeamData.StartTime.isin(validMatchStarts)]
    dfTeamData.reset_index(drop=True, inplace=True)
    dfTeamData.drop_duplicates(ignore_index=True, inplace=True)

    for i in range(tokenCount):
        dfTeamDataMatchX[i] = dfTeamData[dfTeamData['Match']==('Match ' + str(i+1))]
        dfTeamDataMatchX[i].sort_values(by=['Placement'], inplace=True)

    dfTeamDataTotal = dfTeamData[[
        'Name',
        'Damage',
        'Kills',
        'Assists',
        'Score',
        ]]
    dfTeamDataTotal = dfTeamDataTotal.groupby(['Name']).sum()
    dfTeamDataTotal.sort_values(by=['Score'], ascending=False, inplace=True)
    dfTeamDataTotal.reset_index(inplace=True)
 
    teamMatchList = []
    for i in range(tokenCount):
        teamMatchList.append(dfTeamDataMatchX[i])

    #print('Writing HTML files')
    #matchPrinter = 0
    #for i in playerMatchList:
    #    matchPrinter += 1
    #    htmlMatch = i.to_html()
    #    text_file = open ( 'C:/ApexData/playerMatch'+str(matchPrinter)+'.html' , 'w', encoding='utf-8')
    #    text_file.write(htmlMatch)
    #    text_file.close()
    #matchPrinter = 0
    #for i in teamMatchList:
    #    matchPrinter += 1
    #    htmlMatch = i.to_html()
    #    text_file = open ( 'C:/ApexData/teamMatch'+str(matchPrinter)+'.html' , 'w', encoding='utf-8')
    #    text_file.write(htmlMatch)
    #    text_file.close()
    #htmlMatch = dfMatchData.to_html()
    #text_file = open ( 'C:/ApexData/match.html' , 'w', encoding='utf-8')
    #text_file.write(htmlMatch)
    #text_file.close()
    #htmlMatch = dfTeamDataTotal.to_html()
    #text_file = open ( 'C:/ApexData/teamTotals.html' , 'w', encoding='utf-8')
    #text_file.write(htmlMatch)
    #text_file.close()
    #htmlMatch = dfPlayerDataMVP.to_html()
    #text_file = open ( 'C:/ApexData/playerMVP.html' , 'w', encoding='utf-8')
    #text_file.write(htmlMatch)
    #text_file.close()
    #htmlMatch = dfPlayerDataHS.to_html()
    #text_file = open ( 'C:/ApexData/playerHS.html' , 'w', encoding='utf-8')
    #text_file.write(htmlMatch)
    #text_file.close()
    #htmlPlayer = dfPlayerData.to_html()
    #text_file = open ( 'C:/ApexData/player.html' , 'w', encoding='utf-8')
    #text_file.write(htmlPlayer)
    #text_file.close()
    #htmlTeam = dfTeamData.to_html()
    #text_file = open ( 'C:/ApexData/team.html' , 'w', encoding='utf-8')
    #text_file.write(htmlTeam)
    #text_file.close()

    print('Writing to Excel workbook')
    excelWriter = pd.ExcelWriter('C:/ApexData/'+matchDate+'.xlsx', engine='xlsxwriter')
    dfMatchData.to_excel(excelWriter,
    sheet_name='Matches',
    index=False,
    encoding='utf-8')
    dfTeamDataTotal.to_excel(excelWriter,
    sheet_name='Team Totals',
    index=False,
    encoding='utf-8')
    matchPrinter = 0
    for i in teamMatchList:
        matchPrinter += 1
        i.to_excel(excelWriter,
        sheet_name='TD Match '+str(matchPrinter),
        index=False,
        encoding='utf-8')
    dfPlayerDataMVP.to_excel(excelWriter,
    sheet_name='Day MVP',
    index=False,
    encoding='utf-8')
    dfPlayerDataAccuracy.to_excel(excelWriter,
    sheet_name='Accuracy',
    index=False,
    encoding='utf-8')
    dfPlayerDataHS.to_excel(excelWriter,
    sheet_name='Headshot',
    index=False,
    encoding='utf-8')
    matchPrinter = 0
    for i in playerMatchList:
        matchPrinter += 1
        i.to_excel(excelWriter,
        sheet_name='PD Match '+str(matchPrinter),
        index=False,
        encoding='utf-8')
    excelWriter.save()

    print('Done!')
    time.sleep(1)
    input("Press ENTER to close this window...")

def matchNumberer(row):
    for i in range(tokenCount):
        if row['token'] == tokens[i]:
            val = 'Match ' + str(i+1)
    return val

def placementScore(row):
    if row['data_rosters_rosterPlacement'] == 1:
        val = 12
    elif row['data_rosters_rosterPlacement'] == 2:
        val = 9
    elif row['data_rosters_rosterPlacement'] == 3:
        val = 7
    elif row['data_rosters_rosterPlacement'] == 4:
        val = 5
    elif row['data_rosters_rosterPlacement'] == 5:
        val = 4
    elif row['data_rosters_rosterPlacement'] == 6:
        val = 3
    elif row['data_rosters_rosterPlacement'] == 7:
        val = 3
    elif row['data_rosters_rosterPlacement'] == 8:
        val = 2
    elif row['data_rosters_rosterPlacement'] == 9:
        val = 2
    elif row['data_rosters_rosterPlacement'] ==10:
        val = 2
    elif row['data_rosters_rosterPlacement'] == 11:
        val = 1
    elif row['data_rosters_rosterPlacement'] == 12:
        val = 1
    elif row['data_rosters_rosterPlacement'] == 13:
        val = 1
    elif row['data_rosters_rosterPlacement'] == 14:
        val = 1
    elif row['data_rosters_rosterPlacement'] == 15:
        val = 1
    else:
        val = 0
    return val

if __name__ == '__main__':
    main()