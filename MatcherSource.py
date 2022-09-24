import pandas as pd
import numpy as np
import pyodbc
from datetime import date
import re

# Functional Programing for Inputs #

# Default Inputs
defaultStartDate = str(date.today())
defaultTight = .02
defaultLoose = .15

# User Inputs
print('SQL Inputs:')
print('Enter CWR DataFrame Date Range')
print("Date in 'aset_cret_dttm' column")
startDate = input('Earliest Date (YYYY-MM-DD): ')
endDate = input('Latest Date (YYYY-MM-DD): ')
rowLimit = input('Maximum Row Count: ')
print()
print('MP Ranges:')
tight = input('Tight MP +/-: ')
loose = input('Loose MP +/-: ')
print()
print('Save to:')
directory = input('Output Directory: ')
while directory == '':
    directory = input('Directory must be specified: ')
fileName = input('Output FileName: ')



# Default or Inputs
if startDate == '':
    startDate = defaultStartDate
if endDate == '':
    endDate = startDate

if fileName == '':
    fileName = startDate

if tight == '':
    tight = defaultTight
else:
    tight = float(tight)
if loose == '':
    loose = defaultLoose
else:
    loose = float(loose)

if rowLimit != '':
    rowLimit = int(rowLimit)

startDate += ' 00:00:00.00'
endDate += ' 23:59:59.999'


print('___________________________________')
print()
# Import Thor Data through DataBricks
print('Databricks Connection Start')

table_name = "#####"
# Connect to the Databricks cluster by using the
# Data Source Name (DSN) that you created earlier.
conn = pyodbc.connect("DSN=######", autocommit=True)

datetime = 'aset_cret_dttm'
if rowLimit != '':
    query = f"SELECT * FROM {table_name} WHERE {datetime} BETWEEN '{startDate}' AND '{endDate}' LIMIT {rowLimit}"
else:
    query = f"SELECT * FROM {table_name} WHERE {datetime} BETWEEN '{startDate}' AND '{endDate}'"
# Convert to pandas DF
print()
print('IGNORE THIS WARNING -')
dfThor = pd.read_sql(query, conn)
# Only Joint Bars, must be valid
print()
print('Rows within Date Range:', len(dfThor))
dfThor = dfThor[dfThor['aset_sub_typ'] == 'Joint_Bar'].reset_index(drop = True)
print('Rows with Joint Bar:', len(dfThor))
dfThor = dfThor[dfThor['aset_vald_ind'] == 'Y'].reset_index(drop = True)
print('Rows with Valid Asset:', len(dfThor))

print('Databricks Connection End')
print('___________________________________')
print()


# Import CWR Data through DB2
print('DB2 Connection Start')
print()
print('IGNORE THIS WARNING -')

table_name = "#####"
cnx = pyodbc.connect('DSN=######; UID=#######; PWD=######')
query = 'Select Asset_ID, LINE_SEG_NBR, MP_SFX, TRK_TYP_CD, TRK_NBR, RAIL_POS_DESC, Track_Status_DESC, Longitude, Latitude from WP.TRAIL_JOINTS'
dfCwr = pd.read_sql(query, cnx)

print('DB2 Connection End')
print('___________________________________')
print()

# End of Functional Program #


### OOP Programming ###

# thorRow / cwrRow Objects
class Row:
    def __init__(self, ID, line, track, mile, rail, index):
        self.id = ID
        self.ls = line
        self.tn = track

        self.mp = mile
        self.rp = rail
        self.i = index

        # Why list
        self.why = []
        # List of asset IDs that properly matched
        self.matches = []
        # Match Indexes
        self.matchIndexes = []



    def appendMatchIndex(self, newMatch):
        self.matchIndexes.append(newMatch)



    def getID(self):
        return self.id
    def getLS(self):
        return self.ls
    def getTN(self):
        return self.tn
    def getMP(self):
        return self.mp
    def getRP(self):
        return self.rp
    def geti(self):
        return self.i


    def getWhy(self):
        return self.why
    def getMatches(self):
        return self.matches
    def getMatchIndexes(self):
        return self.matchIndexes

    def removeMatch(self, input):
        self.matchIndexes.remove(input)
    def clearMatches(self):
        self.matchIndexes = []



    def makeWhy(self, cwrList):
        for cwrRow in cwrList:

            if abs(self.mp - cwrRow.getMP()) <= loose\
            and self.ls == cwrRow.getLS()\
            and self.tn == cwrRow.getTN():

                self.matches.append(int(cwrRow.getID()))
                self.matchIndexes.append(cwrRow.geti())
                cwrRow.appendMatchIndex(self.i)

                mpSync = True
                rpSync = True

                if abs(self.mp - cwrRow.getMP()) >= tight:
                    mpSync = False
                if self.rp != cwrRow.getRP():
                    rpSync = False

                if mpSync == True:
                    if rpSync == False:
                        self.why.append({int(cwrRow.getID()) : 'Rail OOS'})
                    else:
                        self.why.append({int(cwrRow.getID()) : 'Perfect Match'})
                else:
                    if rpSync == True:
                        self.why.append({int(cwrRow.getID()) : 'MP OOS'})
                    else:
                        self.why.append({int(cwrRow.getID()) : 'MP/Rail OOS'})

    def fillBlankWhy(self):
        # No Match
        if len(self.why) > 0:
            pass
        elif len(self.why) == 0 and len(self.matches) == 0:
            self.why = 'No Matches'
            self.matches = '-'
        # Something went wrong
        else:
            self.why.append('Error')
            self.matches.append('Error')

    def run(self, cwrList):
        self.makeWhy(cwrList)
        self.fillBlankWhy()


# Column Creation
class Columns:
    def __init__(self, dfThor, dfCwr):
        # List holding dataframe rows as "thorRow" objects
        self.thorList = []
        self.cwrList = []
        # Lists to hold whys and matches
        self.whyList = []
        self.matchList = []
        # Best Match Lists
        self.bestMatches = []
        self.bestMatchReasons = []
        self.caseNumbers = []
        # Rail Master Info
        self.exceptions = []
        self.cwrMPs = []
        self.cwrRPs = []

        # Create thorRow objects
        for i in range(len(dfThor['aset_rid'])):

            # Prime Best Match Lists
            self.bestMatches.append('')
            self.bestMatchReasons.append('')
            self.caseNumbers.append('')
            self.exceptions.append('')
            self.cwrMPs.append('')
            self.cwrRPs.append('')


            # Mile Post Intialization
            milePost = dfThor['aset_loctn_mp_nbr'][i]

            if milePost not in [None, '']:
                milePost = float(milePost)
            else:
                milePost = -1

            self.thorList.append(
                Row(
                    # ID Number
                    dfThor['aset_rid'],

                    # Line Segment Number
                    int(dfThor['aset_loctn_lin_seg_nbr'][i]),

                    # Track Number
                    str(dfThor['aset_loctn_trak_nbr'][i]),

                    # Mile Post
                    milePost,

                    # Rail Position
                    dfThor['geaometry_rail_posn_nme'][i].upper(),

                    # Index
                    i
                )
            )
        # Create cwrRow Objects
        for i in range(len(dfCwr['ASSET_ID'])):

            rail_position = dfCwr['RAIL_POS_DESC'][i]

            if rail_position != None:
                rail_position = rail_position.split()[0]

            self.cwrList.append(
                Row(
                    # ID Number
                    dfCwr['ASSET_ID'][i],

                    # Line Segment Number
                    int(dfCwr['LINE_SEG_NBR'][i]),

                    # Track Number
                    str(dfCwr['TRK_NBR'][i]),

                    # Mile Post
                    float(re.findall("\d+\.\d+",dfCwr['MP_SFX'][i])[0]),


                    # Rail Position
                    rail_position,

                    # Index
                    i
                )
            )
# End of Initialization

    # Getters
    def getThor(self):
        return self.thorList
    def getCwr(self):
        return self.cwrList
    def getWhy(self):
        return self.whyList
    def getMatches(self):
        return self.matchList
    def getBestMatches(self):
        return self.bestMatches
    def getBestMatchReasons(self):
        return self.bestMatchReasons
    def getCaseNumbers(self):
        return self.caseNumbers
    def getExceptions(self):
        return self.exceptions
    def getCwrMPs(self):
        return self.cwrMPs
    def getCwrRPs(self):
        return self.cwrRPs


    ### Main Program Functions ###
    # Creates list of all potential matches
    def potentialMatches(self):
        print("Start Matches | Why:")
        print('Total Row Count:', len(self.thorList))
        for thorRow in self.thorList:
            thorRow.run(self.cwrList)
            self.whyList.append(thorRow.getWhy())
            self.matchList.append(thorRow.getMatches())
            if (thorRow.geti()) % 1000 == 0:
                print('Row ' + str(thorRow.geti()) + '...')
        print('End.')
        print('___________________________________')
        print()
    # Function used in dynamic matching
    # Adds object to best match list, removes index from cwr/thor object lists.
    def addAndRemove(self, index, cwrRow, reason, caseNum):
        # Append best match to bestMatches
        self.bestMatches[index] = cwrRow
        self.bestMatchReasons[index] = reason
        self.caseNumbers[index] = caseNum
        # clear match lists
        self.thorList[index].clearMatches()
        self.cwrList[cwrRow.geti()].clearMatches()
        # Remove CWR index from Thor matchList
        for tRow in self.thorList:
            if cwrRow.geti() in tRow.getMatchIndexes():
                tRow.removeMatch(cwrRow.geti())
                break
        # Remove Thor index from CWR matchList
        for cRow in self.cwrList:
            if index in cRow.getMatchIndexes():
                cRow.removeMatch(index)
                break
    # Dynamic Matching - More Accurate
    def dynamicMatching(self):
        print('Dynamic Matching Start')
        # More runs may lead to more good matches, not proven, doesn't significantly affect performance
        for tenRuns in range(10):
            for i in range(len(self.thorList)):
                if self.bestMatches[i] == '':
                    thorRow = self.thorList[i]
                    if thorRow.getMatches() == '-':
                        self.bestMatches[i] = '-'
                        self.bestMatchReasons[i] = '-'
                        self.caseNumbers[i] = '-'
                    elif len(thorRow.getMatchIndexes()) == 0:
                        self.bestMatches[i] = 'Taken'
                        self.bestMatchReasons[i] = 'Available Matches Already Used'
                        self.caseNumbers[i] = 0
                    elif len(thorRow.getMatchIndexes()) == 1:
                        for cwrRow in self.cwrList:
                            # If cwrRow is a match
                            if cwrRow.geti() in thorRow.getMatchIndexes():
                                # If the length of both match Lists == 1 then they only match to eachother
                                if len(cwrRow.getMatchIndexes()) == 1:
                                    reason = 'Thor=1, CWR=1'
                                    caseNum = 1
                                    self.addAndRemove(i, cwrRow, reason, caseNum)
                                # If length of cwrMatches > 1
                                elif len(cwrRow.getMatchIndexes()) > 1:
                                    # Check to see if the thors it matched to also only match to it
                                    oneMatch = oneMatchCheck(cwrRow, self.thorList)
                                    # If multiple thors only match to this particular cwr
                                    if oneMatch == True:
                                        # Check if rail pos match
                                        rpSync = rpSyncCheck(cwrRow, self.thorList)
                                        # if no rails match
                                        if len(rpSync) == 0:
                                            # Check to see if they are within the inner mile post range:
                                            mpSync = mpSyncCheck(cwrRow, self.thorList)
                                            # if no rail or mp sync, find closest
                                            if len(mpSync) == 0:
                                                reason = 'Thor=1 CWR>1 | RP=0 MP=0, Distance'
                                                caseNum = 2
                                                closest = findClosestList(cwrRow, self.thorList, rpSync)
                                                self.addAndRemove(closest.geti(), cwrRow, reason, caseNum)
                                                if closest.geti() != i:
                                                    self.bestMatches[i] = 'Taken'
                                                    self.bestMatchReasons[i] = 'Thor:1 CWR:>1 Taken'
                                                    self.caseNumbers[i] = 3
                                            elif len(mpSync) == 1:
                                                reason = 'Thor=1 CWR>1 | RP=0 MP=1'
                                                caseNum = 4
                                                self.addAndRemove(mpSync[0], cwrRow, reason, caseNum)
                                                if mpSync[0] != i:
                                                    self.bestMatches[i] = 'Taken'
                                                    self.bestMatchReasons[i] = 'Thor:1 CWR:>1 Taken'
                                                    self.caseNumbers[i] = 5
                                            # More than one MP Synced
                                            else:
                                                # find closest
                                                reason = 'Thor=1 CWR>1 | RP=0 MP>1, Distance'
                                                caseNum = 6
                                                closest = findClosestList(cwrRow, self.thorList, mpSync)
                                                self.addAndRemove(closest.geti(), cwrRow, reason, caseNum)
                                                if closest.geti() != i:
                                                    self.bestMatches[i] = 'Taken'
                                                    self.bestMatchReasons[i] = 'Thor:1 CWR:>1 Taken'
                                                    self.caseNumbers[i] = 7
                                        elif len(rpSync) == 1:
                                            reason = 'Thor=1 CWR>1 | RP=1'
                                            caseNum = 8
                                            self.addAndRemove(rpSync[0], cwrRow, reason, caseNum)
                                            if rpSync[0] != i:
                                                self.bestMatches[i] = 'Taken'
                                                self.bestMatchReasons[i] = 'Thor=1 CWR>1 Taken'
                                                self.caseNumbers[i] = 9
                                        # more than one Rail Pos Synced
                                        else:
                                            # Check to see if they are within the inner mile post range:
                                            mpSync = mpSyncCheck(cwrRow, self.thorList)
                                            # if no thors sync with cwr
                                            if len(mpSync) == 0:
                                                reason = 'Thor=1 CWR>1 | RP>1 MP=0, Distance'
                                                caseNum = 10
                                                closest = findClosestList(cwrRow, self.thorList, rpSync)
                                                self.addAndRemove(closest.geti(), cwrRow, reason, caseNum)
                                                if closest.geti() != i:
                                                    self.bestMatches[i] = 'Taken'
                                                    self.bestMatchReasons[i] = 'Thor=1 CWR>1 Taken'
                                                    self.caseNumbers[i] = 11
                                            elif len(mpSync) == 1:
                                                reason = 'Thor=1 CWR>1 | RP>1 MP=1'
                                                caseNum = 12
                                                self.addAndRemove(mpSync[0], cwrRow, reason, caseNum)
                                                if mpSync[0] != i:
                                                    self.bestMatches[i] = 'Taken'
                                                    self.bestMatchReasons[i] = 'Thor=1 CWR>1 Taken'
                                                    self.caseNumbers[i] = 13
                                            # More than one MP Synced
                                            else:
                                                reason = 'Thor=1 CWR>1 | RP>1 MP>1, Distance'
                                                caseNum = 14
                                                closest = findClosestList(cwrRow, self.thorList, mpSync)
                                                self.addAndRemove(closest.geti(), cwrRow, reason, caseNum)
                                                if closest.geti() != i:
                                                    self.bestMatches[i] = 'Taken'
                                                    self.bestMatchReasons[i] = 'Thor=1 CWR>1 | Taken'
                                                    self.caseNumbers[i] = 15
                                    # One Match == False
                                    else:
                                        self.bestMatches[i] = 'DNF'
                                        self.bestMatchReasons[i] = 'Thor=? CWR:>1 THOR match counts vary '
                                        self.caseNumbers[i] = 16
                                # CWR Row has no matches
                                else:
                                    self.bestMatches[i] = 'Taken'
                                    self.bestMatchReasons[i] = 'Thor=1 CWR:0'
                                    self.caseNumbers[i] = 17
                            # cwrRow not a thorRow Match
                            else:
                                pass
                        # for loop
                    # THOR has 2 matches
                    elif len(thorRow.getMatchIndexes()) == 2:
                        cwrOne = self.cwrList[thorRow.getMatchIndexes()[0]]
                        cwrTwo = self.cwrList[thorRow.getMatchIndexes()[1]]
                        # CWRs have same match lists
                        if cwrOne.getMatchIndexes().sort() == cwrTwo.getMatchIndexes().sort():
                            # CWRs have no matches
                            if len(cwrOne.getMatchIndexes()) == 0:
                                self.bestMatches[i] = 'Taken'
                                self.bestMatchReasons[i] = 'THOR:2, CWR:0'
                                self.caseNumbers[i] = 18
                            # both CWRs match to one Thor
                            elif len(cwrOne.getMatchIndexes()) == 1:
                                # match best CWR to their match, error if not == i
                                reason = 'THOR:2, CWR:1 | Found Best'
                                caseNum = 19
                                bestCWR = findPersonalBest(thorRow, self.cwrList)
                                self.addAndRemove(thorRow.geti(), bestCWR, reason, caseNum)
                                if thorRow.geti() != i:
                                    self.bestMatchReasons[i] = 'Manual Input - Taken'
                                    self.caseNumbers[i] = 20
                            # 2 Thors 2 CWRs
                            elif len(cwrOne.getMatchIndexes()) == 2:
                                thorOne = self.thorList[cwrOne.getMatchIndexes()[0]]
                                thorTwo = self.thorList[cwrOne.getMatchIndexes()[1]]

                                cwrRailSync = checkRP(cwrOne, cwrTwo)
                                thorRailSync = checkRP(thorOne, thorTwo)

                                if cwrRailSync == True:
                                    if thorRailSync == True:
                                        # both same RP
                                        if thorOne.getRP() == cwrOne.getRP():
                                            # if thorOne is closer to cwrOne than thorTwo is
                                            if objectOneCloser(cwrOne, thorOne, thorTwo):
                                                # if thorOne is closer to both CWRs than thorTwo
                                                if objectOneCloser(cwrTwo, thorOne, thorTwo):
                                                    # if thorTwo is closer to cwrOne than it is to cwrTwo
                                                    if objectOneCloser(thorTwo, cwrOne, cwrTwo):
                                                        reason = 'THOR:2, CWR:2 | RP=, Respective Position'
                                                        caseNum = 21
                                                        self.addAndRemove(thorOne.geti(), cwrTwo, reason, caseNum)
                                                        reason = 'THOR:2, CWR:2 | RP=, Respective Position'
                                                        caseNum = 22
                                                        self.addAndRemove(thorTwo.geti(), cwrOne, reason, caseNum)
                                                    # if thorTwo is closer to cwrOne than it is to cwrTwo
                                                    else:
                                                        reason = 'THOR:2, CWR:2 | RP=, Respective Position'
                                                        caseNum = 23
                                                        self.addAndRemove(thorOne.geti(), cwrOne, reason, caseNum)
                                                        reason = 'THOR:2, CWR:2 | RP=, Respective Position'
                                                        caseNum = 24
                                                        self.addAndRemove(thorTwo.geti(), cwrTwo, reason, caseNum)
                                                # One - One | Two-Two
                                                else:
                                                    reason = 'THOR:2, CWR:2 | RP=, Respective Position'
                                                    caseNum = 25
                                                    self.addAndRemove(thorOne.geti(), cwrOne, reason, caseNum)
                                                    reason = 'THOR:2, CWR:2 | RP=, Respective Position'
                                                    caseNum = 26
                                                    self.addAndRemove(thorTwo.geti(), cwrTwo, reason, caseNum)
                                            # If thorTwo closer to cwrOne
                                            else:
                                                # if thorTwo is closer to both
                                                if objectOneCloser(cwrTwo, thorTwo, thorOne):
                                                    # if One is closer to cwrOne than it is to cwrTwo
                                                    if objectOneCloser(thorOne, cwrOne, cwrTwo):
                                                        reason = 'THOR:2, CWR:2 | RP=, Respective Position'
                                                        caseNum = 27
                                                        self.addAndRemove(thorOne.geti(), cwrOne, reason, caseNum)
                                                        reason = 'THOR:2, CWR:2 | RP=, Respective Position'
                                                        caseNum = 28
                                                        self.addAndRemove(thorTwo.geti(), cwrTwo, reason, caseNum)
                                                    # if One is closer to cwrTwo than it is to cwrOne
                                                    else:
                                                        reason = 'THOR:2, CWR:2 | RP=, Respective Position'
                                                        caseNum = 29
                                                        self.addAndRemove(thorOne.geti(), cwrTwo, reason, caseNum)
                                                        reason = 'THOR:2, CWR:2 | RP=, Respective Position'
                                                        caseNum = 30
                                                        self.addAndRemove(thorTwo.geti(), cwrOne, reason, caseNum)
                                                # two-one one-two
                                                else:
                                                    reason = 'THOR:2, CWR:2 | RP=, Respective Position'
                                                    caseNum = 31
                                                    self.addAndRemove(thorOne.geti(), cwrTwo, reason, caseNum)
                                                    reason = 'THOR:2, CWR:2 | RP=, Respective Position'
                                                    caseNum = 32
                                                    self.addAndRemove(thorTwo.geti(), cwrOne, reason, caseNum)
                                        # both diff RP
                                        else:
                                            # if thorOne is closer to cwrOne than thorTwo is
                                            if objectOneCloser(cwrOne, thorOne, thorTwo):
                                                # if thorOne is closer to both CWRs than thorTwo
                                                if objectOneCloser(cwrTwo, thorOne, thorTwo):
                                                    # if thorTwo is closer to cwrOne than it is to cwrTwo
                                                    if objectOneCloser(thorTwo, cwrOne, cwrTwo):
                                                        reason = 'THOR:2, CWR:2 | RP!=, Respective Position'
                                                        caseNum = 33
                                                        self.addAndRemove(thorOne.geti(), cwrTwo,reason, caseNum)
                                                        reason = 'THOR:2, CWR:2 | RP!=, Respective Position'
                                                        caseNum = 34
                                                        self.addAndRemove(thorTwo.geti(), cwrOne,reason, caseNum)
                                                    # if thorTwo is closer to cwrOne than it is to cwrTwo
                                                    else:
                                                        reason = 'THOR:2, CWR:2 | RP!=, Respective Position'
                                                        caseNum = 35
                                                        self.addAndRemove(thorOne.geti(), cwrOne, reason, caseNum)
                                                        reason = 'THOR:2, CWR:2 | RP!=, Respective Position'
                                                        caseNum = 36
                                                        self.addAndRemove(thorTwo.geti(), cwrTwo, reason, caseNum)
                                                # One - One | Two-Two
                                                else:
                                                    reason = 'THOR:2, CWR:2 | RP!=, Respective Position'
                                                    caseNum = 37
                                                    self.addAndRemove(thorOne.geti(), cwrOne, reason, caseNum)
                                                    reason = 'THOR:2, CWR:2 | RP!=, Respective Position'
                                                    caseNum = 38
                                                    self.addAndRemove(thorTwo.geti(), cwrTwo, reason, caseNum)
                                            # If thorTwo closer to cwrOne
                                            else:
                                                # if thorTwo is closer to both
                                                if objectOneCloser(cwrTwo, thorTwo, thorOne):
                                                    # if One is closer to cwrOne than it is to cwrTwo
                                                    if objectOneCloser(thorOne, cwrOne, cwrTwo):
                                                        reason = 'THOR:2, CWR:2 | RP!=, Respective Position'
                                                        caseNum = 39
                                                        self.addAndRemove(thorOne.geti(), cwrOne, reason, caseNum)
                                                        reason = 'THOR:2, CWR:2 | RP!=, Respective Position'
                                                        caseNum = 40
                                                        self.addAndRemove(thorTwo.geti(), cwrTwo, reason, caseNum)
                                                    # if One is closer to cwrTwo than it is to cwrOne
                                                    else:
                                                        reason = 'THOR:2, CWR:2 | RP!=, Respective Position'
                                                        caseNum = 41
                                                        self.addAndRemove(thorOne.geti(), cwrTwo, reason, caseNum)
                                                        reason = 'THOR:2, CWR:2 | RP!=, Respective Position'
                                                        caseNum = 42
                                                        self.addAndRemove(thorTwo.geti(), cwrOne, reason, caseNum)
                                                # two-one one-two
                                                else:
                                                    reason = 'THOR:2, CWR:2 | RP!=, Respective Position'
                                                    caseNum = 43
                                                    self.addAndRemove(thorOne.geti(), cwrTwo, reason, caseNum)
                                                    reason = 'THOR:2, CWR:2 | RP!=, Respective Position'
                                                    caseNum = 44
                                                    self.addAndRemove(thorTwo.geti(), cwrOne, reason, caseNum)
                                    # cwr railSync True | thor railSync False
                                    else:
                                        # Find RP match -> find closest, match non RP match to leftover
                                        if checkRP(thorOne, cwrOne):
                                            inSync = thorOne
                                            outSync = thorTwo
                                        else:
                                            inSync = thorOne
                                            outSync = thorTwo

                                        inSyncMatch = findClosest(inSync, cwrOne, cwrTwo)
                                        if inSyncMatch == cwrOne:
                                            outSyncMatch = cwrTwo
                                        else:
                                            outSyncMatch = cwrOne

                                        reason = 'THOR:2, CWR:2 | solo RP Match'
                                        caseNum = 45
                                        self.addAndRemove(inSync.geti(), inSyncMatch, reason, caseNum)
                                        reason = 'THOR:2, CWR:2 | solo RP no Match'
                                        caseNum = 46
                                        self.addAndRemove(outSync.geti(), outSyncMatch, reason, caseNum)
                                # cwr rail sync false
                                else:
                                    # CWR no raiSlSync but Thor railSync
                                    if thorRailSync == True:
                                        # Find RP match -> find closest, match non RP match to leftover
                                        if checkRP(cwrOne, thorOne):
                                            inSync = cwrOne
                                            outSync = cwrTwo
                                        else:
                                            inSync = cwrOne
                                            outSync = cwrTwo

                                        inSyncMatch = findClosest(inSync, thorOne, thorTwo)
                                        if inSyncMatch == thorOne:
                                            outSyncMatch = thorTwo
                                        else:
                                            outSyncMatch = thorOne

                                        reason = 'THOR:2, CWR:2 | solo RP Match'
                                        caseNum = 47
                                        self.addAndRemove(inSyncMatch.geti(), inSync, reason, caseNum)
                                        reason = 'THOR:2, CWR:2 | solo RP no Match'
                                        caseNum = 48
                                        self.addAndRemove(outSyncMatch.geti(), outSync, reason, caseNum)

                                        # Find RP match -> find closest, match non RP match to leftover

                                    #Neither have rail Sync
                                    else:
                                        if thorOne.getRP() == cwrOne.getRP():
                                            reason = 'THOR:2, CWR:2 | Respective RP'
                                            caseNum = 49
                                            self.addAndRemove(thorOne.geti(), cwrOne, reason, caseNum)
                                            reason = 'THOR:2, CWR:2 | Respective RP'
                                            caseNum = 50
                                            self.addAndRemove(thorTwo.geti(), cwrTwo, reason, caseNum)
                                        else:
                                            reason = 'THOR:2, CWR:2 | Respective RP'
                                            caseNum = 51
                                            self.addAndRemove(thorOne.geti(), cwrTwo, reason, caseNum)
                                            reason = 'THOR:2, CWR:2 | Respective RP'
                                            caseNum = 52
                                            self.addAndRemove(thorTwo.geti(), cwrOne, reason, caseNum)

                            # Thor had 2 matches CWRs had more than 2 matches
                            else:
                                cwrOne = self.cwrList[thorRow.getMatchIndexes()[0]]
                                cwrTwo = self.cwrList[thorRow.getMatchIndexes()[1]]

                                cOneBest = findPersonalBest(cwrOne, self.thorList)
                                cTwoBest = findPersonalBest(cwrTwo, self.thorList)

                                if cOneBest == cTwoBest:
                                    if checkRP(cwrOne, cwrTwo):
                                        if objectOneCloser(cOneBest, cwrOne, cwrTwo):
                                            reason = 'THOR=2 CWR>2 | RP=, Distance W'
                                            caseNum = 53
                                            self.addAndRemove(cOneBest.geti(), cwrOne, reason, caseNum)
                                            reason = 'THOR=2 CWR>2 | RP=, Distance L'
                                            caseNum = 54
                                            cTwoBest = findPersonalBest(cwrTwo, self.thorList)
                                            self.addAndRemove(cTwoBest.geti(), cwrTwo, reason, caseNum)
                                        else:
                                            reason = 'THOR=2 CWR>2 | RP=, Distance W'
                                            caseNum = 55
                                            self.addAndRemove(cTwoBest.geti(), cwrTwo, reason, caseNum)
                                            reason = 'THOR=2 CWR>2 | RP=, Distance L'
                                            caseNum = 56
                                            cOneBest = findPersonalBest(cwrOne, self.thorList)
                                            self.addAndRemove(cOneBest.geti(), cwrOne, reason, caseNum)
                                    else:
                                        if checkRP(cOneBest, cwrOne):
                                            reason = 'THOR=2 CWR>2 | RP'
                                            caseNum = 57
                                            self.addAndRemove(cOneBest.geti(), cwrOne, reason, caseNum)
                                            reason = 'THOR=2 CWR>2 | !RP'
                                            caseNum = 58
                                            cTwoBest = findPersonalBest(cwrTwo, self.thorList)
                                            self.addAndRemove(cTwoBest.geti(), cwrTwo, reason, caseNum)
                                        else:
                                            reason = 'THOR=2 CWR>2 | RP'
                                            caseNum = 59
                                            self.addAndRemove(cTwoBest.geti(), cTwoBest, reason, caseNum)
                                            reason = 'THOR=2 CWR>=2 | !RP'
                                            caseNum = 60
                                            cTwoBest = findPersonalBest(cwrTwo, thorList)
                                            self.addAndRemove(cOneBest.geti(), cwrOne, reason, caseNum)
                                else:
                                    reason = 'THOR=2 CWR>2 | Found Best'
                                    caseNum = 61
                                    self.addAndRemove(cOneBest.geti(), cwrOne, reason, caseNum)
                                    reason = 'THOR=2 CWR>2 | Found Best'
                                    caseNum = 62
                                    self.addAndRemove(cTwoBest.geti(), cwrTwo, reason, caseNum)


                        else:
                            if len(cwrOne.getMatchIndexes()) == 0:
                                cTwoBest = findPersonalBest(cwrTwo, self.thorList)
                                reason = 'THOR=2 CWR=? | Found Best'
                                caseNum = 63
                                self.addAndRemove(cTwoBest.geti(), cwrTwo, reason, caseNum)
                            elif len(cwrTwo.getMatchIndexes()) == 0:
                                cOneBest = findPersonalBest(cwrTwo, self.thorList)
                                reason = 'THOR=2 CWR=? | Found Best'
                                caseNum = 64
                                self.addAndRemove(cOneBest.geti(), cwrOne, reason, caseNum)

                            else:
                                cwrOne = self.cwrList[thorRow.getMatchIndexes()[0]]
                                cwrTwo = self.cwrList[thorRow.getMatchIndexes()[1]]

                                cOneBest = findPersonalBest(cwrOne, thorList)
                                cTwoBest = findPersonalBest(cwrTwo, thorList)

                                if cOneBest == cTwoBest:
                                    if cwrOne.getRP() == cwrTwo.getRP():
                                        if objectOneCloser(cOneBest, cwrOne, cwrTwo):
                                            reason = 'THOR=2 CWR=? | RP=, Distance W'
                                            caseNum = 65
                                            self.addAndRemove(cOneBest.geti(), cwrOne, reason, caseNum)
                                            reason = 'THOR=2 CWR=? | RP=, Distance L'
                                            caseNum = 66
                                            cTwoBest = findPersonalBest(cwrTwo, thorList)
                                            self.addAndRemove(cTwoBest.geti(), cwrTwo, reason, caseNum)
                                        else:
                                            reason = 'THOR=2 CWR=? | RP=, Distance W'
                                            caseNum = 67
                                            self.addAndRemove(cTwoBest.geti(), cwrTwo, reason, caseNum)
                                            reason = 'THOR=2 CWR=? | RP=, Distance L'
                                            caseNum = 68
                                            cOneBest = findPersonalBest(cwrOne, thorList)
                                            self.addAndRemove(cOneBest.geti(), cwrOne, reason, caseNum)
                                    else:
                                        if checkRP(cOneBest, cwrOne):
                                            reason = 'THOR=2 CWR=? | RP'
                                            caseNum = 69
                                            self.addAndRemove(cOneBest.geti(), cwrOne, reason, caseNum)
                                            reason = 'THOR=2 CWR=? | !RP'
                                            caseNum = 70
                                            cTwoBest = findPersonalBest(cwrTwo, thorList)
                                            self.addAndRemove(cTwoBest.geti(), cwrTwo, reason, caseNum)
                                        else:
                                            reason = 'THOR=2 CWR=? | RP'
                                            caseNum = 71
                                            self.addAndRemove(cTwoBest.geti(), cTwoBest, reason, caseNum)
                                            reason = 'THOR=2 CWR=? | !RP'
                                            caseNum = 72
                                            cTwoBest = findPersonalBest(cwrTwo, thorList)
                                            self.addAndRemove(cOneBest.geti(), cwrOne, reason, caseNum)

                                    #else above
                                else:
                                    reason = 'THOR=2 CWR=? | Found Best'
                                    caseNum = 73
                                    self.addAndRemove(cOneBest.geti(), cwrOne, reason, caseNum)
                                    reason = 'THOR=2 CWR=? | Found Best'
                                    caseNum = 74
                                    self.addAndRemove(cTwoBest.geti(), cwrTwo, reason, caseNum)

                    # THOR had >2 Matches, all it's CWRs only had one match
                    else:
                        oneMatch = oneMatchCheck(thorRow, self.cwrList)
                        if oneMatch == True:
                            reason = 'Thor>2 CWR=1 | Found Best'
                            caseNum = 75
                            self.addAndRemove(thorRow.geti(), findPersonalBest(thorRow, self.cwrList), reason, caseNum)

                            # else above
                        else:
                            self.bestMatches[i] = 'DNF'
                            self.bestMatchReasons[i] = 'Thor>2 CWR>1 | No Logic'
                            self.caseNumbers[i] = 76
                    # else above
                # Best Match Found, Taken, or N/A
                else:
                    pass
            # for loop
        print('Dynamic Matching End')
    # Simpler Matching Methodology - Less Accurate
    def simpleMatching(self):
        print('Simple Matching Start')
        # arguably should be used for entire program, less accurate but less error prone
        for runNum in range(1000):
            acceptable = loose/(1000-runNum)
            # Rail Sync
            for i in range(len(self.thorList)):
                if self.bestMatches[i] == 'DNF':
                    thorRow = self.thorList[i]
                    if len(thorRow.getMatchIndexes()) == 0:
                        self.bestMatches[i] = 'Taken'
                        self.exceptions[i] = 'Needs Check'
                        self.bestMatchReasons[i] = 'Simple | Rail Sync | run:' + str(runNum)
                        self.caseNumbers[i] = -2
                    for matchIndex in thorRow.getMatchIndexes():
                        cwrRow = self.cwrList[matchIndex]
                        syncList = []
                        if checkRP(thorRow, cwrRow):
                            syncList.append(cwrRow.geti())
                        if len(syncList) > 0:
                            closest = findClosestList(thorRow, self.cwrList, syncList)
                            if abs(thorRow.getMP() - closest.getMP()) <= acceptable:
                                reason = 'Simple | Rail Sync, Distance | run:' + str(runNum)
                                caseNum = 102
                                self.addAndRemove(thorRow.geti(), closest, reason, caseNum)
                                break
        # Rail not Synced
        for runNum in range(1000):
            acceptable = loose/(1000-runNum)

            for i in range(len(self.thorList)):
                if self.bestMatches[i] == 'DNF':
                    thorRow = self.thorList[i]
                    if len(thorRow.getMatchIndexes()) == 0:
                        self.bestMatches[i] = 'Taken'
                        self.exceptions[i] = 'Needs Check'
                        self.bestMatchReasons[i] = 'Simple | Rail !Synced | run:' + str(runNum)
                        self.caseNumbers[i] = -3
                    else:
                        closest = findClosestList(thorRow, self.cwrList, thorRow.getMatchIndexes())
                        if abs(thorRow.getMP() - closest.getMP()) <= acceptable:
                            reason = 'Simple | Rail !Synced, Distance'
                            caseNum = 103
                            self.addAndRemove(thorRow.geti(), closest, reason, caseNum)
        print('Simple Matching End')
        print()
    # Finalizes lists that will be turned into dataframe columns.
    def columnFinalization(self):
        for i in range(len(self.bestMatches)):

            if self.bestMatches[i] in ['-', 'Taken']:
                self.exceptions[i] = 'Missing'
                self.cwrMPs[i] = '-'
                self.cwrRPs[i] = '-'
            elif self.bestMatches[i] == '':
                if self.whyList[i] != 'No Matches':
                    self.bestMatches[i] = 'Taken'
                    self.exceptions[i] = 'Manual Input'
                    self.bestMatchReasons[i] = 'Program Error'
                    self.caseNumbers[i] = -1
                    self.cwrMPs[i] = '-'
                    self.cwrRPs[i] = '-'
                else:
                    self.exceptions[i] = 'Missing'
                    self.cwrMPs[i] = '-'
                    self.cwrRPs[i] = '-'

            elif self.bestMatches[i] == 'DNF':
                self.exceptions[i] = 'Manual Input'
                self.bestMatchReasons[i] = 'Simple Matching Error'
                self.cwrMPs[i] = 'DNF'
                self.cwrRPs[i] = 'DNF'

            else:
                self.cwrMPs[i] = self.bestMatches[i].getMP()
                self.cwrRPs[i] = self.bestMatches[i].getRP()
                self.bestMatches[i] = self.bestMatches[i].getID()

                self.exceptions[i] = createException(i, self.thorList, self.cwrMPs[i], self.cwrRPs[i])




########################
# Best Match Functions #
########################

def checkRP(original, obj):
    return original.getRP() == obj.getRP()
# Check if in tight mile post distance
def checkMP(original, obj):
    return tight >= (original.getMP() - obj.getMP())
# Find closest to original from two different Mile Posts
def findClosest(original, obj1, obj2):
    if abs(original.getMP() - obj1.getMP()) <= abs(original.getMP() - obj2.getMP()):
        return obj1
    else:
        return obj2
# Find closest MP within a list
def findClosestList(original, objList, indexList):
    # listRange = range(len(indexList))
    closest = objList[indexList[0]]
    for i in indexList:
        closest = findClosest(original, closest, objList[i])
    return closest
# T/F is obj 1 closer to orig than obj2
def objectOneCloser(orig, obj1, obj2):
    return (getDistance(orig, obj1) > getDistance(orig, obj2))
# gets MP distance between orig and obj1
def getDistance(orig, obj):
    return abs(orig.getMP() - obj.getMP())
# Find best match within one Object's matchList
def findPersonalBest(orig, objList):
    railSync = []
    for objIndex in orig.getMatchIndexes():
        if checkRP(orig, objList[objIndex]):
            railSync.append(objIndex)
    if len(railSync) == 0:
        mpSync = []
        for objIndex in orig.getMatchIndexes():
            if checkRP(orig, objList[objIndex]):
                mpSync.append(objIndex)
        if len(mpSync) == 0:
            return findClosestList(orig, objList, orig.getMatchIndexes())
        elif len(mpSync) == 1:
            return objList[mpSync[0]]
        else:
            return findClosestList(orig, objList, mpSync)
    elif len(railSync) == 1:
        return objList[railSync[0]]
    else:
        mpSync = []
        for objIndex in orig.getMatchIndexes():
            if checkRP(orig, objList[objIndex]):
                mpSync.append(objIndex)
        if len(mpSync) == 0:
            return findClosestList(orig, objList, railSync)
        elif len(mpSync) == 1:
            return objList[mpSync[0]]
        else:
            return findClosestList(orig, objList, mpSync)
def mpSyncCheck(orig, objList):
    mpSync = []
    for index in orig.getMatchIndexes():
        if checkMP(orig, objList[index]):
            mpSync.append(index)
    return mpSync

def rpSyncCheck(orig, objList):
    rpSync = []
    for index in orig.getMatchIndexes():
        if checkRP(orig, objList[index]):
            rpSync.append(index)
    return rpSync
# If all of "orig's matches only have one match return True"
def oneMatchCheck(orig, objList):
    oneMatch = True
    for index in orig.getMatchIndexes():
        if len(objList[index].getMatchIndexes()) != 1:
            oneMatch = False
    return oneMatch

# Add and Remove Functionality
def removeMatch(self, input):
    self.matchIndexes.remove(input)

########################
# Best Match Functions #
########################


# Exception Column Creator
def createException(i, thorList, cwrMP, cwrRP):
    mpSync = False
    railSync = False

    if abs(cwrMP - thorList[i].getMP()) <= tight:
        mpSync = True
    if cwrRP == thorList[i].getRP():
        railSync = True

    if mpSync == True:
        if railSync == True:
            return 'No Exception'
        else:
            return 'Rail Out of Sync'
    else:
        if railSync == True:
            return 'MP Out of Sync'
        else:
            return 'MP and Rail Out of Sync'



# PROGRAM REALLY RUNS HERE
# Init
columns = Columns(dfThor, dfCwr)
# Potential Matches - matchList and whyList
columns.potentialMatches()
# Dynamic Matching - Best Match Part 1
columns.dynamicMatching()
# Simple Matching - Best Match Part 2
columns.simpleMatching()
# Build Columns
columns.columnFinalization()



# Rename, Add, and Drop Columns
newNames = {'aset_rid':'THOR ID', 'gmtry_car_nbr':'GeoCar', 'aset_loctn_lin_seg_nbr':'Line Segment',\
'aset_loctn_trak_nbr':'Track Number','geaometry_rail_posn_nme':'THOR Rail',\
'aset_loctn_lattd':'THOR LAT', 'aset_loctn_lngtd':'THOR LONG', 'aset_loctn_mp_nbr':'THOR MP',\
'aset_loctn_trak_typ_cd':'Track Type', }
dfThor.rename(columns = newNames, inplace=True)

dfThor['Potential Matches'] = columns.getMatches()
dfThor['Match Details'] = columns.getWhy()
dfThor['CWR ID'] = columns.getBestMatches()
dfThor['BM Logic'] = columns.getBestMatchReasons()
dfThor['Case Number'] = columns.getCaseNumbers()

dfThor['CWR MP'] = columns.getCwrMPs()
dfThor['CWR Rail'] = columns.getCwrRPs()
dfThor['Exception'] = columns.getExceptions()

dfThor['THOR Rail'] = dfThor['THOR Rail'].str.upper()


# dfThor = dfThor[dfThor['Case Number'] != '-'].reset_index(drop = True)
columnOrder = ['GeoCar', 'THOR ID', 'CWR ID', 'Line Segment', 'Track Type',\
'Track Number', 'THOR MP', 'THOR Rail', 'CWR MP', 'CWR Rail', 'Exception',\
'THOR LAT', 'THOR LONG', 'BM Logic', 'Case Number', 'Potential Matches', 'Match Details']
dfThor = dfThor[columnOrder]
print('Columns Added/Dropped')

# Save File
dfThor.to_csv(directory + '\\' + fileName + '.csv', index = False)
print('File Saved')
print('End.')
