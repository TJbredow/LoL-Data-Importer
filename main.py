import requests
import json
import psycopg2
import psycopg2.extras
import pandas as pd
from configparser import ConfigParser
import io
import time

api_key = "RGAPI-c23393b7-328f-4c2e-b76d-bf22a0cfce0a"
summoner1 = "sabbosa"
summoner = summoner1.lower()

def config(filename='database.ini', section='postgresql'):
    #Create Parser var
    parser = ConfigParser()
    #Read Configuration
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]

    else:
        raise Exception ('Section {} not found in the {} file'.format(section, filename))
    return db
def testConnection():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	# close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
def connect():            
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn
def exportToServer(tablename, dataset): #Name of Table you're writing to , and the data var. The dataset is from the pd.DataFrame class
    conn = connect()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            output = io.StringIO()
            dataset.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            contents = output.getvalue()
            cur.copy_from(output, tablename.lower(), null="")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    conn.commit()
    conn.close()
def listColumns():
    colSelect = (
        """
        SELECT
            column_name
        FROM
            information_schema.columns
        WHERE
            table_name = '{}';
        """.format(summoner)
    )
    print(colSelect)
    conn = connect()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(colSelect)
            tablecolumns = cur.fetchall()
            print(tablecolumns)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    conn.commit()
    conn.close()
    return tablecolumns
def removeDuplicateMatches(uniqueColumn):
    tablecolumns = listColumns()
    print(type(tablecolumns))
    insertcommand = "INSERT INTO {}_tmp(".format(summoner)
    print(tablecolumns[1])
    for x in tablecolumns:
        if tablecolumns.index(x) == 0:
            print(x[0])
            insertcommand += x[0]
        else:
            insertcommand += ", "
            print(x[0])
            insertcommand += x[0]

    insertcommand += ") SELECT DISTINCT ON ({}) ".format(uniqueColumn)
    for x in tablecolumns:
        if tablecolumns.index(x) == 0:
            insertcommand += x[0]
        else:
            insertcommand += ", "
            insertcommand += x[0]
    insertcommand += " FROM {};".format(summoner)
    print(insertcommand)
    conn = connect()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute("CREATE TABLE {}_tmp (LIKE {});".format(summoner,summoner))
            print("success - Table Created")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    conn.commit()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(insertcommand)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    conn.commit()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute("DROP TABLE {};".format(summoner))
            cur.execute("ALTER TABLE {}_tmp RENAME TO {}".format(summoner,summoner))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    conn.commit()
    conn.close()    
def tableBuilder(tablename, columns):
    listobj = list(columns)
    conn = connect()
    print(conn)
    command = "CREATE TABLE {} (".format(tablename)
    for x in listobj:
        if listobj.index(x) == 0:
            command += x
            command += " VARCHAR "
        else:
            command += ", "
            command += x
            command += " VARCHAR "
    command+= ");"
    print (command)
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(command)
            print("test")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    conn.commit()
    conn.close()
def jprint(obj):
    text = json.dumps(obj, sort_keys=True, indent=4)
    print (text)
def getaccountId(obj):
    data = obj.get("accountId")
    #print(data)
    return data
def jsontoTable(obj):
    #jprint(response.json())
    data = obj["matches"]
    #print(data)
    #matches = json.loads(data)
    df = pd.DataFrame(data)
    return(df)

def importMatchData():
    gameSelect = ("SELECT gameid FROM {}".format(summoner))
    print(gameSelect)
    conn = connect()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(gameSelect)
            gameids = cur.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    conn.commit()
    conn.close()   
    print(gameids[0][0])

    for x in gameids:
        matchDataFormat(x[0])


def matchDataFormat(gameid):
    response = requests.get("https://na1.api.riotgames.com/lol/match/v4/matches/{}?api_key={}".format(gameid, api_key))
    data = response.json()
    partData = data["participantIdentities"]
    #Pull in initial set
    for x in partData:
        if partData.index(x) == 0:
            usethis = x["player"]
            df = pd.DataFrame(usethis, index=[0])
            dtotal = df.filter(["summonerName", "platformId"], axis = 1)
        else: 
            usethis = x["player"]
            df = pd.DataFrame(usethis, index=[0])
            dr = df.filter(["summonerName", "platformId"], axis = 1)
            dtotal = dtotal.append(dr)
    partData2 = data["participants"]
    print(dtotal)
    #Pull in data from another part of the JSON file
    for x in partData2:
        if partData2.index(x) == 0:
            df = pd.DataFrame(x, index=[0])
            dtotal1 = df.filter(["participantId"], axis = 1)
        else: 
            df = pd.DataFrame(x, index=[0])
            dr = df.filter(["participantId"], axis = 1)
            dtotal1 = dtotal1.append(dr)
    participantId = list(dtotal1["participantId"])
    print(participantId)
    dtotal.insert(0, "participantId", participantId)
    print(dtotal)
    #Pull in data from another part of the JSON file
    for x in partData2:
        if partData2.index(x) == 0:
            df = pd.DataFrame(x, index=[0])
            dtotal1 = df.filter(["teamId", "championId"], axis = 1)
        else: 
            df = pd.DataFrame(x, index=[0])
            dr = df.filter(["teamId", "championId"], axis = 1)
            dtotal1 = dtotal1.append(dr)
    print(dtotal1)
    teamId = list(dtotal1["teamId"])
    dtotal.insert(3, "teamId", teamId)
    championId = list(dtotal1["championId"])
    dtotal.insert(4, "championId", championId)
    print(dtotal)
    listset = ["win", "kills","deaths", "assists", "goldEarned", "totalDamageDealtToChampions"]
    for x in partData2:
        if partData2.index(x) == 0:
            df = pd.DataFrame(x["stats"], index=[0])
            dtotal1 = df.filter(listset, axis = 1)
        else:
            df = pd.DataFrame(x["stats"], index=[0])
            dr = df.filter(listset, axis = 1)
            dtotal1 = dtotal1.append(dr)
    for x in listset:
        xId = list(dtotal1[x])
        columnIndex = listset.index(x) + 5
        print(columnIndex)
        dtotal.insert(columnIndex, x, xId)
    print(dtotal)

    listset1 = ["role", "lane"]
    for x in partData2:
        if partData2.index(x) == 0:
            df = pd.DataFrame(x["timeline"], index=[0])
            dtotal1 = df.filter(listset1, axis = 1)
        else:
            df = pd.DataFrame(x["timeline"], index=[0])
            dr = df.filter(listset1, axis = 1)
            dtotal1 = dtotal1.append(dr)
    print(dtotal1)
    for x in listset1:
        xId = list(dtotal1[x])
        columnIndex = listset1.index(x) + 5
        print(columnIndex)
        dtotal.insert(columnIndex, x, xId)
    #export the dtotal data buffer
    print(dtotal)
    tableBuilder("{}_{}".format(summoner, gameid), dtotal.columns)
    exportToServer("{}_{}".format(summoner, gameid), dtotal)
    #sleep is necessary because of the limited API calls you get without approval (100/min or 20/second)
    time.sleep(.5)


#testConnection()

response = requests.get("https://na1.api.riotgames.com/lol/summoner/v4/summoners/by-name/{}?api_key={}".format(summoner, api_key))
print(response.status_code)
jprint(response.json())
accountId = getaccountId(response.json())

response = requests.get("https://na1.api.riotgames.com/lol/match/v4/matchlists/by-account/{}?api_key={}".format(accountId, api_key))
dataTable = jsontoTable(response.json())
dtColumns = jsontoTable(response.json()).columns
#print(dataTable)
#tableBuilder(summoner, dfColumns)
#exportToServer(dataTable)
#removeDuplicateMatches("gameid")
importMatchData()
