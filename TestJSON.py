import json
import pandas as pd

with open("sample.JSON") as file:
    data = json.load(file)

#print(data["teams"])

#json.dumps(data["teams"], indent = 4)
#print(json.dumps(data["participantIdentities"][0]["player"], indent = 4))
partData = data["participantIdentities"]
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
print(dtotal.columns)


