import sqlite3
import csv
import json
import sys

# proj3_choc.py
# Name: Di Jin
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

def init_db():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)
    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)
    
    conn.commit()
    
    statement = '''
        CREATE TABLE 'Bars' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Company' TEXT NOT NULL,
                'SpecificBeanBarName' TEXT NOT NULL,
                'REF' TEXT,
                'ReviewDate' TEXT,
                'CocoaPercent' REAL,
                'CompanyLocationId' INTEGER,
                'Rating' REAL,
                'BeanType' TEXT,
                'BroadBeanOriginId' INTEGER,
                 FOREIGN KEY ('BroadBeanOriginId')
                 REFERENCES 'Countries' ('Id')
                 ON UPDATE CASCADE ON DELETE CASCADE,
                 FOREIGN KEY ('CompanyLocationId')
                 REFERENCES 'Countries' ('Id')
                 ON UPDATE CASCADE ON DELETE CASCADE
        );
    '''
    cur.execute(statement)
    statement = '''
        CREATE TABLE 'Countries' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Alpha2' TEXT,
                'Alpha3' TEXT,
                'EnglishName' TEXT,
                'Region' TEXT,
                'Subregion' TEXT,
                'Population' INTEGER,
                'Area' REAL
        );
    '''
    cur.execute(statement)
    conn.commit()
    conn.close()


def insert_csv():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    with open(BARSCSV, 'r', encoding='utf-8-sig') as file_obj:
        reader = csv.reader(file_obj)
        next(reader)
        #reader[4] = float(reader[4].strip('%'))
        for row in reader:
            state = [(row[0]),(row[1]),(row[2]),(row[3]),(float(row[4].strip('%'))),(row[5]),(row[6]),(row[7]),(row[8])]
            cur.execute("INSERT INTO Bars (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent,\
                                           CompanyLocationId,Rating,BeanType,BroadBeanOriginId) VALUES \
                                            (?,?,?,?,?,?,?,?,?);", state)
            conn.commit()
            
    state_CompanyLocationId = '''
        UPDATE Bars
        SET (CompanyLocationId) = (SELECT c.ID FROM Countries c WHERE Bars.CompanyLocationId = c.EnglishName)
    '''

    state_BroadBeanOriginId = '''
        UPDATE Bars
        SET (BroadBeanOriginId) = (SELECT c.ID FROM Countries c WHERE Bars.BroadBeanOriginId = c.EnglishName)
    '''

    # execute and commit
    cur.execute(state_CompanyLocationId)
    cur.execute(state_BroadBeanOriginId)
    conn.commit()
    conn.close()
    



def insert_json():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    with open (COUNTRIESJSON,'r') as f:
        jsondata = json.loads(f.read())
    for each in jsondata:
        state = [each['alpha2Code'],each['alpha3Code'],each['name'],each['region'],each['subregion'],each['population'],each['area']]
        cur.execute("INSERT INTO Countries (Alpha2,Alpha3,EnglishName,Region,Subregion,Population,Area) VALUES \
                    (?,?,?,?,?,?,?);", state)
        conn.commit()
    conn.close()



# Part 2: Implement logic to process user commands
def process_command(command):
    each_command = command.split()
    if each_command[0] == 'bars':
        return bars(each_command)
    elif each_command[0] == 'companies':
        return companies(each_command)
    elif each_command[0] == 'countries':
        return countries(each_command)
    elif each_command[0] == 'regions':
        return regions(each_command)
    else:
        print(f'Command not recognized: {command}')
        return


def bars(bar_list):
    sort = 'ratings'
    p1 = ['sellcountry','sourcecountry','sellregion','sourceregion']
    p2 = ['ratings','cocoa']
    p3 = ['top','bottom']
    match = 'top'
    match_limit = 10
    category=''
    if len(bar_list) == 1:
        category = ''
    else:
        bar_list = bar_list[1:]
        for each in bar_list:
            #print(each)
            if '=' in each:
                temp = each.split('=')
                if temp[0] in p1:
                    if temp[0] == p1[0]:
                        category = 'sell'
                        sellercommand = f'Alpha2 = "{temp[1]}"'
                        #print(sellercommand)
                    elif temp[0] == p1[1]:
                        category = 'source'
                        sourcecommand = f'Alpha2 = "{temp[1]}"'
                    elif temp[0] == p1[2]:
                        category = 'sell'
                        sellercommand = f'Region = "{temp[1]}"'
                    elif temp[0] == p1[3]:
                        category = 'source'
                        sourcecommand = f'Region = "{temp[1]}"'
                        #print(sourcecommand)
                elif temp[0] in p3:
                    match = temp[0]
                    match_limit = temp[1]
                else:
                    print(f'Command not recognized: {each}')
                    return
            elif each in p2:
                sort = each
                #print(sort)
            elif each in p3:
                match = each
                #print(match)
            else:
                print(f'Command not recognized: {bar_list}')
                return
            
    if category == 'sell':
        statement = "SELECT SpecificBeanBarName, Company, c.EnglishName, Rating, CocoaPercent, d.EnglishName "          
        statement += "FROM Bars "
        statement += "LEFT JOIN Countries c ON Bars.CompanyLocationId = c.Id "
        statement += "LEFT JOIN Countries d ON Bars.BroadBeanOriginId = d.Id "
        statement += f"WHERE c.{sellercommand} "
       
    if category == 'source':
        statement = "SELECT SpecificBeanBarName, Company, c.EnglishName, Rating, CocoaPercent, d.EnglishName "          
        statement += "FROM Bars "
        statement += "LEFT JOIN Countries c ON Bars.CompanyLocationId = c.Id "
        statement += "LEFT JOIN Countries d ON Bars.BroadBeanOriginId = d.Id "
        statement += f"WHERE d.{sourcecommand} "
        
    if category == '':
        statement = "SELECT SpecificBeanBarName, Company, c.EnglishName, Rating, CocoaPercent, d.EnglishName "          
        statement += "FROM Bars "
        statement += "LEFT JOIN Countries c ON Bars.CompanyLocationId = c.Id "
        statement += "LEFT JOIN Countries d ON Bars.BroadBeanOriginId = d.Id "
        
    
        
    #print(statement)
    if sort == 'ratings':
        #print("11")
        statement += "ORDER BY Rating "

    if sort == 'cocoa':
        #print("11111111111")
        statement += "ORDER BY CocoaPercent "
        
    if match == 'top':
        statement += "DESC "
    if match == 'bottom':
        statement += "ASC "
        
    #if match_limit != 10:
    statement += f"LIMIT {match_limit} "
    #print(statement)
    
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()
    #print(statement)
    cur.execute(statement)
    
    #print(cur)
    result =[]
    for row in cur:
        result.append(row)
    conn.commit()
    conn.close()
    #print(type(result))
    #print(result)
    return result
        
        
    

def companies(comp_list):
    category = ''
    sort = 'ratings'
    match = 'top'
    match_limit = 10
    p1 = ['ratings','cocoa','bars_sold']
    p2 = ['top','bottom']
    p3 = ['country','region']
    if len(comp_list) == 1:
        sort=''
    else:
        comp_list = comp_list[1:]
        for each in comp_list:
            #print(each)
            if "=" in each:
                temp = each.split('=')
                if temp[0] in p3:
                    if temp[0] == p3[0]:
                        category = 'country'
                        temp_command = f'Alpha2 = "{temp[1]}"'
                    elif temp[0] == p3[1]:
                        category = 'region'
                        temp_command = f'Region = "{temp[1]}"'
                elif temp[0] in p2:
                    match = temp[0]
                    match_limit = temp[1]
                else:
                    print(f'Command not recognized: {comp_list}')
                    return
            elif each in p1:
                sort = each
            elif each in p2:
                match = each
            else:
                print(f'Command not recognized: {comp_list}')
                return

    #print(sort,match,temp)
    
    if category == 'country' or category == 'region':
        where_command = f"Where c.{temp_command} "
    elif category == '':
        where_command = ''
        
    
    if sort == 'ratings':
        statement = "SELECT Company, c.EnglishName, AVG(Rating) "
        statement += "FROM Bars "
        statement += "LEFT JOIN Countries As c ON Bars.CompanyLocationId = c.Id "
        statement += where_command
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) >4 "
        statement += "ORDER BY AVG(Rating) "
        
    elif sort == 'cocoa':
        statement = "SELECT Company, c.EnglishName, AVG(CocoaPercent) "
        statement += "FROM Bars "
        statement += "LEFT JOIN Countries As c ON Bars.CompanyLocationId = c.Id "
        statement += where_command
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) >4 "
        statement += "ORDER BY AVG(CocoaPercent) "
        
        
    elif sort == 'bars_sold':
        statement = "SELECT Company, c.EnglishName, COUNT(SpecificBeanBarName) "
        statement += "FROM Bars "
        statement += "LEFT JOIN Countries As c ON Bars.CompanyLocationId = c.Id "
        statement += where_command
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) >4 "
        statement += "ORDER BY COUNT(SpecificBeanBarName) "
        
    elif sort == '':
        statement = "SELECT Company, c.EnglishName, AVG(Rating) "
        statement += "FROM Bars "
        statement += "LEFT JOIN Countries As c ON Bars.CompanyLocationId = c.Id "
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) >4 "
        statement += "ORDER BY AVG(Rating) "
        
        
    if match == 'top':
        statement += "DESC "
        
    if match == 'bottom':
        statement += "ASC "
        
    statement += f"LIMIT {match_limit} "
    
    #print(statement)
    
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()
    #print(statement)
    cur.execute(statement)
    
    #print(cur)
    result =[]
    for row in cur:
        result.append(row)
    conn.commit()
    conn.close()
    #print(type(result))
    #print(result)
    return result

def countries(coun_list):
    sort = 'ratings'
    match = 'top'
    based = 'sellers'
    match_limit = 10
    region =''
    p1 = ['ratings','cocoa','bars_sold']
    p2 = ['top','bottom']
    p3 = ['sellers','sources']
    if len(coun_list) != 1:
        coun_list = coun_list[1:]
        for each in coun_list:
            #print(each)
            if "=" in each:
                temp = each.split('=')
                if temp[0] == 'region':
                    region = temp[1]
                    pass
                elif temp[0] in p2:
                    match = temp[0]
                    match_limit = temp[1]
                else:
                    print(f'Command not recognized: {coun_list}')
                    return
            elif each in p1:
                sort = each
            elif each in p2:
                match = each
            elif each in p3:
                if each == p3[0]:
                    based = 'sellers'
                else:
                    based = 'sources'
            else:
                print(f'Command not recognized: {coun_list}')
                return
    #print(based,sort,match,match_limit,region)
    
    
    if sort == 'ratings':
        statement = "SELECT EnglishName, Region,AVG(Rating) "
        
    elif sort == 'cocoa':
        statement = "SELECT EnglishName, Region,AVG(CocoaPercent) "
        
    elif sort == 'bars_sold':
        statement = "SELECT EnglishName, Region,COUNT(SpecificBeanBarName) "
        
    statement += "FROM countries "
    
    if based == "sellers":
        statement += "LEFT JOIN Bars ON Countries.Id = Bars.CompanyLocationId "
        
    elif based == "sources":
        statement += "LEFT JOIN Bars ON Countries.Id = Bars.BroadBeanOriginId "
        
    statement += "GROUP BY EnglishName "
    statement += "HAVING COUNT(SpecificBeanBarName) > 4 "
    
    if region:
        statement +=f"AND Region = '{region}' "
        
    if sort == 'ratings':
        statement += f"ORDER BY AVG(Rating) "
        
    elif sort == 'cocoa':
        statement += f"ORDER BY AVG(CocoaPercent) "
        
    elif sort == 'bars_sold':
        statement += f"ORDER BY COUNT(SpecificBeanBarName) "
        
    if match == 'top':
        statement += "DESC "
        
    elif match == 'bottom':
        statement += "ASC "
        
    statement += f"LIMIT {match_limit} "
    #print(statement)
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()
    #print(statement)
    cur.execute(statement)
    
    #print(cur)
    result =[]
    for row in cur:
        result.append(row)
    conn.commit()
    conn.close()
    #print(type(result))
    #print(result)
    return result
        
    

def regions(reg_list):
    sort = 'ratings'
    match = 'top'
    based = 'sellers'
    match_limit = 10
    p1 = ['ratings','cocoa','bars_sold']
    p2 = ['top','bottom']
    p3 = ['sellers','sources']
    if len(reg_list) == 1:
        pass
    else:
        reg_list = reg_list[1:]
        for each in reg_list:
            #print(each)
            if "=" in each:
                temp = each.split('=')
                if temp[0] in p2:
                    match = temp[0]
                    match_limit = temp[1]
                else:
                    print(f'Command not recognized: {reg_list}')
                    return
            elif each in p1:
                sort = each
            elif each in p2:
                match = each
            elif each in p3:
                if each == p3[0]:
                    based = 'sellers'
                else:
                    based = 'sources'
            else:
                print(f'Command not recognized: {reg_list}')
                return
    #print(based,sort,match,match_limit,region)
    
    
    if sort == 'ratings':
        statement = "SELECT Region,AVG(Rating) "
        
    elif sort == 'cocoa':
        statement = "SELECT Region,AVG(CocoaPercent) "
        
    elif sort == 'bars_sold':
        statement = "SELECT Region,COUNT(SpecificBeanBarName) "
        
    statement += "FROM countries "
    
    if based == "sellers":
        statement += "LEFT JOIN Bars ON Countries.Id = Bars.CompanyLocationId "
        
    elif based == "sources":
        statement += "LEFT JOIN Bars ON Countries.Id = Bars.BroadBeanOriginId "
        
    statement += "GROUP BY Region "
    statement += "HAVING COUNT(SpecificBeanBarName) > 4 "

        
    if sort == 'ratings':
        statement += f"ORDER BY AVG(Rating) "
        
    elif sort == 'cocoa':
        statement += f"ORDER BY AVG(CocoaPercent) "
        
    elif sort == 'bars_sold':
        statement += f"ORDER BY COUNT(SpecificBeanBarName) "
        
    if match == 'top':
        statement += "DESC "
        
    elif match == 'bottom':
        statement += "ASC "
        
    statement += f"LIMIT {match_limit} "
    #print(statement)
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()
    cur.execute(statement)    
    result =[]
    for row in cur:
        result.append(row)
    conn.commit()
    conn.close()

    #print(result)
    return result


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        if len(response) > 0 and response != 'help' and response != 'exit':
            results = process_command(response)
            if results:
                output(results)
        if response == 'help':
            print(help_text)
            continue
    print('bye')
        
    
def output(result):
    for row in result:
        for each in row: 
            #print(type(each))
            if type(each) is str:
                each = format_text(each)
                #print('1212123')
            elif type(each) is float:
                if each>10:
                    each = str(int(each))+'%'
                    each = '{:6}'.format(each)
                else:
                    each = "{0:.1f}".format(each)
                    each = '{:5}'.format(str(each))
            print(each,end='')
        print(' ')
    
    
def cut_text(long_text):
    #print(len(long_text))
    if len(long_text) > 12:
        long_text = long_text[0:12] + '...'
        return long_text
    else:
        return long_text
# Make sure nothing runs or prints out when this file is run as a module
        
def format_text(text):
    text = cut_text(text)
    text = '{:16}'.format(text)
    return text

if __name__=="__main__":
    init_db()
    insert_json()
    insert_csv()
    interactive_prompt()

    






