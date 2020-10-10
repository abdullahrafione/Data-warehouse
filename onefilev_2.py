import os
import re
import pandas as pyd
from datetime import datetime
from fnmatch import fnmatch

import pyodbc
conn = pyodbc.connect('Driver={SQL Server};'
                                  'Server=DESKTOP-DL730GB;'
                                  'Database=dataware;'
                                  'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute("CREATE TABLE temp_research (id INT IDENTITY(1,1) PRIMARY KEY, Title varchar(MAX),Journal varchar(MAX),Doc_type varchar(MAX),Abstract TEXT,Number_of_references_in_article varchar(MAX),Time_Cited  varchar(MAX),languages  varchar(MAX),Last_180_days_downloads varchar(MAX),downloads_since_2013 varchar(MAX),published_year varchar(MAX),start_page varchar(MAX),End_page varchar(255))")
cursor.commit()
cursor.execute(" CREATE TABLE  temp_Author (id INT IDENTITY(1,1) PRIMARY KEY,Author_name varchar(MAX) )")
cursor.commit()
cursor.execute(" CREATE TABLE  temp_WebofScience (id INT IDENTITY(1,1) PRIMARY KEY,FK_Research_ID int,webofscience_name varchar(MAX) )")
cursor.commit()
cursor.execute(" CREATE TABLE  temp_Author_Keyword (id INT IDENTITY(1,1) PRIMARY KEY,FK_Research_ID int,Authorkeyword varchar(MAX) )")
cursor.commit()
cursor.execute("CREATE TABLE  temp_keyword_plus (id INT IDENTITY(1,1) PRIMARY KEY,FK_Research_ID int,Keyword_Plus varchar(MAX) )")
cursor.commit()
cursor.execute("CREATE TABLE  temp_Research_Area (id INT IDENTITY(1,1) PRIMARY KEY,FK_Research_ID int,Research_Area varchar(MAX) )")
cursor.commit()
cursor.execute("CREATE TABLE  temp_Research_Author (id INT IDENTITY(1,1) PRIMARY KEY,FK_Research_id int,FK_author_id int )")
cursor.commit()
cursor.execute("CREATE TABLE  temp_Research_Affliation (id INT IDENTITY(1,1) PRIMARY KEY,Author_fK_ID int,Institute_fK_ID int )")
cursor.commit()
cursor.execute("CREATE TABLE  temp_Institute (id INT IDENTITY(1,1) PRIMARY KEY,Institute_name varchar(MAX) )")
cursor.commit()
root = 'F:/Software Engineering/Semester-6/Dataware house/DatawareHouseproject/newdata'
pattern = "*.txt"
for path, subdirs, files in os.walk(root):
    for name in files:
        if fnmatch(name, pattern):
            file_name = (os.path.join(path, name))
            print(file_name)
            rex_SI = '^SI'
            rex_TI = '^TI'
            rex_SO = '^SO'
            rex_AB = '^AB'
            rex_LA = '^LA'
            rex_DT = '^DT'
            rex_DI = '^DI'
            rex_DE = '^DE'
            rex_ID = '^ID'
            rex_AD = '^AD'
            rex_C1 = '^C1'
            rex_RP = '^RP'
            rex_EM = '^EM'
            rex_RI = '^RI'
            rex_TC = '^TC'
            rex_Z9 = '^Z9'
            rex_U1 = '^U1'
            rex_U2 = '^U2'
            rex_PU = '^PU'
            rex_PI = '^PI'
            rex_PA = '^PA'
            rex_VL = '^VL'
            rex_IS = '^IS'
            rex_BP = '^BP'
            rex_EP = '^EP'
            rex_PN = '^PN'
            rex_PG = '^PG'
            rex_SC = '^SC'
            rex_GA = '^GA'
            rex_UT = '^UT'
            rex_DA = '^DA'
            rex_CT = '^CT'
            rex_AR = '^AR'
            # SQL opeartions

            # STRING TO LIST

            def listToString(s):
                str1 = ""
                for ele in s:
                    str1 += ele
                    str1 += ";"
                return str1

            def listToString1(s):
                str1 = ""
                for ele in s:
                    str1 += ele
                    str1 += " "
                return str1

            # SEMI COLON SLICER
            # comma_to_semiColon

            def comma_to_semicolon(input_para):
                dataset = input_para
                dataset_semicolon = dataset.replace(',', ';')
                return (dataset_semicolon)

            def semicolon_slicer(input_parameter):
                data = input_parameter
                return_data_list = []
                string_data = str(data).split(';')
                for x in range(len(string_data)):
                    strip_data = string_data[x]
                    strip_dataed = str(strip_data).strip()
                    return_data_list.append(strip_dataed)
                return (return_data_list)

            # AUTHOR FINDER
            def AF_line(num, lines_file):
                num = num
                lines_file = lines_file
                author_return = (AF_read(num, lines_file))  # list=> array
                string_author = (listToString(author_return))  # String
                string_author_split_Af = string_author.split('AF')
                string_author_splited_Af = listToString(string_author_split_Af)
                string_author_splited_semi = string_author_splited_Af.split(
                    ';')
                author_list = []
                Author_list = []
                for x in range(len(string_author_splited_semi)):
                    if (len(string_author_splited_semi[x]) == 0):
                        continue
                    else:
                        dataset = string_author_splited_semi[x]
                        dataset1 = dataset.strip()
                        author_list.append(dataset1)
                for x in range(len(author_list)):
                    Author_comma = author_list[x]
                    author_split = str(Author_comma).split(',')
                    if (len(author_split) == 0 | len(author_split) == 1):
                        dataset2 = str(author_split).strip()
                        Author_list.append(dataset2)
                    else:
                        f_name = str(author_split[1]).strip()
                        l_name = str(author_split[0]).strip()
                        fullname = f_name + ' ' + l_name
                        Author_list.append(fullname)

                return (Author_list)

            def AF_read(num, lines_file):
                index = num
                all_lines = lines_file
                new_list = []
                # increment index symbol fetch all data
                data = (all_lines[index::1])
                y = data[0::]
                for x in range(len(y)):
                    if (re.match(rex_TI, y[x])):
                        return new_list
                    elif (re.match(rex_SO, y[x])):
                        return new_list
                    # check for next tag if found return data lol
                    elif (re.match(rex_LA, y[x])):
                        return new_list  # return attribue, save in varibal and pass it in database
                    else:
                        dataw = str(y[x]).strip()
                        new_list.append(dataw)
                        # return (new_list)

            # TITLE FINDER
            def TI_line(num, lines_file):
                num = num
                lines_file = lines_file
                re_title = (TI_read(num, lines_file))
                string_title = listToString1(re_title)
                string_title_split = string_title.split('TI ')
                # print("sss",string_title_split)
                string_title_splited = (string_title_split[1])
                title = str(string_title_splited).strip()
                return (title)

            def TI_read(num, lines_file):
                index = num
                all_lines = lines_file
                new_list = []
                # increment index symbol fetch all data
                data = (all_lines[index::1])
                y = data[0::]
                for x in range(len(y)):
                    if (re.match(rex_SO, y[x])):
                        return new_list
                    if (re.match(rex_LA, y[x])):
                        return new_list
                    # check for next tag if found return data lol
                    if (re.match(rex_DT, y[x])):
                        return new_list  # return attribue, save in varibal and pass it in database
                    # check for next tag if found return data lol
                    if (re.match(rex_DE, y[x])):
                        return new_list  # return attribue, save in varibal and pass it in database
                    # check for next tag if found return data lol
                    if (re.match(rex_ID, y[x])):
                        return new_list  # return attribue, save in varibal and pass it in database
                    else:
                        dataw = str(y[x]).strip()
                        new_list.append(dataw)

            #  JOURNAL FINDER
            def SO_line(num, lines_file):
                num = num
                lines_file = lines_file
                re_journal = (SO_read(num, lines_file))
                string_journal = listToString1(re_journal)
                string_journal_split = string_journal.strip('SO ')
                string_journal_splited = string_journal_split
                journal = str(string_journal_splited).strip()
                return (journal)

            def SO_read(num, lines_file):
                index = num
                all_lines = lines_file
                new_list = []
                # increment index symbol fetch all data
                data = (all_lines[index::1])
                y = data[0::]
                for x in range(len(y)):
                    if (re.match(rex_LA, y[x])):
                        # print(y[x])
                        return new_list
                    elif (re.match(rex_DT, y[x])):
                        return new_list
                    # check for next tag if found return data lol
                    elif (re.match(rex_DE, y[x])):
                        return new_list  # return attribue, save in varibal and pass it in database
                    else:
                        dataw = str(y[x]).strip()
                        new_list.append(dataw)
                        # return (new_list)

            # AUTHOR_KEYWORD

            def DE_line(num, lines_file):
                num = num
                lines_file = lines_file
                DE_re = (DE_read(num, lines_file))
                DE_sting = listToString1(DE_re)
                DE_split = str(DE_sting).split('DE ')
                DE_splited = DE_split[1]
                DE = str(DE_splited).strip()
                Author_keyword = semicolon_slicer(DE)
                return (Author_keyword)

            def DE_read(num, lines_file):
                index = num
                all_lines = lines_file
                new_list = []
                # increment index symbol fetch all data
                data = (all_lines[index::1])
                y = data[0::]
                for x in range(len(y)):
                    if (re.match(rex_ID, y[x])):
                        return new_list
                    elif (re.match(rex_AB, y[x])):
                        return new_list
                    # check for next tag if found return data lol
                    elif (re.match(rex_C1, y[x])):
                        return new_list  # return attribue, save in varibal and pass it in database
                    # check for next tag if found return data lol
                    elif (re.match(rex_RP, y[x])):
                        return new_list  # return attribue, save in varibal and pass it in database
                    else:
                        dataw = str(y[x]).strip()
                        new_list.append(dataw)
                        # return (new_list)

            # KEYWORD_PLUS
            def ID_line(num, lines_file):
                num = num
                lines_file = lines_file
                ID_re = (ID_read(num, lines_file))
                ID_sting = listToString1(ID_re)
                ID_split = str(ID_sting).split('ID ')
                if (len(ID_split) <= 1):
                    return ("Null")
                else:
                    ID_splited = ID_split[1]
                    ID = str(ID_splited).strip()
                    keyword_plus = semicolon_slicer(ID)
                    return (keyword_plus)

            def ID_read(num, lines_file):
                index = num
                all_lines = lines_file
                new_list = []
                # increment index symbol fetch all data
                data = (all_lines[index::1])
                y = data[0::]
                for x in range(len(y)):
                    if (re.match(rex_AB, y[x])):
                        # print(y[x])
                        return new_list
                    elif (re.match(rex_C1, y[x])):
                        return new_list
                    # check for next tag if found return data lol
                    elif (re.match(rex_RP, y[x])):
                        return new_list  # return attribue, save in varibal and pass it in database
                    else:
                        dataw = str(y[x]).strip()
                        new_list.append(dataw)
                        # return (new_list)

            # ABSTRACRT FINDER
            def AB_line(num, lines_file):
                num = num
                lines_file = lines_file
                abstract_re = (AB_read(num, lines_file))
                abstract_sting = listToString1(abstract_re)
                abstract_split = str(abstract_sting).split('AB ')
                abstract_splited = abstract_split[1]
                abstract = str(abstract_splited).strip()
                return (abstract)

            def AB_read(num, lines_file):
                index = num
                all_lines = lines_file
                new_list = []
                # increment index symbol fetch all data
                data = (all_lines[index::1])
                y = data[0::]
                for x in range(len(y)):
                    if (re.match(rex_C1, y[x])):
                        return new_list
                    elif (re.match(rex_RP, y[x])):
                        return new_list
                    # check for next tag if found return data lol
                    elif (re.match(rex_EM, y[x])):
                        return new_list  # return attribue, save in varibal and pass it in database
                    else:
                        dataw = str(y[x]).strip()
                        new_list.append(dataw)
                        # return (new_list)

            # AUTHOR_AFFLICATION FINDER
            def C1_line(num, lines_file):
                num = num
                lines_file = lines_file
                C1_re = (C1_read(num, lines_file))
                C1_sting = listToString1(C1_re)
                C1_split = str(C1_sting).split('C1 ')
                C1_splited = C1_split[1]
                C1 = str(C1_splited).strip()
                return ("C1 for temp_table", C1)

            def C1_read(num, lines_file):
                index = num
                all_lines = lines_file
                new_list = []
                # increment index symbol fetch all data
                data = (all_lines[index::1])
                y = data[0::]
                for x in range(len(y)):
                    if (re.match(rex_RP, y[x])):
                        # print(y[x])
                        return new_list
                    elif (re.match(rex_EM, y[x])):
                        return new_list
                    # check for next tag if found return data lol
                    elif (re.match(rex_RI, y[x])):
                        return new_list  # return attribue, save in varibal and pass it in database
                    else:
                        dataw = str(y[x]).strip()
                        new_list.append(dataw)
                        # return (new_list)

            # WEB OF SCIENCE WC

            def WC_line(num, line_file):
                num = num
                line_file = line_file
                WC_re = (WC_read(num, line_file))
                if (WC_re == None):
                    return 0
                else:
                    WC_sting = listToString1(WC_re)
                    WC_split = str(WC_sting).split('WC ')
                    WC_splited = WC_split[1]
                    WC = str(WC_splited).strip()
                    woc = comma_to_semicolon(WC)
                    Web_of_Science = semicolon_slicer(woc)
                    return (Web_of_Science)

            def WC_read(num, lines_file):
                index = num
                all_lines = lines_file
                new_list = []
                # increment index symbol fetch all data
                data = (all_lines[index::1])
                y = data[0::]
                for x in range(len(y)):
                    if (re.match(rex_SC, y[x])):
                        return new_list
                    elif (re.match(rex_GA, y[x])):
                        return new_list
                    # check for next tag if found return data lol
                    elif (re.match(rex_UT, y[x])):
                        return  # return attribue, save in varibal and pass it in database
                    else:
                        dataw = str(y[x]).strip()
                        new_list.append(dataw)
                        # return (new_list)

            # RESEARCH_AREA SC
            def SC_line(num, lines_file):
                num = num
                lines_file = lines_file
                SC_re = (SC_read(num, lines_file))
                SC_sting = listToString1(SC_re)
                SC_split = str(SC_sting).split('SC ')
                SC_splited = SC_split[1]
                SC = str(SC_splited).strip()
                sc_research_area = comma_to_semicolon(SC)
                Research_area_sc = semicolon_slicer(sc_research_area)
                return (Research_area_sc)

            def SC_read(num, line_file):
                index = num
                all_lines = line_file
                new_list = []
                # increment index symbol fetch all data
                data = (all_lines[index::1])
                y = data[0::]
                for x in range(len(y)):
                    if (re.match(rex_GA, y[x])):
                        return new_list
                    elif (re.match(rex_UT, y[x])):
                        return new_list
                    # check for next tag if found return data lol
                    elif (re.match(rex_DA, y[x])):
                        return new_list  # return attribue, save in varibal and pass it in database
                    else:
                        dataw = str(y[x]).strip()
                        new_list.append(dataw)
                        # return (new_list)

            with open(file_name, encoding='utf-8') as inputfile:
                Title = Journal = Doc_type = Language = Abstract = Number_of_references_in_article = Time_Cited = Last_180_days_downloads = downloads_since_2013 = published_year = start_page = End_page = " "
                Web_of_Science_category = []
                Research_area = []
                keyword_plus = []
                Author_Keyword = []
                Authors = []
                start_time = datetime.now()
                all_lines = inputfile.readlines()

                # Num will calculate, line=no of lines in file
                for num, line in enumerate(all_lines):
                    # do your work here

                    if line.startswith("AF "):
                        if re.search('^AF', line):
                            Authors = AF_line(num, all_lines).copy()
                    if line.startswith("TI "):
                        if re.search('^TI', line):
                            Title = (TI_line(num, all_lines))
                    if line.startswith("SO "):
                        if re.search('^SO', line):
                            Journal = (SO_line(num, all_lines))
                    if line.startswith("LA "):
                        Language = str((line[2:])).rstrip()
                    if line.startswith("DT "):
                        if re.search('^DT', line):
                            Doc_type = str((line[2:])).rstrip()
                    if line.startswith("DE "):
                        if re.search('^DE', line):
                            Author_Keyword = DE_line(num, all_lines).copy()
                    if line.startswith("ID "):
                        if re.search('^ID', line):
                            keyword_plus_re = ID_line(num, all_lines)
                            if (keyword_plus_re == 'Null'):
                                keyword_plus = ['Null']
                            else:
                                keyword_plus = keyword_plus_re.copy()
                    if line.startswith("AB "):
                        if re.search('^AB', line):
                            Abstract = (AB_line(num, all_lines))
                    # if line.startswith("C1 "):
                    #     if re.search('^C1', line):
                    #         print(C1_line(num))
                    if line.startswith("NR "):
                        if re.search('^NR', line):
                            Number_of_references_in_article = str(
                                (line[2:])).rstrip()
                    if line.startswith("TC "):
                        if re.search('^TC', line):
                            Time_Cited = str((line[2:])).rstrip()
                    if line.startswith("U1 "):
                        if re.search('^U1', line):
                            Last_180_days_downloads = str((line[2:])).rstrip()
                    if line.startswith("U2 "):
                        if re.search('^U2', line):
                            downloads_since_2013 = str((line[2:])).rstrip()
                    if line.startswith("PY "):
                        if re.search('^PY', line):
                            published_year = str((line[2:])).rstrip()
                    if line.startswith("BP "):
                        if re.search('^BP', line):
                            start_page = str((line[2:])).rstrip()
                    if line.startswith("EP "):
                        if re.search('^EP', line):
                            End_page = str((line[2:])).rstrip()
                    if line.startswith("WC "):
                        if re.search('^WC', line):
                            Web_of_Science_category = WC_line(
                                num, all_lines).copy()
                    if line.startswith("SC "):
                        if re.search('^SC', line):
                            Research_area = SC_line(num, all_lines).copy()
                    if line.startswith("ER"):

                        cursor.execute("INSERT INTO temp_research VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (
                            Title, Journal, Doc_type, Abstract, Number_of_references_in_article, Time_Cited, Language,
                            Last_180_days_downloads, downloads_since_2013, published_year, start_page, End_page))
                        cursor.commit()
                        # to get last key

                        cursor.execute("SELECT id FROM temp_research order by id DESC")
                        result=cursor.fetchone()
                        pk_temp_research =int(result[0])


                        for x in range(len(Web_of_Science_category)):
                            dataset = Web_of_Science_category[x]
                            cursor.execute("INSERT INTO temp_WebofScience VALUES (?,?)",(pk_temp_research,dataset))
                            cursor.commit()

                        for x in range(len(Research_area)):
                            dataset = Research_area[x]
                            cursor.execute(
                                "INSERT INTO temp_Research_Area VALUES (?,?)", (pk_temp_research, dataset))
                            cursor.commit()
                        for x in range(len(keyword_plus)):
                            dataset = keyword_plus[x]
                            cursor.execute(
                                "INSERT INTO temp_keyword_plus VALUES (?,?)", (pk_temp_research, dataset))
                            cursor.commit()
                        for x in range(len(Author_Keyword)):
                            dataset = Author_Keyword[x]
                            cursor.execute(
                                "INSERT INTO temp_Author_Keyword  VALUES (?,?)", (pk_temp_research, dataset))
                            cursor.commit()
                        for x in range(len(Authors)):
                            dataset = Authors[x]
                            cursor.execute(
                                "INSERT INTO temp_Author (Author_name) VALUES (?)", (dataset))
                            cursor.commit()
                            cursor.execute("SELECT id FROM temp_Author order by id DESC")

                            result = cursor.fetchone()
                            pk_temp_author = (result[0])
                            cursor.execute(
                                "INSERT INTO temp_research_author VALUES (?,?)", (pk_temp_research, pk_temp_author))
                            cursor.commit()

                        # print(Title ,Journal ,Doc_type,Abstract,Number_of_references_in_article,Time_Cited,Language,Last_180_days_downloads,downloads_since_2013,published_year,start_page,End_page)
                        print("ENTER DATA IN TEMP_TABLES")
                print("out of loop")
                end_time = datetime.now()
                print('Duration: {}'.format(end_time - start_time))
