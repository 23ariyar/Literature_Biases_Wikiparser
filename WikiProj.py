import bz2
import os
from func_timeout import func_set_timeout
from WikiDBBZ2 import WikiDB
import time
from typing import Tuple, List



pathWikiBZ2 = 'C:\\Users\\16507\\Downloads\\enwiki-20201101-pages-articles-multistream5.xml.bz2' #.xml.bz2 file path
#pathWikiDB = 'C:\\Users\\16507\\Downloads\\wiki.db'
FTR = ["novel"] #filter - these words must be in categories to append to the db - make sure the words are in lowercase
NON_FTR = ["births", "musical", "television series", "films"] #filter - these words must NOT be categories to append to the db - make sure the words are in lowercase
start_time = time.time()

bz2_file = bz2.BZ2File(pathWikiBZ2) 


def passes_filter(categories: List[str]) -> bool:
    '''
    Returns a bool depending on if the categories passes the filter
    :param categories: a list of the categories
    '''

    crepr = repr(categories)

    if any([i in crepr for i in NON_FTR]): return False

    return all([i in crepr for i in FTR]) 



def hms_string(sec_elapsed: int) -> str:
    """
    Gets time in Hour:Minutes:Seconds
    :param sec_elapsed: seconds elapsed
    :return: Hour:Minutes:Seconds
    """
    h = int(sec_elapsed / (60 * 60))
    m = int((sec_elapsed % (60 * 60)) / 60)
    s = sec_elapsed % 60
    return "{}:{:>02}:{:>05.2f}".format(h, m, s)


def remove_tag_id(tag: str) -> str:
    '''
    Removes tag on id line
    '''
    return ''.join([i for i in tag if i in '1234567890'])


def parseBZ2Page(file: bz2.BZ2File): #check if id already in db
    '''
    Given that the :param file:'s pointer is at one line past the beginning of the wiki page (line after </page>)
    returns the ID and categories of the Wikipedia page
    :param file: bz2.BZ2File
    '''
    categories = []
    in_categories = False
    found_id = False
    found_ns = False
    found_title = False

    #decompressed_file = b''
    


    for line in file:
        #decompressed_file += line
        if not found_id and b'<id>' in line: #More than one b'<id>' in Wiki page, but first one will be the official ID
            id = remove_tag_id(line.decode("utf-8"))
            found_id = True
        
        if not found_ns and b'<ns>' in line: #If not article page, skip. Article pages have a ns (namespace) of 0
            if b'<ns>0</ns>' not in line:
                return False

        if not found_title and b'<title>' in line: #Parses for the title
            title = line[11:-9]

        if not in_categories: #If not in the category section yet, check to see if you have gotten there
            if b'[[Category:' in line: 
                in_categories = True
                try: categories.append(line.decode("utf-8")[11:-3]) #For some reason, there are lines that can't be decoded
                except UnicodeDecodeError: print(line)
        elif b'[[Category:' not in line: #Once in categories section, check to see if you have exited. If yes, exit.
            break
        else:
            try: categories.append(line.decode("utf-8")[11:-3])
            except UnicodeDecodeError: print(line)
    
    #print(decompressed_file)
    file.seek(-1, 1) #offsets back one (for loop reads an extra non-b'[[Category:' line)
    return ([i if (i[-7:] != ']]</tex') else i[:-7] for i in categories], id, title) #Removes the ]]<tex tag for some lines

def main(file, db):
    '''
    Parses a .xml.bz2 file and returns a dictionary {ID, [Categories]} of articles that pass the filter FTR 
    :param file: bz2.BZFile object
    '''
    pc = 0
    ac = 0
    for line in file: 
        if b'<page>' in line: #</page> indicates new Wikipedia page

            parsed_data = parseBZ2Page(file)
            if parsed_data:
                (categories, id, title) = parsed_data
                pc += 1
            else:
                continue

            if passes_filter(categories): 
                db.insert(id, title, repr(categories))
                ac += 1

            if (pc % 150 == 0): #Print progress every 150 pages
                elapsed_time = time.time() - start_time
                print("{} articles parsed".format(pc), end=" ")
                print("{} articles passes filter".format(ac), end=" ")
                print("Elapsed time: {}".format(hms_string(elapsed_time)))

            #if pc >= 75: #For debugging
            #    break

    db.commit()
    file.close()
    print("Completed! \n")

    return db 



if __name__ == '__main__':
    database = WikiDB("wiki.db")
    print(main(bz2_file, database))
