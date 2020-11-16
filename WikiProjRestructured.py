import bz2
import os
import xml.etree.ElementTree as ET

import time
from typing import Tuple, List

from WikiDBBZ2 import WikiDB




pathWikiBZ2 = 'C:\\Users\\16507\\Downloads\\enwiki-20201020-pages-articles-multistream.xml.bz2' #.xml.bz2 file path
pathWikiDB = 'C:\\Users\\16507\\Documents\\Projects\\WikipediaProject\\Literature-Biases---Wikipedia\\wiki.db'
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

def parseBZ2Page(file: bz2.BZ2File, page_line: bytes): #check if id already in db
    '''
    Given that the :param file:'s pointer is at one line past the beginning of the wiki page (line after </page>)
    returns the categories, id, and title through a tuple
    :param file: bz2.BZ2File
    :param page_line: bytes
    '''
    categories = []
    
    decompressed_file_as_str = page_line.decode("utf-8")

    for line in file: 
        decoded = line.decode("utf-8")
        decompressed_file_as_str += decoded

        if b'[[Category:' in line:
            categories.append(decoded[11:-3]) 

        elif b'</page>' in line: #Once reading a </page> tag, exit. 
            break
    
    root = ET.fromstring(decompressed_file_as_str)
    
    if root.find('ns').text != '0' or 'redirect title' in decompressed_file_as_str: return None

    title = root.find('title').text
    id = root.find('id').text

    '''
    Debugging statements
    print(decompressed_file_as_str)
    print('NS:', ns, '\n', 'Title:', title, '\n', 'ID:', id)
    raise Exception('Let me just take a peek!')
    '''

    return ([i if (i[-7:] != ']]</tex') else i[:-7] for i in categories], id, title) #Removes the ]]<tex tag for some lines

def main(file, db):
    '''
    Parses a .xml.bz2 file and returns a dictionary {ID, [Categories]} of articles that pass the filter FTR 
    :param file: bz2.BZFile object
    '''
    pc = 0 #page count
    ac = 0 #added count

    for line in file: 
        if b'<page>' in line: #</page> indicates new Wikipedia page

            parsed_data = parseBZ2Page(file, line)

            if parsed_data: #parsed_data returns None when the page should be excluded
                (categories, id, title) = parsed_data
                pc += 1
            else:
                continue

            if passes_filter(categories): #if categories passes filter, add to database
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
    database = WikiDB(pathWikiDB)
    print(main(bz2_file, database))
