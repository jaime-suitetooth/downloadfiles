import csv
import logging
import multiprocessing
from multiprocessing import Pool
from netsuitesdk import NetSuiteConnection
from pathlib import Path
# OPERATING_SYSTEM = 'linux'

NETSUITE_ACCOUNT = '1285441'
TOKEN_SECRET = 'c2eedf59afd32be0c7c64ca20837f536420e9f56ad7f2dd6c70b72a54ae420da'
TOKEN_KEY ='d58273025fa9534db6ef7360cdcb58d09e5ad5eb72a485328e5b3ddb85ef8e6d'
CONSUMER_KEY = '56bc5cc192ecef5f51ed89f1c1bcedf86c84fb592a82523f39a70d1520da3f2d'
CONSUMER_SECRET = '73d34a7f290b0f8fdaafc77bbb101206b39fc5866483eae901b0e3034e3d5b8d'
DELIMITER  = ';'
FILE_ID_INDEX = 'fileid'
FOLDER_NAME_INDEX = 'foldername'
FOLDER_ID_INDEX = 'folderid'   
PARENT_FOLDER_INDEX = 'parentfolder'
PARENT_ID_INDEX = 'parentid'   
FILE_NAME_INDEX = 'filename'
ROOTH_PATH = '.'
FILES_PATH = './files_sorted'

class File:

    def __init__(self, name: str, id: str) -> None:
        if name is  None or id is None or len(name) == 0 or len(id) == 0:
            return
        self.name = name
        self.id = id

class Folder:
    def __init__(self, name: str, id:str, parent_id: str, ) -> None:
        self.name = name
        self.path = None
        self.files = []
        self.id = id
        self.parent_id = parent_id
        self.childs = []
        # self.connection = connection

    def add_file(self, name:str, id: str ) -> None:
        if name is None or id is None or len(name) == 0 or len(id) == 0:
            return
        fileInstance = File(name, id)
        self.files.append(fileInstance)
 
    def create_dir(self, path) -> None:
        directory = path / self.name
        directory.mkdir(parents=True, exist_ok=True)
        # logging.warn(f"Folder {self.name} created")
        self.path = directory

class FolderTree: 
    def __init__(self, root_path: str,  csv_file: str, files_path: str, folder_id:str ) -> None:
        self.folders = {}
        self.all_folders = []
        # self.connection = connect()

        self.root_folder = Folder(csv_file, folder_id , None)
        self.load_csv(csv_file, files_path)
        self.recursive_link(self.root_folder, Path(root_path))
         
    def add_folder(self, row: str) -> Folder:
        if row[PARENT_ID_INDEX] is not None and len(row[PARENT_ID_INDEX]) > 0:
            folder = Folder(row[FOLDER_NAME_INDEX], row[FOLDER_ID_INDEX], row[PARENT_ID_INDEX])
            if row[PARENT_ID_INDEX] in self.folders:
                self.folders[row[PARENT_ID_INDEX]].append(folder)
            else:
                self.folders[row[PARENT_ID_INDEX]] = [folder]
            self.all_folders.append(folder)
        else:
            folder = self.root_folder

        return folder

    def load_csv(self, csv_file: str, files_path: str) -> None:
        csv_path = Path(files_path) / csv_file
        with csv_path.open('r') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=DELIMITER)
            row = next(reader)
            folder = self.add_folder(row)
            folder.add_file(row[FILE_NAME_INDEX], row[FILE_ID_INDEX])
            folder_id = row[FOLDER_ID_INDEX]
            parent_id = row[PARENT_ID_INDEX]
            while True:
                try:
                    next_row = next(reader)
                    if next_row[PARENT_ID_INDEX] == parent_id and next_row[FOLDER_ID_INDEX] == folder_id:
                        folder.add_file(next_row[FILE_NAME_INDEX], next_row[FILE_ID_INDEX])
                    else:
                        folder = self.add_folder(next_row)
                        folder.add_file(next_row[FILE_NAME_INDEX], next_row[FILE_ID_INDEX])
                        folder_id = next_row[FOLDER_ID_INDEX]
                        parent_id = next_row[PARENT_ID_INDEX]
                except StopIteration:
                    break

    def recursive_link(self, folder, path) -> None:
        folder.create_dir(path)
        if folder.id in self.folders:
            folder.childs = self.folders[folder.id]
            for child in folder.childs:
                self.recursive_link(child, folder.path)
        else:
            return

def conne() -> NetSuiteConnection:
        NS_ACCOUNT = NETSUITE_ACCOUNT
        NS_CONSUMER_KEY = CONSUMER_KEY
        NS_CONSUMER_SECRET = CONSUMER_SECRET
        NS_TOKEN_KEY = TOKEN_KEY
        NS_TOKEN_SECRET =TOKEN_SECRET
        nc = NetSuiteConnection(
            account=NS_ACCOUNT,
            consumer_key=NS_CONSUMER_KEY,
            consumer_secret=NS_CONSUMER_SECRET,
            token_key=NS_TOKEN_KEY,
            token_secret=NS_TOKEN_SECRET,
        )
        return nc

def donwnload_files(data):
    # logging.warn(data)
    con = conne()
    for tm in data:
        file_binary = con.files.get(internalId=tm[0])
        file_path = tm[1]
        with file_path.open("wb") as f:
            f.write(file_binary.content)

if __name__ == '__main__':

    csv_file = 'Celigo.csv'
    folder_id = '269'
    jobs = []
    job_queue = multiprocessing.Queue()
    folder_tree = FolderTree(ROOTH_PATH, csv_file, FILES_PATH, folder_id)
    count = 0
    tmp_array_of_chunks = []
    all_files_to_process = []
    for fold in folder_tree.all_folders:
    
        for file in fold.files:
            tmp_file = [file.id, fold.path / file.name ]
            if count > 98:
                tmp_array_of_chunks.append(tmp_file)
                all_files_to_process.append(tmp_array_of_chunks)
                tmp_array_of_chunks = []
                count = 0
            else:
                count += 1
                tmp_array_of_chunks.append(tmp_file)
                
    if count > 0:
        all_files_to_process.append(tmp_array_of_chunks)   

    p = Pool(49)
    p.map(donwnload_files, all_files_to_process)

