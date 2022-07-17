import os
import zipfile


# Define folder creation function
def create_downloads_folder():
    # Create downloads directory if not exists
    folderPath = "./downloads"
    

    if not os.path.exists(folderPath):
        print("Folder does not exist\nCreating downloads folder...")
        os.makedirs(folderPath)
    
    return folderPath

def extract_filename(url, folderPath):
    # Extract filename
    fileName = url.split('/')[-1]
    filePath = folderPath + "/" + fileName
    return fileName, filePath

def save_file(req, filePath):
    # Save file
    file = open(filePath, "wb") # write in binary mode
    file.write(req.content)
    file.close()

def unzip_file(filePath, folderPath):
    # Unzip file using context manager
    with zipfile.ZipFile(filePath, 'r') as zipObj:
        zipObj.extractall(folderPath)