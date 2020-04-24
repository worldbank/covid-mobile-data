import os
######################################
  # Folder setup methods - written for the jupyter notebook docker image

#Loops over the requred folders for teh datasource and create any missing folders
def setup_folder(datasource):
    #Loop over required paths, and return true
    for folder in datasource.required_folders():
        test_folder(folder, create_if_not_exist=True)
    return True

#Check if all required folders exist without creating them
def check_folders(datasource):
    return_boolean = True
    #loop over required folders
    for folder in datasource.required_folders():
        if not test_folder(folder, create_if_not_exist=False):
            print("Folder '{}' is required but does not exist".format(folder))
            return_boolean = False
    return return_boolean

#Utility that check if folder exist
def test_folder(path, create_if_not_exist):
    #If folder exists return true
    if os.path.exists(path): return True
    #Else: if create_if_not_exist is true then create folder and return true
    elif create_if_not_exist:
        os.makedirs(path)
        return True
    #Else: Folder does not exist and folder is not created, return false
    else: return False
