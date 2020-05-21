#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import datetime as dt
from modules.DataSource import *
from modules.folder_utils import *


# In[ ]:


#Set relative file path to config file
config_file = '../config_file.py'
exec(open(config_file).read())


# In[ ]:


#Create the DataSource object and show config
ds = DataSource(datasource_configs)
ds.show_config()


# In[ ]:


#Setup all required data folders
setup_folder(ds)


# In[ ]:


#Check if required data folders already exists
check_folders(ds)

