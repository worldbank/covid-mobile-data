# Aggregation notebooks

**How to run `aggregtion_offsite.ipynb` in a docker container**


## Step 1
Clone this repository if you haven’t yet.

## Step 2
Open a terminal and cd into the `cdr-aggregation` directory. Then run `docker-compose up` to start the docker container. You'll see the url for the jupyter server, copy that into your browser.

## Step 3
Set up the folder structure as explained on the readme to [cdr-aggregation](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation). The container is bind-mounted to the cdr-aggregation folder in the github repo you cloned. Use the `cdr-aggregation` folder as the parent for the `data` folder and then follow the structure explained in the readme. All the folders you create should then appear both on your machine and inside the container, as should files that you store there.

## Step 4
Add your raw data to the `data/new/CC/telco` folder.

## Step 5
Add shapefiles and tower locations to `data/support-data/***REMOVED***/geofiles`. Get in touch to get assistance in accessing or creating these.

## Step 6
Modify and add the `config_file.py` to `data/support-data`. 

## Step 7
Open the `aggregation_offsite.ipynb` notebook to run aggregations. If all went well, you should find the indicators in the `data/results folder`
