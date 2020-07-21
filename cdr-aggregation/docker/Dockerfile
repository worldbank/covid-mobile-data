FROM jupyter/pyspark-notebook:dc9744740e12

RUN python --version

RUN conda install --quiet --yes -c \
    conda-forge jupyter_contrib_nbextensions jupyter_nbextensions_configurator \
    geopandas folium descartes

RUN pip install -U folium \
                   geovoronoi \
                   geopy

RUN jupyter labextension install @jupyterlab/toc

VOLUME /home/jovyan/work
WORKDIR /home/jovyan/work
