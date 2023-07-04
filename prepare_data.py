#!/usr/bin/env python
"""
Prepare the NorESM2 output by getting it directly from the http://noresg.nird.sigma2.no THREDDS server
"""

# from siphon import catalogs
import pandas as pd
import xarray as xr
from dask.distributed import Client, LocalCluster
import os.path
import requests
overwrite = False

model = 'NorESM2-LM'
experiments = [
               '1pctCO2', 'abrupt-4xCO2', 'historical', 'piControl', # CMIP
               'hist-GHG', 'hist-aer', # DAMIP
               'ssp126', 'ssp245', 'ssp370', 'ssp370-lowNTCF', 'ssp585' #	ScenarioMIP
]
variables = [
             'tas', 'tasmin', 'tasmax', 'pr'
]

# Check the PANGEO holdings
# for AWS S3:
#df = pd.read_csv("https://cmip6-pds.s3.amazonaws.com/pangeo-cmip6-noQC.csv")

# Unfortuntaly they're missing a couple of the scenarios. 
#set(experiments) - set(df.query("source_id==@model & experiment_id in @experiments").experiment_id.unique())

# And the ones that are there don't have all the variables I need

# So use the NIRD ESGF node directly 
def get_MIP(experiment):
  if experiment == 'ssp245-covid':
    return 'DAMIP'
  elif experiment == 'ssp370-lowNTCF':
    return 'AerChemMIP'
  elif experiment.startswith('ssp'):
    return 'ScenarioMIP'
  elif experiment.startswith('hist-'):
    return 'DAMIP'
  else:
    return 'CMIP'

def esgf_search(server="https://esgf-node.llnl.gov/esg-search/search",
                files_type="OPENDAP", local_node=True, project="CMIP6",
                verbose=False, format="application%2Fsolr%2Bjson",
                use_csrf=False, **search):
    client = requests.session()
    payload = search
    payload["project"] = project
    payload["type"]= "File"
    if local_node:
        payload["distrib"] = "false"
    if use_csrf:
        client.get(server)
        if 'csrftoken' in client.cookies:
            # Django 1.6 and up
            csrftoken = client.cookies['csrftoken']
        else:
            # older versions
            csrftoken = client.cookies['csrf']
        payload["csrfmiddlewaretoken"] = csrftoken

    payload["format"] = format

    offset = 0
    numFound = 10000
    all_files = []
    files_type = files_type.upper()
    while offset < numFound:
        payload["offset"] = offset
        url_keys = []
        for k in payload:
            url_keys += ["{}={}".format(k, payload[k])]

        url = "{}/?{}".format(server, "&".join(url_keys))
        print(url)
        r = client.get(url)
        r.raise_for_status()
        resp = r.json()["response"]
        numFound = int(resp["numFound"])
        resp = resp["docs"]
        offset += len(resp)
        for d in resp:
            if verbose:
                for k in d:
                    print("{}: {}".format(k,d[k]))
            url = d["url"]
            for f in d["url"]:
                sp = f.split("|")
                if sp[-1] == files_type:
                    all_files.append(sp[0].split(".html")[0])
        print(len(all_files))
    print(all_files)
    if(all_files == []):
       raise IndexError
    ds = xr.open_mfdataset(all_files, combine='by_coords',engine='netcdf4')
    return ds[search['variable_id']]

# def get_esgf_data(variable, experiment, ensemble_member):
#   """
#   Inspired by https://github.com/rabernat/pangeo_esgf_demo/blob/master/narr_noaa_thredds.ipynb
#   """

#   # Get the relevant catalog references
#   cat_refs = list({k:v for k,v in full_catalog.catalog_refs.items() if k.startswith(f"CMIP6.{get_MIP(experiment)}.NCC.NorESM2-LM.{experiment}.{ensemble_member}.day.{variable}.")}.values()) 
#   # Get the latest version (in case there are multiple)
#   print(cat_refs)
#   cat_ref = sorted(cat_refs, key=lambda x: str(x))[-1]
#   print(cat_ref)
#   sub_cat = cat_ref.follow().datasets
#   datasets = []
#   # Filter and fix the datasets
#   for cds in sub_cat[:]:
#     # Only pull out the (un-aggregated) NetCDF files
#     if (str(cds).endswith('.nc') and ('aggregated' not in str(cds))):
#       # For some reason these OpenDAP Urls are not referred to as Siphon expects...
#       cds.access_urls['OPENDAP'] = cds.access_urls['OpenDAPServer']
#       datasets.append(cds)
#   dsets = [(cds.remote_access(use_xarray=True)
#              .reset_coords(drop=True)
#              .chunk({'time': 365}))
#          for cds in datasets]
#   ds = xr.combine_by_coords(dsets, combine_attrs='drop')
#   return ds[variable]

if __name__ == '__main__':
    xr.set_options(file_cache_maxsize=10,warn_for_unclosed_files=True)
# https://github.com/pydata/xarray/issues/4082#issuecomment-639111588




    # cluster = LocalCluster(n_workers=4, silence_logs=10,threads_per_worker=1)
    # print(cluster)
    # # client = Client(cluster, worker_dashboard_address=':0', dashboard_address=':0', local_directory='/tmp')
    # client = Client(cluster)
    # print(client)
    # print("starting")

    # Cache the full catalogue from NorESG
    # full_catalog = catalog.TDSCatalog('http://noresg.nird.sigma2.no/thredds/catalog/esgcet/catalog.xml')
    # print("Read full catalogue")
    #Loop over experiments and members creating one (annual mean) file with all variables in for each one
    for experiment in experiments:
      # Just take three ensemble members (there are more in the COVID simulation but we don't need them all)
      for i in range(3):
        physics = 2 if experiment == 'ssp245-covid' else 1  # The COVID simulation uses a different physics setup 
        # TODO - check the differences...
        member = f"r{i+1}i1p1f{physics}"
        print(f"Processing {member} of {experiment}...")
        outfile = f"{model}_{experiment}_{member}.nc"
        if (not overwrite) and os.path.isfile(outfile):
          print("File already exists, skipping.")
          continue

        try: 
          tasmin = esgf_search(activity_id=get_MIP(experiment), variable_id='tasmin', table_id='day',experiment_id=experiment,
                   source_id="NorESM2-LM", member_id=member)
          tasmax = esgf_search(activity_id=get_MIP(experiment), variable_id='tasmax', table_id='day',experiment_id=experiment,
                   source_id="NorESM2-LM", member_id=member)
          tas = esgf_search(activity_id=get_MIP(experiment), variable_id='tas', table_id='day',experiment_id=experiment,
                   source_id="NorESM2-LM", member_id=member)
          pr = esgf_search(activity_id=get_MIP(experiment), variable_id='pr', table_id='day',experiment_id=experiment,
                   source_id="NorESM2-LM", member_id=member)
          #.persist()  # Since we need to process it twice
          # pr1 = get_esgf_data('pr', experiment, member)
        except IndexError:
          print("Skipping this realisation as no data present")
          continue
        
        # Derive additional vars
        dtr = tasmax-tasmin
        print(tasmin,tasmax,dtr)
        ds = xr.Dataset({'diurnal_temperature_range': dtr.groupby('time.year').mean('time'),
                         'tas': tas.groupby('time.year').mean('time'),
                         'pr': pr.groupby('time.year').mean('time'),
                         'pr90': pr.groupby('time.year').quantile(0.9, skipna=True)})
        ds.to_netcdf(f"{model}_{experiment}_{member}.nc")

