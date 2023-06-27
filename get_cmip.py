import os



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


def main():
    model = 'NorESM2-LM'
    experiments = [
                '1pctCO2', 'abrupt-4xCO2', 'historical', 'piControl', # CMIP
                'hist-GHG', 'hist-aer', # DAMIP
                'ssp126', 'ssp245', 'ssp370', 'ssp370-lowNTCF', 'ssp585' #	ScenarioMIP
    ]
    variables = ['tas', 'tasmin', 'tasmax', 'pr']
    for var in variables:
        for variant in range(1,3):
            CMIP_TABLE='day'
            CMIP_VARIABLE=var
            CMIP_EXPERIMENT = 'historical'
            CMIP_VARIANT = f'r{variant}i1p1f1'
            CMIP_SOURCE = model
            CMIP_NODE='aims3.llnl.gov'
            wget_string = f'wget http://esgf-node.llnl.gov/esg-search/wget\?project=CMIP6\&experiment_id={CMIP_EXPERIMENT}\&source_id={CMIP_SOURCE}\&variant_label={CMIP_VARIANT}\&table_id={CMIP_TABLE}\&variable_id={CMIP_VARIABLE}'

            print(var,wget_string)
            os.system(wget_string + f' -O wget_{CMIP_VARIABLE}.txt')


            os.system(f'chmod +x wget_{CMIP_VARIABLE}.txt')

            os.system(f'sh wget_{CMIP_VARIABLE}.txt -s')
            # os.system(f'rm {config.CESM_PATH}/wget*.txt')

if __name__ == '__main__':
    main()