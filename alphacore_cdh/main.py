from pandas import DataFrame, read_parquet, concat
import tabulate
import os
import requests
from awswrangler import s3, athena, exceptions
from alphacore_cdh.services.alphacore import Alphacore
from alphacore_cdh.config import Config

Config.init()
alphacore = Alphacore()


def get_auctions_id_list(platform, product_type) -> list:
  id_list = []
  try:
    df = DataFrame(alphacore.get_auctions(platform, product_type, "FINISHED"))
    df_filtered = df.loc[df["is_result"] == True]
    id_list = df_filtered.id.values.tolist()
  except AttributeError:
    id_list = []
  except KeyError:
    print(f'''Empty auctions_id_list, therefore no 'is_result' column to filter on... ''')
  except Exception as e:
    print(f'''Caught following exception: {e}''')
  return id_list

def get_auctions_results_df(auctions_id_list, get_results_function) -> DataFrame:
  complete_df = DataFrame()
  for auction_id in auctions_id_list:
    regel_fcr_results = get_results_function(auction_id)
    if regel_fcr_results is not None:
      df = DataFrame(regel_fcr_results)
      complete_df = concat([complete_df, df])

  return complete_df

#for athena access, add 'database' and 'table' parameters to to_parquet()
def write(df, folder):
  path = "s3://cdh-aymanecdhtestdatasource-337381/first_dataset/"
  try:

    s3.to_parquet(
        df=df,
        path=path + folder,
        mode='append',
        dataset=True,
        use_threads=True,
    )
    print(f'''Successfully persisted dataframe as parquet file in following s3 directory: '{path}{folder}' ''')
  except exceptions.EmptyDataFrame:
    print(f'''ERROR: Unable to persist empty dataframe! ''')
  except Exception as e:
    print(f'''Caught following exception: {e}''')

rte_afrr_auctions_id_list = get_auctions_id_list("RTE", "AFRR")
rte_mfrr_auctions_id_list = get_auctions_id_list("RTE", "MFRR")
rte_rr_auctions_id_list = get_auctions_id_list("RTE", "RR")

regel_afrr_auctions_id_list = get_auctions_id_list("REGEL", "AFRR")
regel_fcr_auctions_id_list = get_auctions_id_list("REGEL", "FCR")
regel_mfrr_auctions_id_list = get_auctions_id_list("REGEL", "MFRR")

rte_afrr_auctions_results_df = get_auctions_results_df(rte_afrr_auctions_id_list, alphacore.get_rte_afrr_full_results) #automated frequency r
rte_mfrr_auctions_results_df = get_auctions_results_df(rte_mfrr_auctions_id_list, alphacore.get_rte_mfrr_results)
rte_rr_auctions_results_df = get_auctions_results_df(rte_rr_auctions_id_list, alphacore.get_rte_rr_results)

regel_afrr_auctions_results_df = get_auctions_results_df(regel_afrr_auctions_id_list, alphacore.get_regel_afrr_results)
regel_fcr_auctions_results_df = get_auctions_results_df(regel_fcr_auctions_id_list, alphacore.get_regel_fcr_results)
regel_mfrr_auctions_results_df = get_auctions_results_df(regel_mfrr_auctions_id_list, alphacore.get_regel_mfrr_results)


print(rte_afrr_auctions_results_df) #empty
print(rte_mfrr_auctions_results_df)
print(rte_rr_auctions_results_df) #empty

print(regel_afrr_auctions_results_df)
print(regel_fcr_auctions_results_df)
print(regel_mfrr_auctions_results_df)

#############################
#to do:
# use boto3 python library to write these dataframes to CDH
# will need token from Bertrand to do the writing


write(rte_afrr_auctions_results_df, "rte/afrr/")
write(rte_mfrr_auctions_results_df, "rte/mfrr/")
write(rte_rr_auctions_results_df, "rte/rr/")

write(regel_afrr_auctions_results_df, "regel/afrr/")
write(regel_fcr_auctions_results_df, "regel/fcr/")
write(regel_mfrr_auctions_results_df, "regel/mfrr/")




