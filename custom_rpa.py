import json
import pandas as pd
import modelop.utils as utils
from pathlib import Path

import re,os

logger = utils.configure_logger()

#
# This method gets called when the monitor is loaded by the ModelOp runtime. It sets the GLOBAL values that are
# extracted from the report.txt to obtain the DTS and version info to append to the report
#
def table_structure(f1:pd.DataFrame,fold_name):
    table=[]
    res=f1.loc[f1.index[0]].results
    for contour_dict in res:
        for key,val in contour_dict.items():
            ref_link=contour_dict["test"]
            m=re.search(r'OPX\w*',ref_link)
            id=m.group()
            if key not in {"reference","test"}:
                dict_to_add={"id":id,"fold":fold_name,"contour":key}
                dict_to_add.update(contour_dict[key])
                table.append(dict_to_add)
    return table  

def data_frame_per_fold(f1,fold_name): #f1 is output from metrics for the given fold
    df_fold=pd.DataFrame()
    res1=f1.loc[f1.index[0]].results
    for contour_dict in res1:
        for key,val in contour_dict.items():
            ref_link=contour_dict["test"]
            m=re.search(r'OPX\w*',ref_link)
            id=m.group()
            if key not in {"reference","test"}:
                contour=key
                for metric,value in val.items():
                    val[metric]=[value]
                #data={"Id":id,"Contour":contour,"fold_name":fold_name,"metrics":val}
                keys_to_be_added=["Id","Contour","fold_name"]
                dict_fold={"Id":id,"Contour":contour,"fold_name":fold_name}
                dict_fold.update(val)
                df_fold=pd.concat([df_fold,pd.DataFrame(dict_fold)],ignore_index=True)
    return df_fold  
# modelop.init
def init(init_param):
	logger = utils.configure_logger()

#
# This method is the modelops metrics method.  This is always called with a pandas dataframe that is arraylike, and
# contains individual rows represented in a dataframe format that is representative of all of the data that comes in
# as the results of the first input asset on the job.  This method will not be invoked until all data has been read
# from that input asset.
#
# For this example, we simply echo back the first row of that data as a json object.  This is useful for things like
# reading externally generated metrics from an SQL database or an S3 file and having them interpreted as a model test
# result for the association of these results with a model snapshot.
#
# data - The input data of the first input asset of the job, as a pandas dataframe
#

# modelop.metrics
def metrics(fold0: pd.DataFrame,fold1: pd.DataFrame,fold2: pd.DataFrame,fold3: pd.DataFrame,fold4: pd.DataFrame):
    logger.info("Running the metrics function")
    folds=[fold0,fold1,fold2,fold3,fold4]
    fold_names=["fold0","fold1","fold2","fold3","fold4"]
    table=table_structure(fold0,fold_names[0])+table_structure(fold1,fold_names[1])+table_structure(fold2,fold_names[2])+table_structure(fold3,fold_names[3])+table_structure(fold4,fold_names[4])

    df0=data_frame_per_fold(fold0,fold_names[0])
    df1=data_frame_per_fold(fold1,fold_names[1])
    df2=data_frame_per_fold(fold2,fold_names[2])
    df3=data_frame_per_fold(fold3,fold_names[3])
    df4=data_frame_per_fold(fold4,fold_names[4])


    df=pd.concat([df0,df1,df2,df3,df4])
    df=df.rename(columns=lambda col:f'mean_{col}')
    avg=df.iloc[:,3:].mean(axis=0)
    avg_metrics=avg.to_dict()
    final_table=avg_metrics
    final_table["rpa table"]=table

    yield final_table

#
# This main method is utilized to simulate what the engine will do when calling the above metrics function.  It takes
# the json formatted data, and converts it to a pandas dataframe, then passes this into the metrics function for
# processing.  This is a good way to develop your models to be conformant with the engine in that you can run this
# locally first and ensure the python is behaving correctly before deploying on a ModelOp engine.
#
def main():
    raw_json=Path('example_job.json').read_text()
    init_param={'rawJson':raw_json}
    init(init_param)
    df1 = pd.read_json("summary_test.json")
    #df1 = pd.DataFrame.from_dict([data])
    df2=df1
    df3=df2
    print(json.dumps(metrics(df1,df2,df3)))


if __name__ == '__main__':
	main()
