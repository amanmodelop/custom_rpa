import json
import pandas as pd
import modelop.utils as utils
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
                dict_to_add={"id":id,"fold":fold_name}
                dict_to_add.update(contour_dict[key])
                table.append(dict_to_add)
    return table    
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
def metrics(fold1: pd.DataFrame,fold2: pd.DataFrame,fold3: pd.DataFrame):
    logger.info("Running the metrics function")
    folds=[fold1,fold2,fold3]
    final_table=table_structure(fold1,"fold1")+table_structure(fold2,"fold2")+table_structure(fold3,"fold3")
    yield final_table

#
# This main method is utilized to simulate what the engine will do when calling the above metrics function.  It takes
# the json formatted data, and converts it to a pandas dataframe, then passes this into the metrics function for
# processing.  This is a good way to develop your models to be conformant with the engine in that you can run this
# locally first and ensure the python is behaving correctly before deploying on a ModelOp engine.
#
def main():
	with open("example_model_test_result.json", "r") as f:
		contents = f.read()
	data_dict = json.loads(contents)
	df = pd.DataFrame.from_dict([data_dict])
	print(next(metrics(df)))
	#print(metrics(df))


if __name__ == '__main__':
	main()
