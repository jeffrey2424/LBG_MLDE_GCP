# python -m apache_beam_demo


from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms.sql import SqlTransform

import json
import datetime




def print_metrics(obj, desc):
    print("\n")
    print(desc)
    print(obj)
    print(type(obj))
    print(datetime.datetime.now()) 
    return obj


def json_to_tuple(json):
    return json["name"], json["val"]


def val_to_int(tuple_in):
    return tuple_in[0], int(tuple_in[1])


def text_to_json(json_text):
    return json.loads(json_text)


input_file_1 = './input_data_reformat.json'
input_file_2 = './input_data_reformat_join.json'
output_file = './output_data'
agg_type = "sum"

pipelineOptions = PipelineOptions(
    runner='DirectRunner',
    project='Demo_Project',
    job_name='Demo_Job',
)


print("\nRunning Pipeline\n\n")


#with beam.Pipeline(options=pipelineOptions) as demoPipeline:
#    input_data = ( demoPipeline
#                  | "readFromFile" >> ReadFromText(input_file)
#                  | "textToJson" >> beam.Map(text_to_json)
#                  | "jsonToTuple" >> beam.Map(json_to_tuple)
#                  | "printMetrics1" >> beam.Map(print_metrics, "Pre aggregation")
#                  | ( 'Sum values per key' >> beam.CombinePerKey(sum)
#                    if agg_type == "sum" else 
#                      'Get mean value per key' >> beam.combiners.Mean.PerKey()
#                    )
#                  | "printMetrics2" >> beam.Map(print_metrics, "Post aggregation")
#                  | "writeToFile" >> WriteToText(output_file,
#                                                         file_name_suffix=".json",
#                                                         shard_name_template=''
#                                                         )
#                  )



with beam.Pipeline(options=pipelineOptions) as demoPipeline:
    animal_data_1 = ( demoPipeline
                  | "readFromFile_1" >> ReadFromText(input_file_1)
                  | "textToJson_1" >> beam.Map(text_to_json)
                  | "jsonToTuple_1" >> beam.Map(json_to_tuple)
                  | "valToInt_1" >> beam.Map(val_to_int)
#                  | "printMetrics_1" >> beam.Map(print_metrics, "Post aggregation 1")
                  )
    
    animal_data_2 = ( demoPipeline
                  | "readFromFile_2" >> ReadFromText(input_file_2)
                  | "textToJson_2" >> beam.Map(text_to_json)
                  | "jsonToTuple_2" >> beam.Map(json_to_tuple)
#                  | "printMetrics_2" >> beam.Map(print_metrics, "Post aggregation 2")
                  )
    
    animal_data_aggragation = ( animal_data_1
                  | ( 'Sum values per key' >> beam.CombinePerKey(sum)
                    if agg_type == "sum" else 
                      'Get mean value per key' >> beam.combiners.Mean.PerKey()
                    )
#                  | "printMetrics_agg" >> beam.Map(print_metrics, "Post aggregation 3")
            )
                  
    animal_data_join = (({"data_1": animal_data_1, "data_2": animal_data_2})
                  | "coGroupByKey" >> beam.CoGroupByKey()
#                  | "printMetrics_join" >> beam.Map(print_metrics, "Post aggregation 4")
                  | "writeToFile" >> WriteToText(output_file,
                                                         file_name_suffix=".json",
                                                         shard_name_template=''
                                                         )
            )
    animal_sql = ( demoPipeline
                  | "readFromFile_sql" >> ReadFromText(input_file_1)
                  | "textToJson_sql" >> beam.Map(text_to_json)
                  | "rowTransform" >> beam.Map(lambda x: beam.Row(name=str(x["name"]), val=int(x["val"])))
                  | "sqlTransform" >> SqlTransform("""SELECT name from PCOLLECTION""")
                  
                  )
                  
    
    
                  
                  
                  
                  
                  
                  
                  
                  
                  
                  
                  
                  
                  
                  
                  
                  
                  
                  
                  
                  