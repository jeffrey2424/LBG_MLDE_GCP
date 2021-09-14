# python -m apache_beam_demo

from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
import json


def print_metrics(obj):
    print("\n")
    print(obj)
    print(type(obj))
    print(len(obj))  
    return obj


def json_to_tuple(json):
    return json["name"], json["val"]


def text_to_json(json_text):
    return json.loads(json_text)


input_file = './input_data_reformat.json'
output_file = './output_data'

pipelineOptions = PipelineOptions(
    runner='DirectRunner',
    project='Demo_Project',
    job_name='Demo_Job',
)


print("\nRunning Pipeline\n\n")

with beam.Pipeline(options=pipelineOptions) as demoPipeline:
    input_data = ( demoPipeline
                  | "readFromFile" >> ReadFromText(input_file)
                  | "textToJson" >> beam.Map(text_to_json)
                  | "jsonToTuple" >> beam.Map(json_to_tuple)
                  | "groupByKey" >> beam.GroupByKey()
                  | "printMetrics" >> beam.Map(print_metrics)
                  | "writeToFile" >> WriteToText(output_file,
                                                         file_name_suffix=".json",
                                                         shard_name_template=''
                                                         )
                  )
