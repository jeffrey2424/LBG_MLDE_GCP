# python -m apache_beam_demo

from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam


input_file = './input_data.json'
output_file = './output_data'

pipelineOptions = PipelineOptions(
    runner='DirectRunner',
    project='Demo_Project',
    job_name='Demo_Job',
)

print("Test print")

with beam.Pipeline(options=pipelineOptions) as demoPipeline:
    input_data = ( demoPipeline
                  | "readFromFile" >> beam.io.ReadFromText(input_file)
                  # | "Map" >> beam.Map(lambda element: (element[0], element[1]))
                  # | "GroupByKey" >> beam.GroupByKey()
                  # | "mapTuple" >> beam.MapTuple(lambda item: '{}{}'.format(item[0], item[1]))
                  | "writeToFile" >> beam.io.WriteToText(output_file,
                                                         file_name_suffix=".json",
                                                         shard_name_template=''
                                                         )
                  )
