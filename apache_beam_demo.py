# python -m apache_beam_demo

from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam

input_file = './input_data.json'
output_file = './output_data.json'

pipelineOptions = PipelineOptions(
    runner='DirectRunner',
    project='Demo_Project',
    job_name='Demo_Job',
)

with beam.Pipeline(options=pipelineOptions) as demoPipeline:
    input_data = ( demoPipeline
                  | "readFromFile" >> beam.io.ReadFromText(input_file)
                  | "writeToFile" >> beam.io.WriteToText(output_file)
                  )
