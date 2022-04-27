import apache_beam as beam

def run(project, bucket):
    argv = [
        '--project={0}'.format(project),
        '--job_name=examplejob',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(bucket),
        '--temp_location=gs://{0}/temp/'.format(bucket),
        '--region=us-central1',
        '--runner=DataflowRunner'
    ]
    
    file_name1 = 'gs://{}/input/periodic_table.csv'.format(bucket)
    
    file_out1 = 'gs://{}/out'.format(bucket)
    
    with beam.Pipeline(argv=argv) as pipeline:
        input_pc = (pipeline
                    | 'GCS_Read' >> beam.io.ReadFromText(file_name1,skip_header_lines=1)
                    )
                    
        (input_pc
         |'GCS_Write' >> beam.io.WriteToText(file_out1)
         )
         
        BQ_table_Schema = ','.join(['AtomicNumber:numeric','Element:string','Symbol:string','AtomicMass:float','NumberofNeutrons:numeric','NumberofProtons:numeric','NumberofElectrons:numeric',
            'Phase:string','Natural:string','Metal:string','Nonmetal:string','Metalloid:string','Type:string','Discoverer:string','Year:numeric'])
        
        
        (input_pc
         | 'BQ_Write' >> beam.io.WriteToBigQuery(
                    'test_dataset.table1', schema=BQ_table_Schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                    )
         )


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Run simple io pipeline on the cloud')
    parser.add_argument('-p', '--project', help='Unique project ID', required=True)
    parser.add_argument('-b', '--bucket', help='Bucket where input file exists', required=True)

    args = vars(parser.parse_args())

    print("Dataflow Runner is Starting")

    run(project=args['project'], bucket=args['bucket'])
    
        print("Dataflow Runner completed")
