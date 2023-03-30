#import os
import sys
import pandas as pd
from datetime import datetime
#import StringIO
import argparse
import traceback
from argparse import RawTextHelpFormatter
import boto3
from io import BytesIO, StringIO


def parse_args(argv):
    arg_parser = argparse.ArgumentParser(description='''Input Parameters''', formatter_class=RawTextHelpFormatter)
    arg_parser.add_argument('--filename', required=True, default=None, type=str, help='Name of the above file')
    arg_parser.add_argument('--raw_bucket_name', required=True, default=None, type=str, help='Raw Bucket name')
    arg_parser.add_argument('--landing_bucket_name', required=True, default=None, type=str, help='Landing Bucket name')

    try:
        serviceArguments = arg_parser.parse_args(args=argv[1:])
        print(serviceArguments)

        # Call the main script
        serviceArguments = vars(serviceArguments)
        print(serviceArguments)
        response_code = main(serviceArguments)
        return response_code
    except:
        arg_parser.print_help()
        sys.exit(-1)

        
def main(serviceArguments):

    try:

        if len(serviceArguments) == 3:
            print("started")
            s3_resource = boto3.resource('s3')
            
            fileKey = str(serviceArguments['filename'])
            raw_bucket_name = str(serviceArguments['raw_bucket_name'])
            landing_bucket_name = str(serviceArguments['landing_bucket_name'])
            
            print("reading file")
            obj = s3_resource.Object(raw_bucket_name, fileKey)
            data = obj.get()['Body'].read()
            data = BytesIO(data)            
            df = pd.read_csv(data)
            print("file read")
            
            df.drop(columns = ['Unnamed: 0'],inplace=True)
            df.columns = df.columns.str.replace(" ","_")
            print(df.columns)
            print("file processed")
            
            return_buffer = BytesIO()
            df.to_csv(return_buffer, index=False)
            ret_response = s3_resource.Object(landing_bucket_name, fileKey).put(Body=return_buffer.getvalue(), ServerSideEncryption = 'AES256')
            
            return 0
        else:
            raise ValueError('Expecting 1 argument')

    except Exception as e:
        traceback.print_exc()
        return -1

if __name__ == '__main__':
    sys.exit(parse_args(sys.argv))
    
    
