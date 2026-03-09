try:
    import boto3
    import botocore
    print("BOTO3_OK", boto3.__version__)
except Exception as e:
    print("BOTO3_FAIL", type(e).__name__, str(e))
