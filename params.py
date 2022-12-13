import os

BACTH_SIZE = int(os.environ.get("BATCH_SIZE"))
LOCAL_DATA_PATH = os.path.expanduser(os.environ.get("LOCAL_DATA_PATH"))
PROJECT = os.environ.get("PROJECT")
DATASET = os.environ.get("DATASET")
DATA_STORAGE = os.environ.get("DATA_STORAGE")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
NB_BATCH = int(os.environ.get("NB_BATCH"))
