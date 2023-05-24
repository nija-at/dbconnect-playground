from databricks.sdk.dbutils import RemoteDbUtils
from random import randint

utils = RemoteDbUtils()
fs = utils.fs

path = "/tmp/dbc_qa_file{}".format(randint(0, 1000))
print(f"file name {path}")

try:
    fs.put(path, "test", True)

    output = fs.head(path)

    print(output)
finally:
    fs.rm(path)