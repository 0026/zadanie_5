import os

def get_files_size(partition_data, path_var):
    total = 0
    for element in partition_data:
        for entry in os.scandir(path_var + os.sep + element):
             total += entry.stat(follow_symlinks=False).st_size
    yield total


path_rdd = sc.parallelize(["folder1","folder2","folder4","folder4"])
path_var = sc.broadcast("path/path/path")

size = path_rdd.mapPartitions(lambda partition: get_files_size(partition, path_var)).sum()

print(size)