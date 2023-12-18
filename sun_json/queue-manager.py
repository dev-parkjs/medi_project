### import os
import os

### set paths
root = '/Users/parkjisook/Desktop/yeardream/medistream/js/sun_json'
path = f'{root}/queue.txt'
size = len(os.listdir(f'{root}/naverplace_meta/'))
idxs = [i for i in range(1, size+1)]

### create a queue txt file
print(path, size, idxs)
with open(path, 'w') as file:
    for idx in idxs:
        file.write(str(idx)+'\n')
print('created a queue text file')