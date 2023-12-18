### !/bin/bash
FILE_PATH="./queue.txt"
QUEUEMANAGER_PATH="./queue-manager.py"
PREPROCESSOR_PATH="./pyspark-preprocessor.py"
MERGEMANAGER_PATH="./merge-manager.py"

# ## create a queue text file
# python $QUEUEMANAGER_PATH

# ### unset envs
# unset PYSPARK_DRIVER_PYTHON
# unset PYSPARK_DRIVER_PYTHON_OPTS

# ### spark-submit
# count=0
# while [ -f $FILE_PATH ]
# do
#     if ! cat $FILE_PATH | grep -q '[^[:space:]]'; then
#         echo "a queue text file is empty."
#         break
#     fi
#     count=$((count+1))
#     echo "Job: $count"
#     spark-submit \
#         --conf "spark.driver.bindAddress=127.0.0.1" \
#         --master local[*] \
#         $PREPROCESSOR_PATH
# done

### merge csvs
spark-submit \
        --conf "spark.driver.bindAddress=127.0.0.1" \
        --master local[*] \
        $MERGEMANAGER_PATH