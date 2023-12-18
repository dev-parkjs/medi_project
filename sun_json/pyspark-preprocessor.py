### import libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType
from pyspark.sql.functions import array, array_contains, col, concat_ws, explode, flatten, length, regexp_extract, regexp_replace, size, when

### create spark session
spark = SparkSession.builder \
    .appName("medi05_pyspark") \
    .getOrCreate()

### set paths
root_path = '/Users/parkjisook/Desktop/yeardream/medistream/js/sun_json'
text_path = f'{root_path}/queue.txt'
json_root_path = f'{root_path}/naverplace_meta'

### read data
with open(text_path, 'r') as t: 
    l = t.readlines()                
n = l.pop(0).strip()
with open(text_path, 'w') as t: 
    t.writelines(l)
json_path = f'{json_root_path}/naverplace_meta_{n}.json'
data = spark.read.option("multiline", "true").json(json_path)

### preprocessing hospital data
hospital_data = data.select(explode("hospital").alias("h"))
hospital_df = hospital_data.select(
    col("h.id").alias("id"),
    col("h.name").alias("name"),
    col("h.category").alias("category"),
    col("h.category_code").alias("category_code"),
    col("h.category_code_list").alias("category_code_list"),
    col("h.category_count").alias("category_count"),
    col("h.description").alias("description"),
    col("h.road_address").alias("road_address"),
    col("h.road").alias("road"),
    col("h.rcode").alias("rcode"),
    col("h.virtual_phone").alias("virtual_phone"),
    col("h.phone").alias("phone"),
    col("h.payment_info").alias("payment_info"),
    col("h.conveniences").alias("conveniences"),
    col("h.review_setting.keyword").alias("review_keyword"),
    col("h.keywords").alias("keywords"),
    col("h.booking_business_id").alias("booking_business_id"),
    col("h.booking_display_name").alias("booking_display_name"),
    col("h.visitor_reviews_score").alias("visitor_reviews_score"),
    col("h.visitor_reviews_total").alias("visitor_reviews_total"),
    col("h.visitor_reviews_text_review_total").alias("visitor_reviews_text_review_total"),
    col("h.images").alias("images"),
    col("h.homepages.etc").alias("homepages_etc"),
    col("h.homepages.repr").alias("homepages_repr"),
    col("h.homepages.repr.url").alias("is_rep"), # isRep?
    col("h.booking_url").alias("booking_url"),
    col("h.talktalk_url").alias("talktalk_url"),
    col("h.coordinate.x").alias("lon"),
    col("h.coordinate.y").alias("lat"),
)

# regexp_replace
hospital_df = hospital_df.withColumn(
    "description",
    regexp_replace("description", "[\n\r*,]", "")
).withColumn(
    "road",
    regexp_replace("road", "[\n\r*,]", "")
).withColumn(
    "review_keyword",
    regexp_replace("review_keyword", "[\\\"]", "")
)

# etc
hospital_df = hospital_df.withColumn(
    "description_length",
    length("description")
).withColumn(
    "images_count", 
    size("images")
).withColumn(
    'photo_review_ratio',
    (col('visitor_reviews_total')-col('visitor_reviews_text_review_total'))/col('visitor_reviews_total')
).withColumn(
    'homepages_url', 
    flatten(array(array('homepages_repr.url'), 'homepages_etc.url'))
).withColumn(
    'homepages_type', 
    flatten(array(array('homepages_repr.type'), 'homepages_etc.type'))
).withColumn(
    'homepages_order', 
    when(
        col('homepages_repr.order').isNull(), 0
    ).otherwise(
        size(flatten(array(array('homepages_repr.order'), 'homepages_etc.order')))
    )
).withColumn(
    'is_smart_phone',
    col('phone').startswith('010')
).withColumn(
    'is_zero_pay',
    array_contains(col('payment_info'), '제로페이')
).withColumn(
    'isDeadUrl',
    flatten(array(array('homepages_repr.isDeadUrl'), 'homepages_etc.isDeadUrl'))
).withColumn(
    'keywords_1',
    col('keywords')[0]
).withColumn(
    'keywords_2',
    col('keywords')[1]
).withColumn(
    'keywords_3',
    col('keywords')[2]
).withColumn(
    'keywords_4',
    col('keywords')[3]
).withColumn(
    'keywords_5',
    col('keywords')[4]
)
hospital_df = hospital_df.drop("images", "keywords", "homepages_repr", "homepages_etc")

# concat_ws
arr_col_lst = [field.name for field in hospital_df.schema.fields if isinstance(field.dataType, ArrayType)]
for arr_col in arr_col_lst:
    hospital_df = hospital_df.withColumn(arr_col, concat_ws(",", arr_col))

### preprocessing root data
root_data = data.select(explode("root").alias("r"))
root_df = root_data.select(
    col("r.hospital.base.__ref").alias("root_id"),
    col("r.hospital.fsasReviews.total").alias("fsas_reviews_count"),
    col("r.hospital.kinQna.answerCount").alias("kin_qna_count")
)
root_df = root_df.withColumn(
    "root_id",
    regexp_extract("root_id", "HospitalBase:([\\w]+)", 1)
)

### join
df = hospital_df.join(root_df, hospital_df.id == root_df.root_id, "left_outer")
df = df.drop("root_id")

### save
save_path = '/Users/parkjisook/Desktop/yeardream/medistream/js/sun_json/output'
df.coalesce(1).write.mode('append').option("encoding", "utf-8").csv(save_path, header=True)
# df.write.parquet(save_path)

### close spark session
spark.stop()