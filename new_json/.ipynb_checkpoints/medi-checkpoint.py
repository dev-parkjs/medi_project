import re
import os
from functools import reduce
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, coalesce, split, regexp_replace, when, length, get_json_object, explode, expr, concat_ws, array_contains,trim,substring,size,regexp_extract,flatten,array

spark = SparkSession.builder \
    .appName("medi_test") \
    .getOrCreate()

root_path = '/Users/parkjisook/Desktop/yeardream/medistream/js'
json_root_path = f'{root_path}/new_json/naverplace_meta'
save_root_path = f'{root_path}/new_json/output'
text_root_path = f'{root_path}/new_json/test.txt'

def save_to_csv(df, name):
    path = os.path.join(save_root_path, name)
    df.coalesce(1).write.mode('append').option("encoding", "utf-8").csv(path, header=True)

chunk_size = 100
def nth_json_path(n):
    return f'/Users/parkjisook/Desktop/yeardream/medistream/js/new_json/naverplace_meta/naverplace_meta_{n}.json'

def read_write_txt():
    with open(text_root_path, 'r') as file:
        lines = file.readlines()
    n = lines.pop(0).strip()
    with open(text_root_path, 'w') as file:
        file.writelines(lines)
    return n

file_path = '/Users/parkjisook/Desktop/yeardream/medistream/js/new_json/test.txt'
n = read_write_txt()
print(nth_json_path(n))
print('n',n)
data = spark.read.json(nth_json_path(n))

df_exploded = data.select(explode("Hospital").alias("HospitalData"))
df_exploded_2 = data.select(explode("ROOT_QUERY").alias("ROOT_QUERY_Data"))

df_result_2 = df_exploded_2.select(
    regexp_extract(col("ROOT_QUERY_Data.hospital.base.__ref"), 'HospitalBase:(\\d+)', 1).alias("id2"),
    col("ROOT_QUERY_Data.hospital.fsasReviews.total").alias("fsas_reviews_count"),
    col("ROOT_QUERY_Data.hospital.kinQna.answerCount").alias("answerCount")
)

df_result = df_exploded.select(
    col("HospitalData.id").alias("id"),
    col("HospitalData.name").alias("name"),
    col("HospitalData.reviewSettings.keyword").alias("review_keyword"),
    col("HospitalData.description").alias("description"),
    col("HospitalData.road").alias("road"),
    col("HospitalData.bookingBusinessId").alias("booking_business_id"),
    col("HospitalData.bookingDisplayName").alias("booking_display_name"),
    col("HospitalData.category").alias("category"),
    col("HospitalData.categoryCode").alias("category_code"),
    col("HospitalData.categoryCount").alias("category_count"),
    col("HospitalData.rcode").alias("rcode"),
    col("HospitalData.virtualPhone").alias("virtual_phone"),
    col("HospitalData.phone").alias("hospital_phone"),
    col("HospitalData.naverBookingUrl").alias("naver_booking_url"),
    col("HospitalData.conveniences").alias("conveniences_str"),
    col("HospitalData.talktalkUrl").alias("talktalk_url"),
    col("HospitalData.roadAddress").alias("road_address"),
    col("HospitalData.keywords").alias("hospital_keywords_str"),
    col("HospitalData.paymentInfo").alias("payment_info_str"),
    col("HospitalData.coordinate.x").alias("lon"),
    col("HospitalData.coordinate.y").alias("lat"),
    col("HospitalData.homepages.etc.url").alias("homepages_url_str"),
    col("HospitalData.homepages.etc.landingUrl").alias("homepages_landingUrl_str"),
    col("HospitalData.homepages.etc.type").alias("homepages_type_str"),
    col("HospitalData.homepages.etc.order").alias("homepages_order_str"),
    col("HospitalData.homepages.etc.isDeadUrl").alias("homepages_isDeadUrl_str"),
    col("HospitalData.homepages.etc.isRep").alias("homepages_isRep_str"),
    col("HospitalData.visitorReviewsTotal").alias("visitor_Reviews_Total"),
    col("HospitalData.visitorReviewsScore").alias("visitor_Reviews_Score"),
    col("HospitalData.visitorReviewsTextReviewTotal").alias("visitor_Reviews_Text_Review_Total"),
    col("HospitalData.images").alias("images")
)

deduplicated_df = df_result.dropDuplicates([
    "id",
    "name",
    "booking_business_id",
    "booking_display_name",
    "category",
    "category_code",
    "category_count",
    "description",
    "road",
    "rcode",
    "virtual_phone",
    "hospital_phone",
    "naver_booking_url",
    "conveniences_str",
    "talktalk_url",
    "road_address",
    "hospital_keywords_str",
    "payment_info_str",
    "lon",
    "lat",
    "homepages_url_str",
    "homepages_landingUrl_str",
    "homepages_type_str",
    "homepages_order_str",
    "homepages_isDeadUrl_str",
    "homepages_isRep_str",
    "visitor_Reviews_Total",
    "visitor_Reviews_Score",
    "visitor_Reviews_Text_ReviewTotal",
    "images"
])

# 열을 콤마로 구분된 문자열로 변환
deduplicated_df = deduplicated_df.withColumn("conveniences", concat_ws(",", "conveniences_str"))
deduplicated_df = deduplicated_df.withColumn("hospital_keywords", concat_ws(",", "hospital_keywords_str"))
deduplicated_df = deduplicated_df.withColumn("payment_info", concat_ws(",", "payment_info_str"))
deduplicated_df = deduplicated_df.withColumn("homepages_url", concat_ws(",", "homepages_url_str"))
deduplicated_df = deduplicated_df.withColumn("homepages_landingUrl", concat_ws(",", "homepages_landingUrl_str"))
deduplicated_df = deduplicated_df.withColumn("homepages_type", concat_ws(",", "homepages_type_str"))
deduplicated_df = deduplicated_df.withColumn("homepages_order", concat_ws(",", "homepages_order_str"))
deduplicated_df = deduplicated_df.withColumn("homepages_isDeadUrl", concat_ws(",", "homepages_isDeadUrl_str"))
deduplicated_df = deduplicated_df.withColumn("homepages_isRep", concat_ws(",", "homepages_isRep_str"))

# 더 이상 필요하지 않은 중간 열 삭제
deduplicated_df = deduplicated_df.drop("conveniences_str", 
                                       "hospital_keywords_str", 
                                       "payment_info_str", 
                                       "homepages_url_str",
                                       "homepages_landingUrl_str",
                                       "homepages_type_str",
                                       "homepages_order_str",
                                       "homepages_isDeadUrl_str",
                                       "homepages_isRep_str")


deduplicated_df = deduplicated_df.withColumn(
    'description',
    regexp_replace(
        trim(
            when(col('description').isNotNull(), col('description'))
        ),
        '\n|\r|\*|,',
        ' '
    )
)

deduplicated_df = deduplicated_df.withColumn(
    'road',
    regexp_replace(
        trim(
            when(col('road').isNotNull(), col('road'))
        ),
        '\n|\r|\*|,',
        ' '
    )
)

deduplicated_df = deduplicated_df.withColumn(
    'review_keyword',
    regexp_replace(
        trim(
            when(col('review_keyword').isNotNull(), col('review_keyword'))
        ),
        r'["\\]',
        ''
    )
)


deduplicated_df = deduplicated_df.withColumn("image_count", size(col("images")))
deduplicated_df = deduplicated_df.drop("images")

#is_smart_phone
deduplicated_df = deduplicated_df.withColumn(
    'is_smart_phone',
    when(
        col('virtual_phone').isNotNull() & col('virtual_phone').startswith('010'),
        True
    ).otherwise(False)
)

# payment_info & zero_pay
deduplicated_df = deduplicated_df.withColumn(
    'zero_pay',
    col('payment_info').contains('제로페이')
)

#is_blog_exposed
deduplicated_df = deduplicated_df.withColumn(
    'is_blog_exposed',
    col('homepages_type').contains('블로그')
)
#keywords
deduplicated_df = deduplicated_df.withColumn(
    'keywords_array',
    split(col('hospital_keywords'), ',')
)

deduplicated_df = deduplicated_df.withColumn(
    'keywords_1', col('keywords_array')[0]
).withColumn(
    'keywords_2', col('keywords_array')[1]
).withColumn(
    'keywords_3', col('keywords_array')[2]
).withColumn(
    'keywords_4', col('keywords_array')[3]
).withColumn(
    'keywords_5', col('keywords_array')[4]
)

deduplicated_df = deduplicated_df.drop('keywords_array')

#hospital_description_length
deduplicated_df = deduplicated_df.withColumn('description_length', length('description'))

#photo_review_ratio
deduplicated_df = deduplicated_df.withColumn(
    'photo_review_ratio',
    (col('visitorReviewsTotal') - col('visitorReviewsTextReviewTotal')) / col('visitorReviewsTotal')
)

joined_df = deduplicated_df.join(df_result_2, deduplicated_df.id == df_result_2.id2, "inner")

selected_columns = [
    "id", 
    "name", 
    "review_keyword",    
    "hospital_keywords",
    "keywords_1", "keywords_2","keywords_3", "keywords_4", "keywords_5", 
    "photo_review_ratio",
    "image_count",
    "naver_booking_url", "talktalk_url",
    "road",
    "road_address", "lon", "lat", 
    "hospital_phone", 
    "virtual_phone","is_smart_phone",
    "homepages_url", "homepages_landingUrl", "homepages_type",
    "homepages_order", "homepages_isDeadUrl", "homepages_isRep", 
    "is_blog_exposed",
    "payment_info",  "zero_pay",
    "conveniences", 
    "description", "description_length",
    "answerCount",
    "booking_business_id",
    "booking_display_name", 
    "category", "category_code", "category_count",
    "rcode", 
    "visitor_Reviews_Total", "visitor_Reviews_Score","visitor_Reviews_Text_Review_Total",
    "fsas_reviews_count"
]
    

result_df = joined_df.select(*selected_columns)



save_path = f'{save_root_path}/naverplace_meta_{n}'

result_df.write.parquet(save_path)