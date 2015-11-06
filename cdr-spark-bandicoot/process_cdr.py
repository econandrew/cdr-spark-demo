import sys
import csv
import StringIO

from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.types import *

SAMPLE = None
COALESCE = None
REPORT_SIZES = False

# List of file columns - actually the first column is the user_id but it's treated separately
HEADERS = ('interaction','direction','correspondent_id','datetime','call_duration','antenna_id')

METRIC_FIELDS = set([
    "active_days__allweek__allday__callandtext__mean","active_days__allweek__allday__callandtext__std",
    "balance_of_contacts__allweek__allday__call__mean__mean","balance_of_contacts__allweek__allday__call__mean__std",
    "balance_of_contacts__allweek__allday__call__std__mean","balance_of_contacts__allweek__allday__call__std__std",
    "call_duration__allweek__allday__call__mean__mean","call_duration__allweek__allday__call__mean__std",
    "call_duration__allweek__allday__call__std__mean","call_duration__allweek__allday__call__std__std",
    "churn_rate__mean","churn_rate__std","entropy_of_antennas__allweek__allday__mean",
    "entropy_of_antennas__allweek__allday__std","entropy_of_contacts__allweek__allday__call__mean",
    "entropy_of_contacts__allweek__allday__call__std","frequent_antennas__allweek__allday__mean",
    "frequent_antennas__allweek__allday__std","interactions_per_contact__allweek__allday__call__mean__mean",
    "interactions_per_contact__allweek__allday__call__mean__std",
    "interactions_per_contact__allweek__allday__call__std__mean",
    "interactions_per_contact__allweek__allday__call__std__std","interevent_time__allweek__allday__call__mean__mean",
    "interevent_time__allweek__allday__call__mean__std","interevent_time__allweek__allday__call__std__mean",
    "interevent_time__allweek__allday__call__std__std","number_of_antennas__allweek__allday__mean",
    "number_of_antennas__allweek__allday__std","number_of_contacts__allweek__allday__call__mean",
    "number_of_contacts__allweek__allday__call__std","number_of_interaction_in__allweek__allday__call__mean",
    "number_of_interaction_in__allweek__allday__call__std","number_of_interaction_out__allweek__allday__call__mean",
    "number_of_interaction_out__allweek__allday__call__std","number_of_interactions__allweek__allday__call__mean",
    "number_of_interactions__allweek__allday__call__std","percent_at_home__allweek__allday__mean",
    "percent_at_home__allweek__allday__std","percent_initiated_interactions__allweek__allday__call__mean",
    "percent_initiated_interactions__allweek__allday__call__std","percent_nocturnal__allweek__allday__call__mean",
    "percent_nocturnal__allweek__allday__call__std","percent_pareto_durations__allweek__allday__call__mean",
    "percent_pareto_durations__allweek__allday__call__std","percent_pareto_interactions__allweek__allday__call__mean",
    "percent_pareto_interactions__allweek__allday__call__std","radius_of_gyration__allweek__allday__mean",
    "radius_of_gyration__allweek__allday__std","reporting__number_of_records"
])

# CONFIGURATION ###############################################################
conf = SparkConf().setMaster("yarn-client") \
                  .setAppName("ProcessCDR") \
                  .set("spark.speculation", True) \
                  .set("spark.scheduler.executorTaskBlacklistTime", 5*60*1000) \
                  .set("spark.akka.frameSize", 30)
                  
# Need to send the bandicoot library to all nodes
sc = SparkContext(conf=conf, pyFiles=['lib/bandicoot-0.4.0-py2.6.egg', 'code/counter.py'])
sqlContext = SQLContext(sc)

# BANDICOOT IMPORTS ###########################################################
import bandicoot.io
import bandicoot.individual
import bandicoot.utils

# HELPERS #####################################################################

# The following four functions are used to do map reduce keyed averages - we
# have to accumulate both the sum and the count, and then finally divide them
# out to get averages

# Converts {k: v} -> {k: (v, 1)} for counting or {k: None} -> {k: (0, 0)}
def avg_augment(d):
    return dict([(k, (v if v else 0, 1 if v else 0)) for k,v in d.iteritems()])

# Add the elements of two lists
def add_elements(l1, l2):
    return map(sum, zip(l1, l2))
    
# Combines {k: (v, 1)} and {k: (w, 1)} to {k: (v+w, 1+1)}
def avg_reduce(d1, d2):
    out = dict()
    for k in d1:
        out[k] = add_elements(d1[k], d2[k])
    return out

# Combines {k: (v, c)} -> {k: v/c)} or {k: (v, 0)} -> {k: None}
def avg_calc(d):
    return dict([(k, (float(v[0])/v[1] if v[1] else None)) for k,v in d.iteritems()])

# Return the head and tail of a list
def decapitate(l):
    return l[0], l[1:]

# BANDICOOT FUNCTIONS #########################################################
    
# Convert a line of the CSV to a dict with headers
def line2dict(l):
    return dict(zip(HEADERS, l))
    
# Load the lines: [lines] -> [dicts] -> [records]
# TODO: don't ignore bad_records
def load_lines(user_id, lines, antennas):
    records = map(bandicoot.io._parse_record, map(line2dict, lines))
    user, bad_records = bandicoot.io.load(user_id, records, antennas)
    return user
    
# Load antennas by hand
def load_antennas(antennas_path):
    if antennas_path is not None:
        with open(antennas_path, 'rb') as csv_file:
            reader = csv.DictReader(csv_file)
            antennas = dict((d['place_id'], (float(d['latitude']),
                                             float(d['longitude'])))
                            for d in reader)
        return antennas

# MAIN PROCESSING #############################################################

fn_in_cdr_lines = sys.argv[1]
fn_in_antennas = sys.argv[2]
fn_out_sizes = sys.argv[3]
fn_out_metrics = sys.argv[4]
fn_out_metrics_header = sys.argv[5]

# Load the antennas on the driver - it's a small file so we don't worry about
# spark broadcasting it
antennas = load_antennas(fn_in_antennas)

# Load the lines into Spark
lines = sc.textFile(fn_in_cdr_lines)
    
if SAMPLE: # If SAMPLE is a float we take a random % of records
    lines = lines.sample(True, SAMPLE, 0)
    
if COALESCE: # Experimental, to see if changing # of partitions helps
    lines = lines.coalesce(COALESCE)
    
user_keyed_lines = lines.map(lambda l: decapitate(l.split(",")))

# Collect the counts per user, possibly output
user_lines_count = user_keyed_lines.map(lambda (k,v): (k, 1)).reduceByKey(lambda a,b: a+b)
if REPORT_SIZES: # If this, then just report the counts 
    user_lines_count \
        .map(lambda (k, count): "{0},{1}".format(k,count)) \
        .saveAsTextFile(fn_out_sizes)
    sys.exit(0)

# Filter out the biggest users - they overwhelm the executor they end up on
# TODO: be smarter about this
big_users = user_lines_count \
    .filter(lambda (k, count): count > 20000) \
    .map(lambda (k, count): k).collect()
    
user_keyed_lines = user_keyed_lines.filter(lambda (k, line): k not in big_users)    

# SHUFFLE
# Rearrange so that all of a user's lines are on a single node (in single executor)
user_lines = user_keyed_lines.groupByKey()

# Load each user's lines into Bandicoot and create user records
users = user_lines.map(lambda (k, lines): load_lines(k, lines, antennas))

# Created flattened statistics for each user - only include those in METRIC_FIELDS
metrics_dicts = users.map(
    lambda u: (u, bandicoot.utils.flatten(bandicoot.utils.all(u)))
)
metrics_dicts = metrics_dicts.map(
    lambda (u, metrics): (u, dict([(k, v) for k, v in metrics.iteritems() if k in METRIC_FIELDS]))
)

# Rekey data to user home if they have one, else drop
metrics_homed = metrics_dicts \
    .filter(lambda (u, metrics): u.home and u.home.location) \
    .map(lambda (u, metrics): (u.home.location, metrics))

# Calculate means of all metrics, plus count - this a mess now
metrics_dicts = metrics_homed \
    .map(lambda (home, metrics): (home, (1, avg_augment(metrics)))) \
    .reduceByKey(lambda (count1, m1), (count2, m2): ((count1+count2), avg_reduce(m1, m2))) \
    .map(lambda (home, (count, aug_metrics)): (home, (count,avg_calc(aug_metrics))))

# OUTPUT DATA #################################################################
   
# Extract headers and sort
metrics_head = metrics_dicts.first()[1][1].keys()
metrics_head.sort()

# Save headers
sc.parallelize([','.join(['lat','lon','count']+metrics_head)], 1) \
    .saveAsTextFile(fn_out_metrics_header)

# Output data using databricks CSV - this may be overkill but saveAsTextFile was causing problems at one point
metrics_list = metrics_dicts.map(lambda (h, (c,d)): [str(h[0]),str(h[1]),str(c)]+[str(d.get(k, "")) for k in metrics_head])

fields = [StructField(field_name, StringType(), True) for field_name in ['lat','lon','count']+metrics_head]
schema = StructType(fields)

# Apply the schema to the RDD.
metrics_frame = sqlContext.createDataFrame(metrics_list, schema)
metrics_frame.save(fn_out_metrics, "com.databricks.spark.csv")