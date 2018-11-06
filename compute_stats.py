from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local").appName("distinct counts").getOrCreate()
inp=spark.read.csv('sampledata.txt',sep='|',header=False,dateFormat='%Y-%m-%d',inferSchema=True)
inp_df=inp.toDF('c1','c2','c3','c4','c5')
l1=['c2','c3']
l2=[]
agg_dict={}
for element in l1:
    l2.append("case when {}='{}' then 1 else 0 end as {}".format(element,'Y',element+'_Y'))
    l2.append("case when {}='{}' then 1 else 0 end as {}".format(element, 'N', element + '_N'))
    l2.append("case when {}='{}' then 1 else 0 end as {}".format(element, '', element + '_empty'))
    agg_dict[element+'_Y']="sum"
    agg_dict[element + '_N'] = "sum"
    agg_dict[element + '_empty'] = "sum"
inp_df_extra_columns=inp_df.selectExpr(l2)
inp_df_agg=inp_df_extra_columns.agg(agg_dict)
for col in inp_df_agg.schema.names:
    col1=col.replace('sum(','').replace(')','')
    print "{}|{}".format(col1,inp_df_agg.head(1)[0][col])
