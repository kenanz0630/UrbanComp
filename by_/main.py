import dbProcess
import tweet_term, tweet_auto, tweet_temp, dbscan
import foursq, myWordcloud
'''


TWEET DATA SUMMARY
*****************************************************************************

tweet_temp.tweet_n_all()

tweet_temp.tweet_n_all_doy()


TWEET PROCESSING
*****************************************************************************
PROCESS VALID TWEETS IN CITY: TWEET_SENTI = TXT_SENTI + EMOJI_SENTI
------------------------------------------------------------------------------

dbProcess.dbProcess('tweet_all_new','tweet_pgh',maxIter=False)


TWEET_CITY SUMMARY
*****************************************************************************
ALL TWEET_N 
------------------------------------------------------------------------------
'''
tbname='tweet_all_new'
'''
tweet_temp.tweet_n_city(tbname)

tweet_temp.tweet_n_city_scale(tbname,'doy')
tweet_temp.tweet_n_city_scale(tbname,'how')


AUTO_TWEET VS GENU_TWEET
------------------------------------------------------------------------------
'''
names=['non','4sq','inst','job']
'''
tweet_auto.tweet_stats(tbname,names)

tweet_auto.tweet_n(tbname,names)

tweet_auto.tweet_n_doy(tbname,names)

tweet_auto.tweet_senti_hist(tbname,names)


GENU_TWEET TEMP SENTI
------------------------------------------------------------------------------

tweet_temp.tweet_senti_city(tbname)

tweet_temp.tweet_senti_city_scale(tbname,'doy')
tweet_temp.tweet_senti_city_scale(tbname,'how')

fname=tbname+'_tweet_cls_sup'

tweet_temp.tweet_senti_cls(fname,range(5))

tweet_temp.tweet_senti_cls_scale(fname,range(5),'doy')
tweet_temp.tweet_senti_cls_scale(fname,range(5),'how')



SPATIAL CLUSTERING
*****************************************************************************
ALL GENU-TWEET CLUSTERS
------------------------------------------------------------------------------
eps=0.002
minPts=500

dbscan.dbscan(tbname,eps,minPts)


SEASONAL GENU-TWEET CLUSTERS
------------------------------------------------------------------------------
'''
eps=0.0015
minPts=[200,200,150,180]
seasons=[['spring',60,151,eps,minPts[0]],
		['summer',152,243,eps,minPts[1]],
		['fall',244,344,eps,minPts[2]],
		['winter',345,59,eps,minPts[3]]]
'''
dbscan.dbscan_season(tbname,seasons)


SUPER GENU-TWEET CLUSTERS
------------------------------------------------------------------------------
'''
fname=tbname+'_tweet_cls'
exts=['spr','sum','fal','win']
eps=0.0015
minPts=450
eps_ns=0.0015
minPts_ns=200
minCls=400
'''
dbscan.dbscan_sup(fname,exts,eps,minPts,eps_ns,minPts_ns,minCls)

AUTO-TWEET CLUSTERS
------------------------------------------------------------------------------



CLUSTER COMMON TERM/TAG
*****************************************************************************
GENU-TWEET TERM 
-----------------------------------------------------------------------------
'''
fname=tbname+'_tweet_cls_sup'

tweet_term.process_term_cls(fname)

tweet_term.common_term(fname,80)

fname+='_terms'
tweet_term.common_term_cls(fname,50)
'''

fn_data=tbname+'_tweet_cls_sup'
fn_term=fn_data+'_terms_tfIdf'
cls=range(7)+range(10,14)+[15,16,18]
tweet_term.common_term_cls_senti(fn_term,fn_data,cls=cls)


TWEET TAG
------------------------------------------------------------------------------

tweet_tag.common_tag_tb(tbname,'tweet_pgh')

tweet_tag.common_tag_cl(tbname+'_pos_sup',30,cls=cls)


WORD CLOUD 
------------------------------------------------------------------------------
'''
fname=tbname+'_tweet_cls_sup_terms'
cls=range(7)+range(10,14)+[15,16,18]
names=['downtown','oakland','console energy center','southside',\
'pnc park','heinz field','strip district','cmu','pit tech center',\
'southside works','station square','east liberty',\
'lower lawrenceville','shadyside']
'''
myWordcloud.wordCloud(fname,cls,names)




AUTO-TWEET ANALYSIS
*****************************************************************************

4SQ
------------------------------------------------------------------------------
foursq.foursq_tweet(tbname)

foursq.foursq_venue(tbname+'_4sq_tweet')

foursq.foursq_venue_map(tbname+'_4sq_venue',minCheckin=5)

foursq.foursq_err_tweet(tbname+'_4sq_venue',tbname)

INSTAGRAM
------------------------------------------------------------------------------
inst


CLUSTER EVENT
*****************************************************************************
EVENT DETECT 
------------------------------------------------------------------------------
>> EVENT-TERM SCALE-SCAN
'''
cls=range(1,4)
scales=[365,30,14,7]
'''
tweet_term.process_tweet_term('tweet_all_pos_sup')

event_detect.scale_scan_term('tweet_all_pos_sup_full',scales,cls)

event_detect.scale_scan_tag('tweet_all_pos_sup',scales,cls)

>> EVENT-TERM/TAG CLUSTERING

fname='tweet_all_pos_sup_full'
event_detect.scale_scan_event(fname,scales,cls)


EVENT TERM TF/SENTI 
------------------------------------------------------------------------------

fname='tweet_all_pos_sup_full_scs_event_filt'

event_detect.event_tf_senti(fname,'tweet_all_pos_sup_full',cls)

EVENT WORDCLOUD
------------------------------------------------------------------------------

for cl in cls:
	fname='event_detect\\tweet_all_pos_sup_full_scs_event_filt_tf_cl%d'%cl
	myWordCloud.wordCloud_single(fname,'event',(255,51,153))


CLUSTER TOPIC
*****************************************************************************
LDA MODEL 
------------------------------------------------------------------------------

fname='tweet_all_pos_sup_full'
cls=range(1,6)
topics=[15,15,10,18,10]
tweet_term.tweet_topic(fname,cls,topics)

fname='tweet_all_pos_sup_full_term_lda'

cls=range(1,6)
tweet_term.topic_tf_senti(fname,'tweet_all_pos_sup_full',cls)

cls=range(4,6)
for cl in cls:
	fname='tweet_term\\tweet_all_pos_sup_full_term_lda_topic_cl%d'%cl
	myWordCloud.wordCloud_single(fname,'topic',(255,51,153))
'''