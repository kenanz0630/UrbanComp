import dbProcess, dbscan
import tweet_term, tweet_auto, tweet_temp, tweet_venue
import myWordcloud
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
AUTO/GENU TWEET_N 
------------------------------------------------------------------------------
'''
tbname='tweet_all_dist'
'''
tweet_temp.tweet_n_city(tbname)
tweet_temp.tweet_n_city_scale(tbname,'doy')
tweet_temp.tweet_n_city_scale(tbname,'how')



GENU_TWEET TEMP SENTI CITY-LEVEL
------------------------------------------------------------------------------

tweet_temp.tweet_senti_city(tbname)
#tweet_temp.tweet_senti_city_scale(tbname,'doy')
tweet_temp.tweet_senti_city_scale(tbname,'how')

tweet_temp.tweet_senti_city_hist(tbname)





GENU-TWEET SPATIAL CLUSTERING
*****************************************************************************
SEASONAL GENU-TWEET CLUSTERS WITH SAMPLE DATASET
------------------------------------------------------------------------------
OPTION 1: SAME RANDOM VALUE FOR EACH SEASON >> DIFF MINPTS
OPTION 2: SAME SAMPLE SIZE FOR EACH SEASON >> SAME MINPTS (CHOSEN)
'''
eps=0.0015
#minPts=[200,200,150,180]
minPts=[250,250,250,250]
seasons=[['spring',60,151,eps,minPts[0]],
		['summer',152,243,eps,minPts[1]],
		['fall',244,344,eps,minPts[2]],
		['winter',345,59,eps,minPts[3]]]
'''
dbscan.dbscan_sample_season(tbname,seasons,0.4)


SUPER GENU-TWEET CLUSTERS
------------------------------------------------------------------------------

fname=tbname+'_rand_tweet_cls'
exts=['spr','sum','fal','win']
eps=0.0015
minPts=650
eps_ns=0.0015
minPts_ns=250
minCls=400

dbscan.dbscan_sup(fname,exts,eps,minPts,eps_ns,minPts_ns,minCls)



CREATE CLUSTER TABLE
------------------------------------------------------------------------------

fname=tbname+'_rand_tweet_cls_sup'
dbProcess.tbCreate_cls(fname)




GENUE-TWEET CLUSTER SENTI
*****************************************************************************
GENU_TWEET CLUSTER SENTI
------------------------------------------------------------------------------
>> ALL CLUSTER HIST AND STATS
'''
fname=tbname+'_rand_tweet_cls_sup'
'''
tweet_temp.tweet_senti_cls_stats(fname)

>> MOST-HAPPY/UNHAPPY-TWEET USER

pos=[16,24,18]
neg=[8,12,27]
tweet_temp.tweet_senti_cls_user(fname,1,pos,50)
tweet_temp.tweet_senti_cls_user(fname,-1,neg,50)


>> CLUSTER SENTI NORM BY USER

tweet_temp.tweet_senti_norm_cls_stats(fname)


>> SENTI_VAL AVG T-TEST: HAPPY VS UNHAPPY CLUSTERS

tweet_temp.tweet_senti_cls_ttest(fname,sample=500)


>> CLUSTERS TEMP SENTI TERRACE VILLAGE/18, STATION SQUARE/14, STRIP DISTRICT/7 
PERRY NORTH/23, HIGHLAND PARK/30, PERRY SOUTH/31, HOMESTEAD 26

cls=[18,14,7,23,30,31]

tweet_temp.tweet_senti_cls(fname,cls)
tweet_temp.tweet_senti_cls_scale(fname,cls,'how')

tweet_temp.tweet_senti_norm_cls(fname,cls)
tweet_temp.tweet_senti_cls_norm_scale(fname,cls,'how')

cls=range(8)
tweet_temp.tweet_senti_norm_cls(fname,cls)
tweet_temp.tweet_senti_cls_norm_scale(fname,cls,'how')
tweet_temp.tweet_senti_cls_norm_scale(fname,cls,'doy')



GENU-TWEET CLUSTER COMMON TERM
*****************************************************************************
GENU-TWEET TERM TF-IDF
-----------------------------------------------------------------------------
>> HAPPY TWEETS

fname=tbname+'_rand_tweet_cls_sup'

tweet_term.process_term_cls(fname,senti=1)
#tweet_term.tf_idf_test(fname+'_term_pos')

fname+='_term_pos'
tweet_term.common_term(fname,60)
'''
fname=tbname+'_rand_tweet_cls_sup_term_pos'
#cls=range(8)+range(9,12)+[14,15,19,22,30]
cls=range(8)
'''
tweet_term.common_term_cls(fname,50,cls)



>> UNHAPPY TWEETS

fname=tbname+'_rand_tweet_cls_sup'

tweet_term.process_term_cls(fname,senti=-1)

fname+='_term_neg'
tweet_term.common_term(fname,60)

fname=tbname+'_rand_tweet_cls_sup_term_neg'
cls=range(8)
tweet_term.common_term_cls(fname,50,cls)


>> EMOTIONAL TWEETS

fname=tbname+'_rand_tweet_cls_sup'

tweet_term.process_term_cls(fname)

fname+='_term'
tweet_term.common_term(fname,60)

fname=tbname+'_rand_tweet_cls_sup_term'
cls=range(8)
tweet_term.common_term_cls(fname,50,cls)




GENU-TWEET TERM SENTI
-----------------------------------------------------------------------------
>> HAPPY TWEETS
'''
fn_data=tbname+'_rand_tweet_cls_sup'
fn_term=fn_data+'_term_pos_tfIdf'
'''
tweet_term.common_term_cls_senti(fn_term,fn_data,senti=1)


>> UNHAPPY TWEETS

fn_term=fn_term.replace('pos','neg')
tweet_term.common_term_cls_senti(fn_term,fn_data,senti=-1)

>> EMOTIONAL TWEETS

fn_term=fn_term.replace('_pos','')
tweet_term.common_term_cls_senti(fn_term,fn_data)




WORD CLOUD BASED ON TERM TF-IDF AND SENTI
------------------------------------------------------------------------------
'''
names=['downtown','oakland','southside','console energy center',\
'pnc park','heinz field','shadyside & east liberty','strip district']

'''
>> HAPPY TWEETS

fname=tbname+'_rand_tweet_cls_sup_term_pos'
myWordcloud.wordCloud_cls(fname,cls,names,senti=1)

>> UNHAPPY TWEETS

fname=tbname+'_rand_tweet_cls_sup_term_neg'
myWordcloud.wordCloud_cls(fname,cls,names,senti=-1)


>> EMOTIONAL TWEETS

fname=tbname+'_rand_tweet_cls_sup_term'
myWordcloud.wordCloud_cls(fname,cls,names)





AUTO-TWEET ANALYSIS
*****************************************************************************
VENUE EXPORT AND MERGE
------------------------------------------------------------------------------

tweet_venue.export_venue(tbname)


ASSIGN VENUE TO SUP-CLUSTER
------------------------------------------------------------------------------

eps=0.0015
minPts=250
fn_cls=tbname+'_rand_tweet_cls_sup'
fn_auto=tbname+'_venue_tweet'
dbscan.dbscan_auto_tweet(fn_cls,fn_auto,eps,minPts)


TEMP VENUE CHECKIN VS URBAN HAPPINESS
------------------------------------------------------------------------------
>> CITY LEVEL
'''
fn_senti=tbname+'_senti_city'
fn_n=tbname+'_n_city'

'''
tweet_temp.checkin_corroef_city_scale(fn_senti,fn_n,'how')



>> SUP-CLUSTER LEVEL 
'''
cls=range(16)
fname=tbname+'_venue_tweet_cls'
'''
tweet_venue.venue_checkin_sum(fname)

'''
cls=[0,2,4,9,6,1,3,7,22,5]
'''
fname=tbname+'_rand_tweet_cls_sup'
tweet_temp.tweet_senti_cls_norm_scale(fname,cls,'how')


tweet_temp.venue_checkin_cls_scale(fname,cls,'how')


fn_senti=tbname+'_rand_tweet_cls_sup_senti_norm'
fn_checkin=tbname+'_venue_tweet_cls_checkin'
tweet_temp.checkin_corroef_cls_scale(fn_senti,fn_checkin,cls,'how')



TEMP VENUE CHECKIN VS URBAN EMOTION STRENGTH
------------------------------------------------------------------------------
>> CITY LEVEL

tweet_temp.tweet_senti_city_abs(tbname)

fn_senti=tbname+'_senti_city_abs'
fn_n=tbname+'_n_city'
tweet_temp.checkin_corroef_city_scale(fn_senti,fn_n,'how')


>> SUP-CLUSTER LEVEL 

fname=tbname+'_rand_tweet_cls_sup'
tweet_temp.tweet_senti_cls_norm_abs(fname,cls)


fn_senti=tbname+'_rand_tweet_cls_sup_senti_norm_abs'
fn_checkin=tbname+'_venue_tweet_cls_checkin'
tweet_temp.checkin_corroef_cls_scale(fn_senti,fn_checkin,cls,'how')



POPULAR VENUE WORDCLOUD
------------------------------------------------------------------------------

#cls=range(8)
cls=[0,2,4,9,6,1,3,7]
fname=tbname+'_venue_cls'
tweet_venue.pop_venue_cls(fname,cls,20)

names=['downtown','oakland','southside','console energy center',\
'pnc park','heinz field','shadyside & east liberty','strip district']
'''
names=['downtown','southside','pnc park','waterfront','shadyside & east liberty',
'oakland','console energy center','strip district']
'''
>> RANK BY CHECKIN

fname=tbname+'_venue_cls_pop_checkin'
myWordcloud.wordCloud_venue(fname,cls,names,type='checkin')


>> RANK BY DIFF-USER

fname=tbname+'_venue_cls_pop_user'
myWordcloud.wordCloud_venue(fname,cls,names,type='user')



VENUE SEARCH IN 4SQ DATABASE
------------------------------------------------------------------------------
'''
foursq={'id':'CLLMAZ0UKCRB2NFMQPB1MVM3VVAFYOABWIFZO3PQC4QRKVNB',\
'secret':'YTNNANYZHG0C2G1TRHQTMZDSWZDRDXDY04KI4DRNL3JBGQEY'}

uses=['Arts & Entertainment',
'College & University',
'Event',
'Food',
'Nightlife Spot',
'Outdoors & Recreation',
'Professional & Other Places',
'Residence',
'Shop & Service',
'Travel & Transport']


'''
tweet_venue.venue_category_hier(foursq)

fname=tbname+'_venue_tweet_cls'
tweet_venue.search_venue(fname,foursq,maxDist=50)




VENUE PROFILING
------------------------------------------------------------------------------

fname=tbname+'_venue_tweet_cls_new'
fn_cat='venue_cat_hier'

tweet_venue.venue_profile(fname,fn_cat)



CREATE VENUE_TWEET & VENUE TABLE
------------------------------------------------------------------------------

fname=tbname+'_venue_tweet_cls_new'
fn_cat='venue_cat_hier'
dbProcess.tbCreate_venue_tweet(fname,fn_cat,fname.replace('_new',''))

fname=tbname+'_venue_cls_new'
dbProcess.tbCreate_venue(fname,fname.replace('_new',''))




VENUE CATEGORY AND URBAN USE
------------------------------------------------------------------------------
'''
uses+=['NA']
'''
tweet_venue.venue_category_sum(tbname+'_venue_cls',cls,uses)

fname=tbname+'_venue_tweet_cls_new'
fn_cat='venue_cat_hier'

#tweet_temp.urban_use_city(fname,fn_cat,uses)

tweet_temp.urban_use_city(fname,fn_cat,uses,'how')

tweet_temp.urban_use_cls(fname,fn_cat,cls,uses,'how')



TEMP URBAN USE VS HAPPINESS
------------------------------------------------------------------------------

fn_senti=tbname+'_senti_city'
fn_use=tbname+'_venue_city_cat'

tweet_temp.urban_use_corroef_city(fn_senti,fn_use,uses,'how')

fn_senti=tbname+'_rand_tweet_cls_sup_senti_norm'
fn_use=tbname+'_venue_cl_cat'
cls=[0,2,9,6]
tweet_temp.urban_use_corroef_cls(fn_senti,fn_use,uses,cls,'how')





TEMP URBAN USE VS EMOTION STRENGTH
------------------------------------------------------------------------------

fn_senti=tbname+'_senti_city_abs'
fn_use=tbname+'_venue_city_cat'

tweet_temp.urban_use_corroef_city(fn_senti,fn_use,uses,'how')

fn_senti=tbname+'_rand_tweet_cls_sup_senti_norm_abs'
fn_use=tbname+'_venue_cl_cat'
cls=[0,2,9,6]
tweet_temp.urban_use_corroef_cls(fn_senti,fn_use,uses,cls,'how')





CLUSRER URBAN USE
------------------------------------------------------------------------------
>>> OVERALL MIXED-USE
'''
uses=['Arts & Entertainment',
'College & University',
'Food',
'Nightlife Spot',
'Outdoors & Recreation',
'Professional & Other Places',
'Residence',
'Shop & Service',
'Travel & Transport',
'NA']

cls=range(32)
tweet_venue.venue_cat_entropy(tbname+'_venue_tweet_cls',cls)
'''
>>> TEMP MIXED-USE

tweet_venue.venue_cat_temp_entropy(tbname+'_venue_tweet_cls',cls,uses)

cls=range(32)
use_cl=[([0],7),([1,2,9,6],4),([22,7,4,3,5],2),([14,11,19],1)]
tweet_venue.venue_category_repr(tbname+'_venue_tweet_cls',uses,use_cl,cls)


TWEET SENTIMENT COMPARISON
------------------------------------------------------------------------------
>> AVG SENTI T-TEST
'''
fname=tbname+'_rand_tweet_cls_sup_senti_ttest'
cls=[0,1,2,9,6,22,7,4,3,5,14,11,19,10,15,12,8,13]
'''
tweet_venue.rewrite_ttest(fname,cls)


>> TEMP SENTI AVG & STD
'''
fname=tbname+'_rand_tweet_cls_sup'
'''
tweet_temp.tweet_senti_cls_norm_scale(fname,cls,'how')

fname+='_senti_norm_how'
tweet_venue.senti_scale_stats(fname,cls,'how')


WORDCLOUD BASED ON URBAN USE
------------------------------------------------------------------------------

tb_venue=tbname+'_venue_cls'
cls=cls[:-5]
minCheckin=5
minUser=5
maxN=25
tweet_venue.repr_venue_cls(tb_venue,cls,uses,minCheckin,minUser,maxN)

>> POP VENUE BY CHECKIN
'''
cls=[0,1,2,9,6,22,7,4,3,5,14,11,19]
names=['downtown','oakland','southside','waterfront','shadyside-east liberty',
'walnut st','strip district','pnc park','console energy center','heinz field',
'station square','cmu','lawrencevile']
'''

myWordcloud.wordCloud_use_name(uses)


fname=tbname+'_venue_cls_cat_repr_pop_checkin'
myWordcloud.wordCloud_venue_use(fname,cls,names,'checkin',uses)

fname=tbname+'_venue_cls_cat_repr_pop_user'
myWordcloud.wordCloud_venue_use(fname,cls,names,'user',uses)









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