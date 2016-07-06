import dbProcess
import tweet_term, tweet_auto, tweet_temp, dbscan
import myWordcloud



# TWEET DATA SUMMARY
# *****************************************************************************

# tweet_temp.tweet_n_all()
#
# tweet_temp.tweet_n_all_doy()


# TWEET PROCESSING
# *****************************************************************************
# PROCESS VALID TWEETS IN CITY: TWEET_SENTI = TXT_SENTI + EMOJI_SENTI
# ------------------------------------------------------------------------------
#
# dbProcess.dbProcess('tweet_pgh_tmp','pittsburgh',maxIter=False)


# TWEET_CITY SUMMARY
# *****************************************************************************
# AUTO/GENU TWEET_N
# ------------------------------------------------------------------------------

tbname='tweet_pgh_tmp'

# tweet_temp.tweet_n_city(tbname)
# tweet_temp.tweet_n_city_scale(tbname,'doy')
# tweet_temp.tweet_n_city_scale(tbname,'how')
#
#
#
# # GENU_TWEET TEMP SENTI CITY-LEVEL
# # ------------------------------------------------------------------------------
#
# tweet_temp.tweet_senti_city(tbname)
#
# tweet_temp.tweet_senti_city_scale(tbname,'doy')
# tweet_temp.tweet_senti_city_scale(tbname,'how')
#
#
#
#
#
# # GENU-TWEET SPATIAL CLUSTERING
# # *****************************************************************************
# # ALL GENU-TWEET CLUSTERS
# # ------------------------------------------------------------------------------
# eps=0.002
# minPts=500
#
# dbscan.dbscan(tbname,eps,minPts)
#
#
# SEASONAL GENU-TWEET CLUSTERS
# ------------------------------------------------------------------------------

eps=0.0015
minPts=[200,200,150,180]
seasons=[['spring',60,151,eps,minPts[0]],
		['summer',152,243,eps,minPts[1]],
		['fall',244,344,eps,minPts[2]],
		['winter',345,59,eps,minPts[3]]]

dbscan.dbscan_season(tbname,seasons)


# SUPER GENU-TWEET CLUSTERS
# ------------------------------------------------------------------------------

fname=tbname+'_tweet_cls'
exts=['spr','sum','fal','win']
eps=0.0015
minPts=450
eps_ns=0.0015
minPts_ns=200
minCls=400
#
# dbscan.dbscan_sup(fname,exts,eps,minPts,eps_ns,minPts_ns,minCls)
#
#
#
# # GENUE-TWEET CLUSTER TEMP SENTI
# # *****************************************************************************
# # GENU_TWEET CLUSTER TEMP SENTI
# # ------------------------------------------------------------------------------
#
# fname=tbname+'_tweet_cls_sup'
# cls=range(7)+range(10,14)+[15,16,18]
#
# tweet_temp.tweet_senti_cls(fname,cls)
# tweet_temp.tweet_senti_cls_scale(fname,cls,'doy')
# tweet_temp.tweet_senti_cls_scale(fname,cls,'how')
#
# # GENU_TWEET CLUSTER TEMP SENTI NORMALIZATION
# # ------------------------------------------------------------------------------
# # SCHEME 1: NORM BY POS_N >>>>>
#
# fn_cls=tbname+'_tweet_cls_sup_senti'
# fn_city=tbname+'_senti_city'
# tweet_temp.tweet_senti_cls_norm(fn_cls,fn_city)
# tweet_temp.tweet_senti_cls_norm(fn_cls,fn_city,'doy')
# tweet_temp.tweet_senti_cls_norm(fn_cls,fn_city,'how')
#
# # SCHEME 2: NORM BY TWEET_N >>>>>
#
# fn_cls=tbname+'_tweet_cls_sup_senti'
# fn_city=tbname+'_n_city'
# tweet_temp.tweet_senti_cls_norm(fn_cls,fn_city)
# tweet_temp.tweet_senti_cls_norm(fn_cls,fn_city,'doy')
# tweet_temp.tweet_senti_cls_norm(fn_cls,fn_city,'how')
#
#
#
# # GENU-TWEET CLUSTER COMMON TERM/TAG
# # *****************************************************************************
# # GENU-TWEET TERM TF-IDF
# # -----------------------------------------------------------------------------
# fname=tbname+'_tweet_cls_sup'
# tweet_term.process_term_cls(fname)
#
# fname+='_terms'
# tweet_term.common_term(fname,80)
#
# fname=tbname+'_tweet_cls_sup_terms'
# tweet_term.common_term_cls(fname,50)
#
#
# # GENU-TWEET TERM SENTI
# # -----------------------------------------------------------------------------
#
# fn_data=tbname+'_tweet_cls_sup'
# fn_term=fn_data+'_terms_tfIdf'
#
# tweet_term.common_term_cls_senti(fn_term,fn_data)
#
#
# # WORD CLOUD OF TF-IDF AND SENTI
# # ------------------------------------------------------------------------------
#
# fname=tbname+'_tweet_cls_sup_terms'
# cls=range(7)+range(10,14)+[15,16,18]
# names=['downtown','oakland','console energy center','southside',\
# 'pnc park','heinz field','strip district','cmu','pitts tech center',\
# 'southside works','station square','east liberty',\
# 'lower lawrenceville','shadyside']
#
# myWordcloud.wordCloud_cls(fname,cls,names)
#
# # CREATE CLUSTER TABLE
# # ------------------------------------------------------------------------------
#
# fname=tbname+'_tweet_cls_sup'
# dbProcess.tbCreate_cls(fname)
#
#
#
#
#
# # AUTO-TWEET ANALYSIS
# # *****************************************************************************
# # AUTO_TWEET VS GENU_TWEET
# # ------------------------------------------------------------------------------
#
# names=['non','4sq','inst','job']
#
# tweet_auto.auto_tweet_stats(tbname,names)
#
# tweet_auto.auto_tweet_senti_hist(tbname,names)
#
#
# # AUTO_TWEET DIST
# # ------------------------------------------------------------------------------
#
# autos=['4sq','inst','job']
#
# for auto in autos:
# 	tweet_auto.auto_tweet_map(tbname,auto)
#
#
# # FOURSQ & INST VENUE
# # ------------------------------------------------------------------------------
#
# tweet_auto.auto_tweet_venue(tbname,'4sq')
#
# tweet_auto.auto_tweet_venue(tbname,'inst')
#
#
# # ASSIGN VENUE TO SUP-CLUSTERS
# # ------------------------------------------------------------------------------
#
# eps=0.0015
# minPts=450
# fn_cls=tbname+'_tweet_cls_sup'
# fn_venue=tbname+'_inst_venue'
# dbscan.dbscan_venue(fn_cls,fn_venue,eps,minPts)
#
# fn_venue=fn_venue.replace('inst','4sq')
# dbscan.dbscan_venue(fn_cls,fn_venue,eps,minPts)
#
#
# # REWRITE VENUE TWEET BY CLUSTER
# # ------------------------------------------------------------------------------
#
# for name in ['inst','4sq']:
# 	fn_venue=tbname+'_%s_venue_cls'%name
# 	fn_tweet=tbname+'_%s_tweet_new'%name
# 	tweet_auto.venue_tweet_cls(fn_venue,fn_tweet)
#
#
# # CREATE VENUE TABLE
# # ------------------------------------------------------------------------------
#
# for name in ['inst','4sq']:
# 	fname=tbname+'_%s_venue_cls_tweet'%name
# 	dbProcess.tbCreate_venue(fname,tbname+'_%s'%name)
#
#
#
# # VENUE TEMP CHECKIN VS CLUSTER TEMP SENTI
# # ------------------------------------------------------------------------------
#
# fn_city=tbname+'_n_city'
# for name in ['inst','4sq']:
# 	#fname=tbname+'_%s_venue_cls_tweet'%name
# 	fname=tbname+'_%s_venue_cls_checkin'%name
# 	for scale in ['doy','how']:
# 		#tweet_temp.venue_checkin_cls(fname,cls,scale)
# 		tweet_temp.venue_checkin_cls_norm(fname,fn_city,scale)
#
#
# fn_senti=tbname+'_senti_city'
# fn_n=tbname+'_n_city'
# for name in ['inst','4sq']:
# 	for scale in ['doy','how']:
# 		tweet_temp.corroef_city(fn_senti,fn_n,name,scale)
#
# # RESULTS
# # ===================================
# # Corroef >>
# # city-level doy inst 0.157918771596
# # Corroef >>
# # city-level how inst 0.48269828415
# # Corroef >>
# # city-level doy 4sq 0.186369697372
# # Corroef >>
# # city-level how 4sq 0.473871377638
#
#
# fn_senti=tbname+'_tweet_cls_sup_senti'
#
# names=['downtown','oakland','console energy center','southside',\
# 'pnc park','heinz field','strip district','cmu','pitts tech center',\
# 'southside works','station square','east liberty',\
# 'lower lawrenceville','shadyside']
#
#
# fn_checkin=tbname+'_inst_venue_cls_checkin'
# autos=['inst','4sq']
# scales=['doy','how']
# tweet_temp.corroef_cls(fn_senti,fn_checkin,cls,names,scales,autos)
#
#
# # WORD CLOUD OF VENUE CHECKIN
# # ------------------------------------------------------------------------------
#
# # tb=tbname+'_4sq'
# # myWordcloud.wordCloud_venue(tb,cls,names,minCheckin=1,maxVenue=50)
#
#
#
# '''
#
#
#
#
#
#
# CLUSTER EVENT
# *****************************************************************************
# EVENT DETECT
# ------------------------------------------------------------------------------
# >> EVENT-TERM SCALE-SCAN
# '''
# cls=range(1,4)
# scales=[365,30,14,7]
#
# tweet_term.process_tweet_term('tweet_all_pos_sup')
#
# event_detect.scale_scan_term('tweet_all_pos_sup_full',scales,cls)
#
# event_detect.scale_scan_tag('tweet_all_pos_sup',scales,cls)
#
# # >> EVENT-TERM/TAG CLUSTERING
#
# fname='tweet_all_pos_sup_full'
# event_detect.scale_scan_event(fname,scales,cls)
#
#
# # EVENT TERM TF/SENTI
# # ------------------------------------------------------------------------------
#
# fname='tweet_all_pos_sup_full_scs_event_filt'
#
# event_detect.event_tf_senti(fname,'tweet_all_pos_sup_full',cls)
#
# # EVENT WORDCLOUD
# # ------------------------------------------------------------------------------
#
# for cl in cls:
# 	fname='event_detect\\tweet_all_pos_sup_full_scs_event_filt_tf_cl%d'%cl
# 	myWordCloud.wordCloud_single(fname,'event',(255,51,153))
#
#
# # CLUSTER TOPIC
# # *****************************************************************************
# # LDA MODEL
# # ------------------------------------------------------------------------------
#
# fname='tweet_all_pos_sup_full'
# cls=range(1,6)
# topics=[15,15,10,18,10]
# tweet_term.tweet_topic(fname,cls,topics)
#
# fname='tweet_all_pos_sup_full_term_lda'
#
# cls=range(1,6)
# tweet_term.topic_tf_senti(fname,'tweet_all_pos_sup_full',cls)
#
# cls=range(4,6)
# for cl in cls:
# 	fname='tweet_term\\tweet_all_pos_sup_full_term_lda_topic_cl%d'%cl
# 	myWordCloud.wordCloud_single(fname,'topic',(255,51,153))
