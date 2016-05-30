'''DB_PROCESS: PROCESS RAW DATA AND WRITE SHP FILE'''

from vaderSentiment.vaderSentiment import sentiment as vader
import preprocessing as pre
import math
import os
import psycopg2
import re

'''CLASS TWEET'''
class Tweet:
	def __init__(self,id,username,year,doy,dow,hour,lon,lat,txt,source,bool=0):
		self.id=id
		self.username=username
		self.year=year
		self.doy=doy
		self.dow=dow
		self.hour=hour
		self.lat=lat
		self.lon=lon
		self.senti_bool=bool
		self.clean_text(txt) #assign self.txt & self.emoji & self.hashtag
		self.auto_tweet(source) #check if auto tweet and assign self.auto
		self.preprocess() #assign self.term
		self.happy_val() #assign self.senti_val

	

	def clean_text(self,txt):
		emoji="/[\u2190-\u21FF]|[\u2600-\u26FF]|[\u2700-\u27BF]|[\u3000-\u303F]|[\u1F300-\u1F64F]|[\u1F680-\u1F6FF]/g"
		l=txt.split() #spit str to list
		#remove element starting with #/@/http and "@..."/emojis
		emoji_dict=self.init_emoji()
		l_clean=[x for x in l if (x.find('@')==-1 and x.find('http')==-1)]
		[self.emoji,self.txt,self.tag]=self.find_emoji_tag(l_clean,emoji_dict) #emoji process

	def init_emoji(self):
		try: # UCS-4
			highpoints = re.compile((u'[\U00010000-\U0010ffff]').encode('utf-8'))
		except re.error: # UCS-2
			highpoints = re.compile((u'[\uD800-\uDBFF][\uDC00-\uDFFF]').encode('utf-8'))
		return highpoints

	def find_emoji_tag(self,l,emoji_dict):
		emoji=[]
		txt=[]
		tag=[]
		for term in l:
			emoji+=re.findall(emoji_dict,term)
			if term[0]=='#':
				tag+=[term]
			else:
				txt+=[str(emoji_dict.sub('', term))]
		return ''.join(emoji),' '.join(txt),''.join(tag)


	def auto_tweet(self,source):
		#instagram
		if "Just posted a photo" in self.txt and "instagram" in source:
			self.auto='inst'
		elif ("#hiring" in self.tag or "#job" in self.tag) and "tweetmyjobs" in source:
			self.auto='job'
		elif "I'm at" in self.txt and "foursquare" in source:
			self.auto='4sq'
		else:
			self.auto=''

	def preprocess(self):
		l_0=pre.tokenize_tweet(self.txt)
		l_1=pre.remove_stopwords(l_0)
		#self.term=pre.stemming(l_1)
		self.term=l_1

	def happy_val(self):
		vs=vader(self.txt)
		if self.emoji=='':
			self.senti_val=vs['pos']-vs['neg']
		else:
			txt=vs['pos']-vs['neg']
			emoji=self.emoji_senti(self.emoji)
			self.senti_val=txt+emoji

	def emoji_senti(self,emojis):
		emoji_score=self.init_emoji_score()
		emoji_dict=self.init_emoji_dict()

		count=0
		score=0.0
		for emoji in re.findall(emoji_dict,emojis):
			if repr(emoji) in emoji_score:
				count+=1
				score+=emoji_score[repr(emoji)]
		if count:
			score/=count

		return score

	def init_emoji_score(self):
		emoji_score=dict()
		data=file('emoji_senti.csv').readlines()
		n=len(data)
		data=[data[i][:-1].split(',') for i in xrange(n)]
		for i in xrange(n):
			emoji_score[data[i][0]]=float(data[i][1])

		return emoji_score

	def init_emoji_dict(self):
		try: # UCS-4
			emoji_dict = re.compile(u'[\U00010000-\U0010ffff]')
		except re.error: # UCS-2
			emoji_dict = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]')

		return emoji_dict


'''CLASS PGCONTROLLER'''
class PgController:
	def __init__(self,tbname,dbname,user):
		self.tbname=tbname
		self.dbname=dbname
		self.user=user
		[self.cur,self.conn]=db_conn(self.dbname,self.user)



'''MAIN FUNCTION'''
def dbProcess(tbname,dbname,t_range=False,maxIter=200,user='postgres'):
	print "Connect to database"
	[cur,conn]=db_conn(dbname,user)

	print "Create new tables"
	init_tb(tbname,cur,conn)		

	if t_range: #if def time range
		print "Process by week"
		main_by_week(tbname,cur,conn,t_range)
	else: #otherwise
		print "Process all"
		main_all(tbname,cur,conn,maxIter)

	print "Dichotomize pos/neg tweets"
	senti_dicho(tbname,cur,conn)
	
	print "Create index on id, coordinate and timestamp"
	create_index(tbname,cur,conn)

	print "Disconnect database "
	cur.close()
	conn.close()

'''HELPER FUNCTION'''
'''------------------------------------------------------------------'''
def db_conn(dbname,user):
	conn_info="dbname="+dbname+" user="+user
	conn=psycopg2.connect(conn_info)
	cur=conn.cursor()
	return cur,conn

def init_tb(tbname,cur,conn):
	cur.execute("drop table if exists %s" %tbname)
	cur.execute("""create table %s(
		id text,
		username text,
		year integer, doy integer, dow integer, hour integer,
		lon real, lat real, coordinates geometry(Point,4326),
		txt text,
		auto_tweet text,
		emoji text,
		tag text,
		senti_val real, senti_bool integer,
		term text);
		"""%tbname)
	conn.commit()

'''------------------------------------------------------------------'''
def main_all(tbname,cur,conn,maxIter):
	part=1
	limit=10000
	offset=1
	data_all=[]
	while 1:
		cur.execute("""select id_str,user_screen_name,
			date_part('year',created_at),date_part('doy',created_at),date_part('dow',created_at),
			date_part('hour', created_at),st_astext(coordinates),text,source from tweet_pgh 
			where in_reply_to_screen_name is null and 
			in_reply_to_status_id is null limit %s offset %s;
			""",(limit,offset))		
		n=cur.rowcount
		print "Process part %d from %d to %d" %(part,offset,offset+n-1)		
		tweet_process(cur.fetchall(),tbname,cur,conn)
		if n<limit:
			break
		elif maxIter and part>=maxIter:
			break
		else:
			offset=part*limit	
			part+=1		

def main_by_week(tbname,cur,conn,t_range):
	i=1
	[ts1,week]=t_range	
	data_all=[]
	while 1:
		print "Process week %d" % i
		cur.execute("select date %s + interval '%s day';",(ts1,7))
		ts2=cur.fetchone()[0]
		#extract user_name, dow, hour, coordinates, text, source
		cur.execute("""select id_str,user_screen_name,
			date_part('year',created_at),date_part('doy',created_at),date_part('dow',created_at),
			date_part('hour', created_at),st_astext(coordinates),text,source from tweet_pgh 
			where in_reply_to_screen_name is null and in_reply_to_status_id is null and 
			created_at <@ tsrange(%s,%s);""",(ts1,ts2))
		print "Finish extract data"
		tweet_process(cur.fetchall(),tbname,cur,conn)
		if i==week:
			break
		else:
			ts1=ts2	
			i+=1

def tweet_process(data,tbname,cur,conn):
	n=len(data)
	if n>0:
		print "Screen valid data"
		[data,n_valid]=tweet_valid(data)
		print "%d valid, %0.1f%%" % (n_valid,100.0*n_valid/n)	

		data=filter_inpit(data)
		print "%d tweets in pitts" %len(data)

		print "Write processed tweet data into new table %s" %tbname
		write_tb(data,tbname,cur,conn)
	else:
		print "No records to be processed"

def tweet_valid(data):
	n_valid=0
	data_valid=[]
	for i in xrange(len(data)):
		data[i]=row_process(data[i])
		tweet=init_tweet(data[i])		
		if tweet.txt!='':
			data_valid+=[tweet]
			n_valid+=1
	return data_valid,n_valid

def row_process(row):
	row_p=[row[0]] #id
	row_p+=[row[1]] #username
	row_p+=[int(row[2]),int(row[3]),int(row[4]),int(row[5])] #year,doy, dow, hour
	row_p+=row[6][6:-1].split() #[lon,lat]
	row_p+=row[7:] #[txt,source]
	return row_p

def init_tweet(row):
	id=row[0]
	username=row[1]
	[year,doy,dow,hour]=row[2:6]
	[lon,lat]=[float(row[6]),float(row[7])]
	[txt,source]=row[8:]
	tweet=Tweet(id,username,year,doy,dow,hour,lon,lat,txt,source)
	return tweet

def filter_inpit(data):
	range=[(-80.10,40.36),(-79.86,40.51)]
	n=len(data)
	data_filter=[data[i] for i in xrange(n) if in_range(data[i],range)]
	return data_filter

def in_range(data,range):
	[lon,lat]=[data.lon,data.lat]
	[(lon_min,lat_min),(lon_max,lat_max)]=range
	if lon>lon_min and lon<lon_max and lat>lat_min and lat<lat_max:
		return True
	else:
		return False

def write_tb(data,tbname,cur,conn):
	for tweet in data:
		f=insert_data(tbname,tweet,cur,conn)	
	print "Insert %d tweet records in table %s" %(len(data),tbname)

def insert_data(tbname,tweet,cur,conn):
	term=','.join(tweet.term)
	txt=tweet.txt.replace("\'","\'\'")
	coordi="ST_GeomFromText('POINT(%f %f)',4326)"%(tweet.lon,tweet.lat)
	emoji="'"+tweet.emoji+"'" if tweet.emoji!='' else 'Null'
	auto_tweet="'"+tweet.auto+"'" if tweet.auto!='' else 'Null'
	tag="'"+tweet.tag.replace("\'","\'\'")+"'" if tweet.tag!='' else 'Null'
	try:
		cur.execute("""insert into %s
	 	values('%s','%s',%d,%d,%d,%d,%f,%f,%s,'%s',%s,%s,%s,%f,%d,'%s');
	 	"""%(tbname,tweet.id,tweet.username,tweet.year,tweet.doy,tweet.dow,tweet.hour,tweet.lon,tweet.lat,coordi,
			txt,auto_tweet,emoji,tag,tweet.senti_val,tweet.senti_bool,term))
	except:
		print "ERROR!Tweet content:"
		print '\tid %s  username %s'%(tweet.id,tweet.username) 
		print '\ttxt %s  term %s'%(txt,term)
		print '\tauto_tweet %s'%auto_tweet
		print '\temoji %s'%emoji
		print '\ttag %s'%tag
	conn.commit()


'''------------------------------------------------------------------'''
def senti_dicho(tbname,cur,conn):
	print "Calculate senti_val mean and std"
	[mean,std]=stat_senti(tbname,cur)
	print "Assign senti_bool"
	senti_bool(tbname,mean,std,cur,conn)

def stat_senti(tbname,cur):
	cur.execute("select avg(senti_val),stddev(senti_val) from %s;" %tbname)
	(mean,std)=cur.fetchall()[0]

	return mean,std

def senti_bool(tbname,mean,std,cur,conn):
	cur.execute('update %s set senti_bool=1 where senti_val>%f;'%(tbname,mean+std))
	conn.commit()
	cur.execute('update %s set senti_bool=-1 where senti_val<%f;'%(tbname,mean-std))
	conn.commit()

'''------------------------------------------------------------------'''
def create_index(tbname,cur,conn):
	cur.execute('create index %s_coordi_idx on %s using gist(coordinates)'%(tbname,tbname))
	conn.commit()
	cur.execute('create index %s_doy_idx on %s using btree(doy)'%(tbname,tbname))
	conn.commit()
	cur.execute('create index %s_dow_idx on %s using btree(dow)'%(tbname,tbname))
	conn.commit()
	cur.execute('create index %s_hour_idx on %s using btree(hour)'%(tbname,tbname))
	conn.commit()
	cur.execute('create index %s_id_idx on %s(id)'%(tbname,tbname))
	conn.commit()

