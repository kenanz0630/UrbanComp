import dbProcess, os, math, shapefile
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from string import punctuation
from geopy.distance import great_circle



'''--- MAIN FUNCTION ---'''
def auto_tweet_both(pg,auto,sep='\t'):
	data_b=[]
	name_b=[]
	for i in xrange(len(auto)):
		print '-- Export %s auto-tweet txt'%auto[i]
		pg.cur.execute('''
			select txt,lon,lat,username,year,doy,dow,hour,senti_val 
			from %s where auto_tweet='%s';'''%(pg.tbname,auto[i]))
		data=pg.cur.fetchall()
		n=pg.cur.rowcount
		txt=[data[j][0] for j in xrange(n)]
		print '-- Process venue name'
		name=process_venue_name(txt,auto[i])
		n=len(name)
		data_b+=[data[j] for j in xrange(n) if name[j]!='']
		name_b+=[name[j] for j in xrange(n) if name[j]!='']
		n=len(data)
	
	venues=list(set(name_b))	
	print '---- Find %d venue names from auto-tweets'%len(venues)
	tweet=init_clusters(len(venues),venues)
	n=len(data_b)
	for venue in venues:
		tweet[venue]=[data_b[i] for i in xrange(n) if name_b[i]==venue]
	
	print '-- Write results'
	f=os.open('txt\\auto-tweet\\'+pg.tbname+'_tweet.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'venue,lon,lat,user,year,doy,dow,hour,senti_val,txt\n'.replace(',',sep))


	for venue in venues:
		n=len(tweet[venue])
		for i in xrange(n):
			(txt,lon,lat,user,year,doy,dow,hour,senti_val)=tweet[venue][i]
			os.write(f,'%s'%venue+(',%0.4f,%0.4f,%s,%d,%d,%d,%d,%0.3f,'\
				%(lon,lat,user,year,doy,dow,hour,senti_val)).replace(',',sep)+'%s\n'%txt)

	os.close(f) 

	
def auto_tweet(pg,auto,auto_txt,sep='\t'): 
	#process auto_tweet to extract full venue name (including punctuations)
	#export and write tweets of each venue	
	print '-- Export auto-tweet txt'
	pg.cur.execute('''
		select txt,lon,lat,username,year,doy,dow,hour,senti_val 
		from %s where auto_tweet='%s';'''%(pg.tbname,auto))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	txt=[data[i][0] for i in xrange(n)]
	
	print '-- Process venue name'
	name=process_venue_name(txt,auto)
	data=[data[i] for i in xrange(n) if name[i]!='']
	name=[name[i] for i in xrange(n) if name[i]!='']
	n=len(data)
	venues=list(set(name))
	print '---- Find %d venue names'%(len(venues))

	print '-- Filter auto-tweet of each venue'
	tweet=init_clusters(len(venues),venues)
	for venue in venues:
		tweet[venue]=[data[i] for i in xrange(n) if name[i]==venue]
		
	
	print '-- Write results'
	f=os.open('txt\\auto-tweet\\'+pg.tbname+'_%s_tweet.txt'%auto,os.O_RDWR|os.O_CREAT)
	os.write(f,'venue,lon,lat,user,year,doy,dow,hour,senti_val,txt\n'.replace(',',sep))


	for venue in venues:
		n=len(tweet[venue])
		for i in xrange(n):
			(txt,lon,lat,user,year,doy,dow,hour,senti_val)=tweet[venue][i]
			os.write(f,'%s'%venue+(',%0.4f,%0.4f,%s,%d,%d,%d,%d,%0.3f,'\
				%(lon,lat,user,year,doy,dow,hour,senti_val)).replace(',',sep)+'%s\n'%txt)

	os.close(f) 

def auto_venue(fname,maxDist=50,sep='\t'):
	#process venues by name and distance
	#write venue info and rewrite venue tweets
	print '-- Extract auto-tweet'
	data=file('txt\\auto-tweet\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	data=[[data[i][0].replace('&amp;','&')]+data[i][1:] for i in xrange(n)] #remove wronglt coded 'amp'

	data=[[remove_punctuation(data[i][0])]+data[i][1:] for i in xrange(n) if remove_punctuation(data[i][0])] 
	#remove punctuation and gap in venue name
	n=len(data)
	venues=list(set([data[i][0] for i in xrange(n)])) #list of processed venue names

	print '-- Init venue book'
	book=init_clusters(len(venues),venues)
	for i in xrange(n):
		book[data[i][0]]+=[data[i][1:]]
	
	print 'Search and redefine diff venues with same name'
	for venue in venues:
		 if find_diff_venue(book[venue],maxDist):
		 	[book,n]=redef_venue(book,venue,maxDist)
		 	print '---- %d venues named %s'%(n,venue)

	print '-- Merge venues with similar name and proximity'
	venues=book.keys()
	while len(venues)>1:
		venue_0=venues[0]
		venue_remove=[venue_0]
		for venue in venues[1:]:
			if to_merge(venue_0,venue,book,maxDist):				
				name=merge_venue_name(venue_0,venue)
				print '---- merge %s and %s by %s'%(venue_0,venue,name)
				book[name]=book[venue]+book[venue_0]
				del book[venue]
				venue_remove+=[venue]
		if len(venue_remove)>1:
			del book[venue_0]
		for venue in venue_remove:
			venues.remove(venue)

	
	print '-- Write new tweet txt'
	f=os.open('txt\\auto-tweet\\'+fname+'_new.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'venue,lon,lat,user,year,doy,dow,hour,senti_val,txt\n'.replace(',',sep))

	venues=book.keys()
	for venue in venues:
		tweets=book[venue]
		lon=np.mean([float(tweet[0]) for tweet in tweets])
		lat=np.mean([float(tweet[1]) for tweet in tweets])
		for tweet in tweets:
			os.write(f,sep.join(['%s'%venue,'%0.4f'%lon,'%0.4f'%lat]+tweet[2:])+'\n')

	os.close(f)


	print '-- Process venue stats'	
	stats=init_clusters(len(venues),venues)
	for venue in venues:
		stats[venue]=process_venue_stats(book[venue])

	print '-- Sort venue by check-in'
	checkin=[stats[venue][2] for venue in venues]
	idx=np.argsort(checkin)[::-1]
	venues=[venues[i] for i in idx]

	print '-- Write results'
	f=os.open('txt\\auto-tweet\\'+fname[:5]+fname[5:].replace('tweet','venue')+'.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'venue,lon,lat,checkin_n,user_n,doy_mode,dow_mode,hour_mode,senti_avg\n'.replace(',',sep))

	for venue in venues:
		(lon,lat,checkin,user,doy,dow,hour,senti)=stats[venue]
		os.write(f,'%s'%venue+(',%0.4f,%0.4f,%d,%s,%d,%d,%d,%0.3f\n'\
			%(lon,lat,checkin,user,doy,dow,hour,senti)).replace(',',sep))

	os.close(f) 

def auto_venue_map(fname,minCheckin=1,sep='\t'): #draw map and write gis shapefile of venues with minCheckin
	print 'Extract 4sq venues'
	data=file('txt\\auto-tweet\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	data=[data[i] for i in xrange(n) if int(data[i][3])>minCheckin]
	
	print 'Draw png map'
	draw_venue_map(fname,data)

	print 'Write shp file'
	write_venue_shp(fname,data)

def auto_err_tweet(fn_venue,fn_tweet,sep='\t',thresh=0.0001): #search venue with non-zero senti
	print '-- Extract venues and search venues with error senti'
	data=file('txt\\auto-tweet\\'+fn_venue+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	venues=[tuple(data[i][:3]) for i in xrange(n) if abs(float(data[i][-1])-0)>thresh]

	print '-- Extract venue tweet'
	data=file('txt\\auto-tweet\\'+fn_tweet+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	
	
	print '-- Export auto-tweet of err venues'
	tweet=init_clusters(len(venues),venues)
	venue_remove=[]
	for venue in venues:
		(name,lon,lat)=venue
		coordi=(float(lon),float(lat))
		tweet[venue]=[data[i] for i in xrange(n) if data[i][0]==name]
		if len(tweet[venue])==0:
			print '---- venue %s no tweet found'%venue[0]
			del tweet[venue]
			venue_remove+=[venue]

	for venue in venue_remove:
		venues.remove(venue)

	print '-- Write results'
	f=os.open('txt\\auto-tweet\\'+fn_tweet.replace('new','err_senti')+'.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'venue,lon,lat,senti,txt\n'.replace(',',sep))

	for venue in venues:
		tweets=tweet[venue]
		for t in tweets:
			os.write(f,sep.join(list(venue)+t[-2:])+'\n')		

	os.close(f) 



'''--- HELPER FUNCTION ---'''
def init_clusters(k,names,vals=False):
	d=dict()
	for i in xrange(k):
		key=names[i]
		if vals:
			d[key]=vals[i]
		else:	
			d[key]=[]
	return d

def color_generator(k):
	cache=cache_pre()
	if k<= len(cache):
		color=cache[:k]
		return color
	else:
		print "Number of clusters exceeds cache"
		return 

def cache_pre():
	cache=colors.ColorConverter.cache
	unique_color=[]
	for color in cache:
		if (cache[color] not in unique_color) and (cache[color]!=(0.0,0.0,0.0)) and (cache[color]!=(1.0,1.0,1.0)):
			unique_color+=[color]
	return unique_color

def remove_punctuation(txt):
	if len(txt)==0:
		return False
	elif txt[0] in punctuation:
		return remove_punctuation(txt[1:])
	elif txt[-1] in punctuation:
		return remove_punctuation(txt[:-1])
	else:
		return txt.strip() 

'''------------------------------------------------------------------'''
def process_venue_name(txt,auto):
	#format: I'm at .... - for #activity/#neighborhood ... 
	#(Pittsburgh/#other_city_name, PA)/in Pittsburgh/#orther_city_name,PA w/ #number others
	venues=[]
	if auto=='4sq':
		for tweet in txt:
			tweet=tweet.replace("I'm at ",'')
			tweet=tweet.strip()
			if len(tweet)>0:				
				tweet=tweet.split(' ') 
				i=0
				venue=''
				while i<len(tweet):
					if tweet[i]=='-':
						break
					elif '(' in tweet[i]:
						break
					elif tweet[i]=='w/':
						break
					elif tweet[i]=='in':
						break
					elif tweet[i]=='for':
						break
					else:
						venue+=' %s'%tweet[i]		
					i+=1
				venues+=[venue.strip()]
	elif auto=='inst':
		for tweet in txt:
			tweet=tweet.replace('Just posted a photo ','')
			tweet=tweet.strip()
			if len(tweet)>0:
				tweet=tweet.split(' ')
				i=0
				venue=''
				while i<len(tweet):
					if tweet[i][-1]==',' or tweet[i][-1]=='.':
						venue+='%s'%tweet[i][:-1]
						break
					elif tweet[i]=='-':
						break
					elif '(' in tweet[i]:
						break
					else:
						venue+=' %s'%tweet[i]
					i+=1
				venues+=[venue.strip()]

	return venues

'''------------------------------------------------------------------'''
def find_diff_venue(data,d):
	n=len(data)
	coordi_0=(float(data[0][0]),float(data[0][1]))
	i=1
	while i<n:
		coordi=(float(data[i][0]),float(data[i][1]))
		if dist(coordi,coordi_0)>d:
			return True
		i+=1
	return False

def redef_venue(book,venue,d):
	data=book[venue]
	n=len(data)
	coordis=venue_coordis([(float(data[i][0]),float(data[i][1])) for i in xrange(n)],d)
	m=len(coordis)
	for i in xrange(m):
		coordi=coordis[i]
		name='%s_%d'%(venue,i+1)
		book[name]=[data[j] for j in xrange(n) if dist((float(data[j][0]),float(data[j][1])),coordi)<d]
	del book[venue]

	return book,m

def venue_coordis(coordis,d):
	venues=[[coordis[0],1]]
	for coordi in coordis[1:]:
		new=True
		for venue in venues:
			if dist(coordi,venue[0])<d:
				(lon_0,lat_0)=venue[0]
				n=venue[1]
				(lon,lat)=coordi
				venue[0]=((lon_0*n+lon)/(n+1),(lat_0*n+lat)/(n+1))
				venue[1]+=1
				new=False			
		if new:
			venues+=[[coordi,1]]

	return [venue[0] for venue in venues]

def dist(coordi1,coordi2): #exchange (lon,lat) to (lat,lon)
	coordi1=(coordi1[1],coordi2[0])
	coordi2=(coordi2[1],coordi2[0])
	
	return great_circle(coordi1,coordi2).meters

def to_merge(venue_0,venue,book,d):
	if merge_venue_name(venue_0,venue):
		coordi_0=venue_center(book[venue_0])
		coordi=venue_center(book[venue])
		if dist(coordi_0,coordi)<d:
			return True
	return False
	
def merge_venue_name(venue_0,venue):
	name=venue[:venue.find('_')] if '_' in venue else venue
	name_0=venue_0[:venue_0.find('_')] if '_' in venue_0 else venue_0
	#contain or be contained
	if name_0.find(' ')>0 and name in name_0: 
		return venue_0
	elif name.find(' ')>0 and name_0 in name:
		return venue
	#partly match
	else:
		name=name.split(' ')
		name_0=name_0.split(' ')
		if len(name)+len(name_0)-len(set(name+name_0))>1: #at least two words match
			l=name if len(name)>len(name_0) else name_0
			return ' '.join(l)
	return False

def process_venue_stats(data):
	n=len(data)
	(lon,lat)=venue_center(data)
	checkin=n
	user=len(list(set([data[i][2] for i in xrange(n)])))
	[year,doy,dow,hour]=[stats.mode([int(data[i][j]) for i in xrange(n)])[0][0] for j in xrange(3,7)]
	senti=np.mean([float(data[i][-2]) for i in xrange(n)])

	return [lon,lat,checkin,user,doy,dow,hour,senti]

def venue_center(data):
	n=len(data)
	lon=np.mean([float(data[i][0]) for i in xrange(n)])
	lat=np.mean([float(data[i][1]) for i in xrange(n)])
	return (lon,lat)

'''------------------------------------------------------------------'''
def draw_venue_map(fname,data):
	plt.figure()
	n=len(data)
	x=[float(data[i][1]) for i in xrange(n)]
	y=[float(data[i][2]) for i in xrange(n)]
	plt.scatter(x,y,marker='o')
	plt.xlabel('lon')
	plt.ylabel('lat')
	plt.xlim(-80.10,-79.86)
	plt.ylim(40.36,40.51)
	plt.title('autouare Venues')
	plt.savefig('png\\'+fname+'.png')

def write_venue_shp(fname,data):
	fields=[['NAME','C',50],['CHECKIN','N',8],['USER','N',8]]
	f=shapefile.Writer(shapeType=1)
	f.autoBalance = 1
	for field in fields:
		f.field(field[0],field[1],field[2])

	n=len(data)
	for i in xrange(n):
		name=data[i][0]
		lon=float(data[i][1])
		lat=float(data[i][2])
		checkin=int(data[i][3])
		user=int(data[i][4])
		f.point(lon,lat)
		f.record(NAME=name,CHECKIN=checkin,USER=user)
	f.save('shp\\'+fname+'.shp')	

'''------------------------------------------------------------------'''
def export_err_tweet(pg,name,coordi,auto,auto_txt,d=0.02,thresh=0.0001):
	name=name.replace("'","''")
	query=create_venue_query(name,auto_txt)
	pg.cur.execute('''
		select lon,lat,txt,senti_val from %s 
		where auto_tweet='%s' and %s and abs(senti_val-0)>%f;
		'''%(pg.tbname,auto,query,thresh))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	coordis=[(data[i][0],data[i][1]) for i in xrange(n)]
	data=[data[i][2:] for i in xrange(n) if dist(coordis[i],coordi)<d]
	n=len(data)

	tweet=[]
	if len(data)>0:
		txts=list(set([data[i][0] for i in xrange(n)]))		
		for txt in txts:
			senti=np.mean([data[i][1] for i in xrange(n) if data[i][0]==txt])
			tweet+=[[txt,senti]]

	return tweet
	
		
def create_venue_query(name,txt):
	name=name.split(' ')
	n=len(name)
	if n==1:
		return "txt like '%%%s%s%%'"%(txt,name[0])
	else:
		query="("
		for i in xrange(n-1):
			pair=' '.join([name[i],name[i+1]])
			query+="txt like '%%%s%s%%' or "%(txt,pair)
		return query[:-4]+")"


'''--- TEST FUNCTION ---'''
'''
txt=["I'm at AMC Loews Waterfront 22 - for The LEGO Movie (Homestead, PA) w/ 3 others",
"I'm at Goodwill / ComputerWorks - Lawrenceville - (Pittsburgh, PA)",
"I'm at Sienna Mercato w/",
"I'm at AMC Loews Waterfront 22 - for The LEGO Movie (Homestead, PA) w/ 3 others"]

print process_venue_name(txt)

print set([(0,1),(0,2),(1,2),(1,2)])

a=dict()
a[(0,1)]=[0]
print a

print merge_venue_name("Pittsburgh_9","Yolanda's Hair Design")
print dist((-80.0039,40.4468),(-79.9843,40.4514))

print create_venue_query('AMC Loews Waterfront 22')

'''