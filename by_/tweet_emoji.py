import dbProcess, os, re
import tf_idf 
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter

def common_emoji_tb(tbname,dbname,m,user='postgres'):
	#export common emoji of all/pos/neg tweets
	print "Create pgcontroller"
	pg=dbProcess.PgController(tbname,dbname,user)
	
	print 'Export emoji from table %s'%tbname
	print '>> Emoji of all tweets'
	pg.cur.execute('select emoji from %s where emoji is not Null;'%tbname)
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	data=[data[i][0] for i in xrange(n)]

	print '>> Emoji of positive tweets'
	pg.cur.execute('select emoji from %s where emoji is not Null and senti_bool=1;'%tbname)
	pos=pg.cur.fetchall()
	n=pg.cur.rowcount
	pos=[pos[i][0] for i in xrange(n)]

	print '>> Emoji of negative tweets'
	pg.cur.execute('select emoji from %s where emoji is not Null and senti_bool=-1;'%tbname)
	neg=pg.cur.fetchall()
	n=pg.cur.rowcount
	neg=[neg[i][0] for i in xrange(n)]

	data=[data,pos,neg]

	print 'Count emoji tf'
	counter=[Counter() for i in xrange(3)]
	emoji_dict=init_emoji()
	counter=[tf_idf.count(data[i],counter[i],ref=emoji_dict,type='emoji') for i in xrange(3)]
	
	print 'Most common emojis and remove same ones'
	emoji=[counter[i].most_common(m) for i in xrange(3)]
	emoji_pos=[]
	emoji_neg=[]
	emoji_all=[emoji[0][i][0] for i in xrange(m)]
	for i in xrange(m):
		if emoji[1][i][0] not in emoji_all:
			emoji_pos+=[emoji[1][i]]
		if emoji[2][i][0] not in emoji_all:
			emoji_neg+=[emoji[2][i]]
	(m_pos,m_neg)=(len(emoji_pos),len(emoji_neg))
	emoji[1]=emoji_pos+[('',0) for i in xrange(m_pos,m)]
	emoji[2]=emoji_neg+[('',0) for i in xrange(m_neg,m)]


	print 'Write results'
	write_common_emoji_tb(tbname,emoji)

def common_emoji_cl(fname,m,names=False):
	#high tf-idf emoji of each cluster
	print 'Extract terms from %s'%fname
	[emoji,emoji_cl]=extract_data(fname)
	N=len(emoji)
	k=len(emoji_cl)
	N_cl=[len(emoji_cl[i]) for i in xrange(1,k+1)]

	print 'Count emoji tf'
	counter=Counter()
	counter=tf_idf.count(emoji,counter,ref=emoji_dict,type='emoji')
	counter_cl=[tf_idf.tf(counter,emoji_cl[i+1],ref=emoji_dict,type='emoji') for i in xrange(k)]

	print 'Calculate cluster emoji tf-idf'	
	tfIdf=[tf_idf.tf_idf(counter,counter_cl[i+1],N,N_cl[i]) for i in xrange(k)]
	term=tfIdf[0][0]
	tfIdf=[tfIdf[i][1] for i in xrange(k)]

	print 'Write results'
	write_common_emoji_cl(fname,mtx,names)


def tweet_senti_compare(tbname,dbname,fname,user='postgres',sep='\t'):
	print "Create pgcontroller"
	pg=dbProcess.PgController(tbname,dbname,user)
	
	print 'Export tweets with emoji from %s'%tbname
	pg.cur.execute('select senti_val,term,emoji from %s where emoji is not Null;'%tbname)
	data=pg.cur.fetchall()
	n=pg.cur.rowcount

	(avg,std)=(0.03879,0.28125)
	tsenti_val=[(data[i][0]-avg)/std for i in xrange(n)]
	term=[data[i][1] for i in xrange(n)] 
	emoji=[unicode(data[i][-1],'utf-8') for i in xrange(n)]
	
	print 'Evaluate tweet sentiment by emoji'
	[idx,esenti_val]=emoji_senti(fname,emoji)

	
	print 'Write results'
	f=os.open('txt\\'+tbname+'_senti_compare.txt', os.O_RDWR|os.O_CREAT)
	l=['id','tsenti_val','esenti_val','term','emoji']
	os.write(f,sep.join(l)+'\n')
	
	m=len(idx)
	for i in xrange(m):
		l=['%d'%(i+1),'%0.3f'%tsenti_val[idx[i]],'%0.3f'%esenti_val[i],\
			'%s'%term[idx[i]],'%s'%emoji[idx[i]].encode('utf-8')]
		os.write(f,sep.join(l)+'\n')
	os.close(f)


	print 'Draw graph'
	plt.figure()
	x=esenti_val
	y=[tsenti_val[idx[i]] for i in xrange(m)]
	plt.scatter(x,y,marker='o')
	plt.xlabel('emoji_senti')
	plt.ylabel('txt_senti')
	plt.title('Tweet Sentiment Score Comparison')
	plt.savefig('png\\'+tbname+'_senti_compare.png')
	
	r=np.corrcoef([x,y])
	print r
	print 'Corrlation between emoji and txt senti val is %0.3f'%(r[0][1])

	print 'Draw hist'
	plt.figure()
	plt.hist(x,bins=50)
	plt.xlabel('emoji_senti')
	plt.yscale('log')
	plt.title('Tweet Emoji Sentiment Score Distribution')
	plt.savefig('png\\'+tbname+'_esenti_hist.png')

	plt.figure()
	plt.hist(y,bins=50)
	plt.xlabel('txt_senti')
	plt.yscale('log')
	plt.title('Tweet Text Sentiment Score Distribution')
	plt.savefig('png\\'+tbname+'_tsenti_hist.png')



'''--- HELPER FUNCTION ---'''
'''------------------------------------------------------------------'''
def init_emoji():
	try: # UCS-4
		highpoints = re.compile(u'[\U00010000-\U0010ffff]')
	except re.error: # UCS-2
		highpoints = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]')
	return highpoints

def init_clusters(k,names):
	d=dict()
	for i in xrange(k):
		key=names[i]
		d[key]=[]
	return d
'''------------------------------------------------------------------'''
def emoji_senti(fname,emoji):
	emoji_score=dict()
	data=file(fname+'.csv').readlines()
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	for i in xrange(n):
		emoji_score[data[i][0]]=float(data[i][1])

	emoji_dict=init_emoji()
	n=len(emoji)
	score=[0.0 for i in xrange(n)]
	idx=[]
	for i in xrange(n):
		line=re.findall(emoji_dict,emoji[i])
		count=0
		for e in line:
			if repr(e) in emoji_score:
				idx+=[i]
				count+=1
				score[i]+=emoji_score[repr(e)]
		if count:
			score[i]/=count

	idx=list(set(idx))

	m=len(idx)
	#score=[score[idx[i]] for i in xrange(m)]
	print '%d(%0.3f%%) tweets with senti-scored emoji'%(m,m*100.0/n)

	avg=np.mean(score)
	std=np.std(score)
	print 'Emoji sentiment avg:%0.3f std:%0.3f'%(np.mean(score),np.std(score))
	#score=[(score[i]-avg)/std for i in xrange(m)]
	#print 'Normalized emoji sentiment avg:%0.1f std:%0.1f'%(np.mean(score),np.std(score))
	print 'Emoji senti max:%0.3f, min: %0.3f'%(max(score),min(score))
	#r=max(score)-min(score)
	#score=[score[i]/r for i in xrange(m)]
	
	return idx,score

'''------------------------------------------------------------------'''
def write_common_emoji_tb(fname,emoji):
	m=len(emoji[0])
	f=os.open('txt\\'+fname+'_common_emoji_x%d.txt'%m, os.O_RDWR|os.O_CREAT)
	os.write(f,'all,,,pos,,,neg,,,\n')
	l=['emoji,,count,' for i in xrange(3)]
	os.write(f,''.join(l)+'\n')

	for j in xrange(m):
		l=['%s,%s,%d,'%(emoji[i][j][0].encode('utf-8'),repr(emoji[i][j][0]),emoji[i][j][1]) for i in xrange(3)]
		os.write(f,''.join(l)+'\n')
	os.close(f)

def write_common_emoji_cl(fname,mtx,names=False):
	f=os.open('txt\\'+fname+'_emoji_tfidf.txt', os.O_RDWR|os.O_CREAT)
	
	if names:
		cls=[name[0] for name in names]
		names=[name[1] for name in names]
	else:
		cls=range(1,len(mtx)+1)
		names=['cluster%d'%i for cl in cls]

	k=len(cls)
	l=['%s,,'%names[i] for i in xrange(k)]
	os.write(f,''.join(l)+'\n')
	l=['emoji,tf-idf,' for i in xrange(k)]
	os.write(f,''.join(l)+'\n')

	m=len(mtx[0])
	for i in xrange(m):
		l=['%s,%0.4f,'%(mtx[j][0],mtx[j][1]) for j in xrange(k)]
		os.write(f,''.join(l)+'\n')

	os.close(f)

'''--- TEST FUNCTION ---'''
'''
a=dict()
a['a']=1
a['b']=2
l=['a','c','a']
score=0
for i in l:
	if i in a:
		score+=a[i]
print score
'''
