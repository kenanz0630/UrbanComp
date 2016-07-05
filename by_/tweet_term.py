import tf_idf
import nltk.tag, lda, os, copy
import numpy as np 
import preprocessing as pre
from nltk.tag.perceptron import PerceptronTagger
from collections import Counter

def process_term_cls(fname,senti=False,sep='\t'): #process cluster-tweet terms into cobo of uni-/bi-/tri-grams
	print 'Extract cluster-tweet terms'
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	var=data[0]
	data=data[1:]

	n=len(data)
	idx=var.index('term')	
	clid=[int(data[i][0]) for i in xrange(n)]
	happy=[int(data[i][-1]) for i in xrange(n)]
	term=[data[i][idx].replace(',&,amp,',',&,') for i in xrange(n)]
	term=[t.split(',') for t in term] 
	#replace ,&,amp, by ,&, 
	for i in xrange(len(term)):
		if 'amp' in term[i]:
			term[i].remove('amp')


	#exclude noise data
	if senti is False:
		term=[term[i] for i in xrange(n) if (clid[i]!=-1 and happy[i]!=0)]
		idx=var.index('user')
		user=[data[i][idx] for i in xrange(n) if (clid[i]!=-1 and happy[i]!=0)]
		clid=[clid[i] for i in xrange(n) if (clid[i]!=-1 and happy[i]!=0)]
	else:	
		term=[term[i] for i in xrange(n) if (clid[i]!=-1 and happy[i]==senti)]
		idx=var.index('user')
		user=[data[i][idx] for i in xrange(n) if (clid[i]!=-1 and happy[i]==senti)]
		clid=[clid[i] for i in xrange(n) if (clid[i]!=-1 and happy[i]==senti)]
		
	
	n=len(term)
	print 'Process tweet terms'
	tagger=PerceptronTagger()		
	term=[process_term(term[i],tagger) for i in xrange(n)]

	print 'Write results'
	if senti is False:
		ext=''
	else:
		ext='_pos' if senti==1 else '_neg'
	f=os.open('txt\\'+fname+'_term%s.txt'%ext, os.O_RDWR|os.O_CREAT)

	os.write(f,sep.join(['clid','user','term'])+'\n')
	for i in xrange(n):
		os.write(f,sep.join(['%d'%clid[i],user[i],','.join(term[i])])+'\n')

	os.close(f)

def common_term(fname,m,rand_n=400,sep='\t'): 
	#seach common terms in happy tweets, rand sample of cluster and noise
	print 'Extract cluster-tweet terms'
	data=file('txt\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	term=[data[i][-1].split(',') for i in xrange(n)] 
	clid=[int(data[i][0]) for i in xrange(n)]

	k=max(clid)+1
	term_cl=init_clusters(k+1,range(-1,k),[[term[i] for i in xrange(n) if clid[i]==j] for j in xrange(-1,k)])

	print 'Random sampling of cluster-tweet and noise'
	term_r=[]
	for cl in xrange(-1,k):
		term_r+=list(np.random.permutation(term_cl[cl])[:rand_n])

	print 'Count terms'
	counter=Counter()
	counter=tf_idf.tf(counter,term_r,type='term')
	common=counter.most_common()[:m]

	print 'Write results'
	f=os.open('txt\\'+fname+'_common.txt', os.O_RDWR|os.O_CREAT)
	os.write(f,'term,count\n')
	for i in xrange(m):
		os.write(f,'%s,%d\n'%(common[i][0],common[i][1]))

	os.close(f)

def common_term_cls(fname,m,cls=False,top_user=100,sep='\t'): 
	#seach common/representative terms of clusters by tf-idf algo
	'''logic should be finding terms that are special for the cluster 
		but not for one user posting tweets in the cluster'''
	
	print 'Extract cluster-tweet terms'
	data=file('txt\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	clid=[int(data[i][0]) for i in xrange(n)]
	user=[data[i][1] for i in xrange(n)]
	term=[data[i][-1].split(',') for i in xrange(n)]
	
	print 'Process term by clusters and users'
	if not cls:
		cls=range(max(clid)+1)		
	term_cl=init_clusters(len(cls),cls)
	user_cl=init_clusters(len(cls),cls)	
	for cl in cls:
		term_cl[cl]=[term[i] for i in xrange(n) if clid[i]==cl]
		user_cl[cl]=[user[i] for i in xrange(n) if clid[i]==cl]


	print 'Count all-term tf'
	counter=Counter()
	counter=tf_idf.tf(counter,term,'term')
	
	print 'Remove common terms'
	data=file('txt\\'+fname+'_common.txt').readlines()[1:]
	l=len(data)
	data=[data[i][:-1].split(',') for i in xrange(l)]
	common=set([data[i][0] for i in xrange(l)])
	term_remove=[]
	for key in counter.keys():
		l=key.split(' ')
		if l[0] in common or l[-1] in common: #first/last word is common term
			term_remove+=[key]
	for key in term_remove:
		del counter[key]

	tfidf_cl=init_clusters(len(cls),cls)
	for cl in cls:
		print 'Calculate term and user tf-idf -- Cluster%d'%cl
		counter_copy=copy.copy(counter)
		remove_copy=copy.copy(term_remove)
		tfidf_cl[cl]=tf_idf_cl(term_cl[cl],user_cl[cl],counter_copy,top_user,n,remove_copy)

	
	print 'Remove shared term and self-merging'
	mtx=[tfidf_cl[cl] for cl in cls]
	mtx=tf_idf.screen(mtx,m,type='term')

	print 'Write results'
	f=os.open('txt\\'+fname+'_tfIdf.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,','.join(['cluster%d,'%cl for cl in cls])+'\n')
	os.write(f,','.join(['term,tf-idf' for cl in cls])+'\n')

	k=len(cls)
	mtx=[fill_empty(list(mtx[i]),m,'f') for i in xrange(k)]	
	for i in xrange(m):
		os.write(f,','.join(['%s,%0.3f'%(mtx[j][i][0],mtx[j][i][1]) for j in xrange(k)])+'\n')

	os.close(f)


def common_term_cls_senti(fn_term,fn_data,senti=False,cls=False,sep='\t'):
	print 'Extract cluster-tweet common terms'
	data=file('txt\\'+fn_term+'.txt').readlines()[2:]
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	k=len(data[0])/2
	term=[[data[i][2*j] for i in xrange(n)] for j in xrange(k)]
	m=len(term[0])

	if not cls:
		cls=range(k)		
	term_cl=[]
	for i in xrange(k):
		if i in cls:
			term_cl+=[[t.replace(' ',',') for t in term[i]]]
	term_cl=init_clusters(len(cls),cls,term_cl)
	
	print 'Extract cluster-tweet data'
	data=file('txt\\'+fn_data+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	var=data[0]
	data=data[1:]
	n-=1

	if senti is False:
		clid=[int(data[i][0]) for i in xrange(n)]
		senti_val=[abs(float(data[i][-2])) for i in xrange(n)]
		idx=var.index('term')
		term=[data[i][idx] for i in xrange(n)]
	else:
		clid=[int(data[i][0]) for i in xrange(n) if int(data[i][-1])==senti]
		senti_val=[float(data[i][-2]) for i in xrange(n) if int(data[i][-1])==senti]
		idx=var.index('term')
		term=[data[i][idx] for i in xrange(n) if int(data[i][-1])==senti]	

	
	print 'Process term clusters'
	data_cl=init_clusters(len(cls),cls)
	n=len(clid)
	for cl in cls:
		data_cl[cl]=[[term[i],senti_val[i]] for i in xrange(n) if clid[i]==cl]

	print 'Calculate cluster senti scales'
	scales=[np.mean([senti_val[i] for i in xrange(n) if clid[i]==cl]) for cl in cls]
	#senti_min=min(scales)
	#scales=[scale/senti_min for scale in scales]

	print 'Calculate common term senti'
	senti_cl=init_clusters(len(cls),cls)	
	for cl in cls:
		senti_cl[cl]=common_term_senti(term_cl[cl],data_cl[cl])

	
	print 'Write results'
	f=os.open('txt\\'+fn_term.replace('tfIdf','senti')+'.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,','.join(['cluster%d,'%cl for cl in cls])+'\n')
	os.write(f,','.join(['term,senti' for cl in cls])+'\n')

	k=len(cls)
	mtx=[senti_cl[cl] for cl in cls]
	#mtx=[fill_empty(senti_cl[cl],m,'f') for cl in cls]
	for i in xrange(m):
		os.write(f,','.join(['%s,%0.3f'%(mtx[j][i][0].replace(',',' '),mtx[j][i][1]) for j in xrange(k)])+'\n')
	os.write(f,','.join([',%0.3f'%scale for scale in scales])+'\n')

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

def fill_empty(data,m,type):
	n=len(data)
	if n<m:
		if type=='f':
			data+=[('',0.0) for j in xrange(m-n)]
		if type=='i':
			data+=[('',0) for j in xrange(m-n)]

	return data

'''------------------------------------------------------------------'''
def process_term(data,tagger):
	try:
		data_tag=nltk.tag._pos_tag(data,None,tagger)
	except:
		data_tag=[('',None)]
	data_uni=data 
	data_bi=n_gram(data_tag,2) 
	data_tri=n_gram(data_tag,3) 
	data=set(data_uni+data_bi+data_tri)

	return list(data)

def n_gram(data,n):
	data_n=[]
	i=n
	while i<=len(data):
		tag=[]
		for j in xrange(i-n,i):
			tag+=[data[j][1]]
		if 'NN' in tag or 'NNS' in tag:
			data_n+=[' '.join([data[k][0] for k in xrange(i-n,i)])]
		i+=1
	return data_n


'''------------------------------------------------------------------'''
def tf_idf_cl(term_cl,user,counter,top_user,N,term_remove):
	''''''
	names=list(set(user))
	m=len(user)
	count=[len([user[i] for i in xrange(m) if user[i]==name]) for name in names]
	idx=np.argsort(count)[::-1]
	if top_user>len(idx):
		top_user=len(idx)
		print '---- cluster has less than %d unique users'%top_user
	names=[names[i] for i in idx[:top_user]]
	term_user=init_clusters(len(names),names)	
	for name in names:
		term_user[name]=[term_cl[i] for i in xrange(m) if user[i]==name]
	''''''
	print '-- Count cls-term tf'
	counter_i=copy.copy(counter)
	counter_i.subtract(counter)
	counter_term=tf_idf.tf(counter_i,term_cl,'term')

	''''''
	print '-- Count user-term tf'
	counter_user=init_clusters(len(names),names)
	for name in names:
		counter_i=copy.copy(counter_term)
		counter_i.subtract(counter_term)
		counter_user[name]=tf_idf.tf(counter_i,term_user[name],'term')
	''''''
	print '-- Clean counters'
	if '' in counter.keys():
		del counter['']
		del counter_term['']
		''''''
		for name in names:
			del counter_user[name]['']
		''''''
	
	for term in counter_term:
		if counter_term[term]==0:
			term_remove+=[term]
	for term in term_remove:
		del counter_term[term]
		''''''
		for name in names:
			del counter_user[name][term]
		''''''

	
	print '-- Calculate cls-term tfidf'
	term_tfidf=tf_idf.tf_idf(counter,counter_term,N)
	''''''
	print '-- Calculate user-term tfidf'
	user_tfidf=init_clusters(len(names),names)
	for name in names:
		user_tfidf[name]=tf_idf.tf_idf(counter_term,counter_user[name],m)
	''''''
	print '-- Calculate term norm-tfidf'
	#user_tfidf=[max([user_tfidf[name][term] for name in names]) for term in term_tfidf]
	user_tfidf=[np.std([user_tfidf[name][term] for name in names]) for term in term_tfidf]
	#user_tfidf=[0 for term in term_tfidf]

	term=term_tfidf.keys()
	n=len(term)
	tfidf=[term_tfidf[term[i]]/(1+user_tfidf[i]) for i in xrange(n)]
	
	return [(term[i],tfidf[i]) for i in xrange(n)]

def tf_idf_test(fname,cl=0,sep='\t'):
	data=file('txt\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	user=[data[i][1] for i in xrange(n) if int(data[i][0])==cl]
	term=[data[i][-1].split(',') for i in xrange(n)]
	term_cl=[term[i] for i in xrange(n) if int(data[i][0])==cl]

	names=list(set(user))
	m=len(user)
	count=[len([user[i] for i in xrange(m) if user[i]==name]) for name in names]
	idx=np.argsort(count)[::-1]
	#N=int(len(idx)*0.1)
	N=50
	names=[names[i] for i in idx[:N]]
	term_user=init_clusters(len(names),names)	
	for name in names:
		term_user[name]=[term_cl[i] for i in xrange(m) if user[i]==name]

	print 'Count all tf'
	counter=Counter()
	counter=tf_idf.tf(counter,term,'term')

	print 'Count cl tf'
	counter_i=copy.copy(counter)
	counter_i.subtract(counter)
	counter_term=tf_idf.tf(counter_i,term_cl,'term')

	remove=[]
	for term in counter_term:
		if counter_term[term]==0:
			remove+=[term]
	for term in remove:
		del counter_term[term]

	print 'Count user tf'
	print '#_name',len(names)
	counter_user=init_clusters(len(names),names)
	for name in names:
		print '-- %s'%name
		counter_i=copy.copy(counter_term)
		counter_i.subtract(counter_term)
		counter_user[name]=tf_idf.tf(counter_i,term_user[name],'term')
	
	print 'Calculate cl tfidf'
	term_tfidf=tf_idf.tf_idf(counter,counter_term,n)

	print 'Calculate user tfidf'
	user_tfidf=init_clusters(len(names),names)
	for name in names:
		user_tfidf[name]=tf_idf.tf_idf(counter_term,counter_user[name],m)
	
	print 'Sort tfidf'
	user_tfidf=[max([user_tfidf[name][term] for name in names]) for term in term_tfidf]
	#user_tfidf=[np.std([user_tfidf[name][term] for name in names]) for term in term_tfidf]
	
	term=term_tfidf.keys()
	n=len(term)
	tfidf=[term_tfidf[term[i]]/(1+user_tfidf[i]) for i in xrange(n)]
	term_tfidf=[term_tfidf[term[i]] for i in xrange(n)]

	f=os.open('txt\\tfidf_test.txt', os.O_RDWR|os.O_CREAT)
	idx=np.argsort(term_tfidf)[::-1]	
	os.write(f,'term_tfidf,'+','.join([term[i] for i in idx[:10]])+'\n')
	os.write(f,','+','.join(['%0.4f'%term_tfidf[i] for i in idx[:10]])+'\n')

	idx=np.argsort(user_tfidf)[::-1]	
	os.write(f,'user_tfidf,'+','.join([term[i] for i in idx[:10]])+'\n')
	os.write(f,','+','.join(['%0.4f'%user_tfidf[i] for i in idx[:10]])+'\n')
	
	idx=np.argsort(tfidf)[::-1]	
	os.write(f,'tfidf,'+','.join([term[i] for i in idx[:10]])+'\n')
	os.write(f,','+','.join(['%0.4f'%tfidf[i] for i in idx[:10]])+'\n')
	
	os.close(f)

'''------------------------------------------------------------------'''

def common_term_senti(term,data):
	[m,n]=[len(term),len(data)]
	senti=[[term[i],0.0] for i in xrange(m)]
	for i in xrange(m):
		if term[i]!='':
			s=[data[j][1] for j in xrange(n) if term[i] in data[j][0]]
			if s==[]:
				if term.count(',')>3: #four-gram or more
					trunc_term=truncate(term[i])
					for trunc in trunc_term:
						s+=[len([data[j][1]] for j in xrange(n) if trunc in data[j][0])]
					idx=s.index(max(s))
					print '-- cannot find %s, replace by %s'\
						%(term[i].replace(',',' '),trunc_term[idx].replace(',',' '))
					s=[data[j][1] for j in xrange(n) if trunc_term[idx] in data[j][0]]
					senti[i]=[trunc_term[idx],np.mean(s)]
				else:
					print '-- cannot find %s'%term[i].replace(',',' ')
			else:
				senti[i][1]=np.mean(s)

	#senti_min=min([senti[i][1] for i in xrange(m) if senti[i][1]!=0])
	#senti=[(senti[i][0],senti[i][1]/senti_min) for i in xrange(m)]

	return senti

def truncate(term): #truncate term into tri-grams
	term=term.split(',')
	n=len(term)
	trunc=[]
	for i in xrange(n-2):
		trunc+=[','.join(term[i:i+3])]

	return trunc

'''TEST FUNCTION'''
'''
print truncate('taylor,swift,heinz,stadium')

tagger=PerceptronTagger()
data=['peace','love','little','donuts']
print process_term(data,tagger)
'''

