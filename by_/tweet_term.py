import tf_idf
import nltk.tag, lda, os, copy
import numpy as np 
import preprocessing as pre
from nltk.tag.perceptron import PerceptronTagger
from collections import Counter

def process_term_cls(fname,sep='\t'): #process cluster-tweet terms into cobo of uni-/bi-/tri-grams
	print 'Extract cluster-tweet terms'
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	var=data[0]
	data=data[1:]

	n=len(data)
	idx=var.index('term')	
	clid=[int(data[i][0]) for i in xrange(n)]
	term=[data[i][idx].split(',') for i in xrange(n) if clid[i]!=-1] #exclude noise data
	clid=[clid[i] for i in xrange(n) if clid[i]!=-1]

	print 'Process tweet terms'
	tagger=PerceptronTagger()
	n=len(term)	
	term=[process_term(term[i],tagger) for i in xrange(n)]

	print 'Write results'
	f=os.open('txt\\'+fname+'_terms.txt', os.O_RDWR|os.O_CREAT)
	os.write(f,sep.join(['clid','term'])+'\n')
	for i in xrange(n):
		os.write(f,sep.join(['%d'%clid[i],','.join(term[i])])+'\n')

	os.close(f)

def common_term(fname,m,rand_n=400,sep='\t'): 
	#seach common terms in happy tweets, rand sample of cluster and noise
	print 'Extract cluster-tweet terms'
	data=file('txt\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	term=[data[i][1].split(',') for i in xrange(n)]
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


def common_term_cls(fname,m,cls=False,sep='\t'): #seach common/representative terms of clusters
	print 'Extract cluster-tweet terms'
	data=file('txt\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	clid=[int(data[i][0]) for i in xrange(n)]
	term=[data[i][1].split(',') for i in xrange(n)]

	print 'Process term clusters'
	if not cls:
		cls=range(max(clid)+1)		
	term_cl=init_clusters(len(cls),cls)

	for cl in cls:
		term_cl[cl]=[term[i] for i in xrange(n) if clid[i]==cl]

	print 'Count term tf'
	counter=Counter()
	counter=tf_idf.tf(counter,term,'term')

	print 'Remove common terms'
	data=file('txt\\'+fname+'_common.txt').readlines()[1:]
	l=len(data)
	data=[data[i][:-1].split(',') for i in xrange(l)]
	common=set([data[i][0] for i in xrange(l)])
	term_remove=[]
	for t in counter.keys():
		if t in common:
			term_remove+=[t]
	for t in term_remove:
		del counter[t]

	#print "'another alive in counter"
	#print 'another alive' in counter

	print 'Calculate term tf-idf'
	counter_cl=dict()
	for cl in cls:
		counter_i=copy.copy(counter)
		counter_i.subtract(counter)
		#if cl==17:
			#print "'another alive in counter_i"
			#print 'another alive' in counter_i
		counter_cl[cl]=tf_idf.tf(counter_i,term_cl[cl],'term')
	tfIdf=dict()
	for cl in cls:
		tfIdf[cl]=tf_idf.tf_idf(counter,counter_cl[cl],n)


	print 'remove shared term and self-merging'
	mtx=[tfIdf[cl][1] for cl in cls]
	term=tfIdf[cls[0]][0]
	mtx=tf_idf.screen(term,mtx,m,type='term')

	print 'Write results'
	f=os.open('txt\\'+fname+'_tfIdf.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,','.join(['cluster%d,'%cl for cl in cls])+'\n')
	os.write(f,','.join(['term,tf-idf' for cl in cls])+'\n')

	k=len(cls)
	mtx=[fill_empty(list(mtx[i]),m,'f') for i in xrange(k)]	
	for i in xrange(m):
		os.write(f,','.join(['%s,%0.3f'%(mtx[j][i][0],mtx[j][i][1]) for j in xrange(k)])+'\n')

	os.close(f)

def common_term_cls_senti(fn_term,fn_data,cls=False,sep='\t'):
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
	clid=[int(data[i][0]) for i in xrange(n)]
	senti=[float(data[i][-1]) for i in xrange(n)]
	idx=var.index('term')
	term=[data[i][idx] for i in xrange(n)]
	data_cl=init_clusters(len(cls),cls)
	
	print 'Process term clusters'
	for cl in cls:
		data_cl[cl]=[[term[i],senti[i]] for i in xrange(n) if clid[i]==cl]

	print 'Calculate cluster senti scales'
	scales=[np.mean([senti[i] for i in xrange(n) if clid[i]==cl]) for cl in cls]
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
def common_term_senti(term,data):
	[m,n]=[len(term),len(data)]
	senti=[[term[i],0.0] for i in xrange(m)]
	for i in xrange(m):
		if term[i]!='':
			s=[data[j][1] for j in xrange(n) if term[i] in data[j][0]]
			if s==[]:
				print term[i]
			else:
				senti[i][1]=np.mean(s)

	#senti_min=min([senti[i][1] for i in xrange(m) if senti[i][1]!=0])
	#senti=[(senti[i][0],senti[i][1]/senti_min) for i in xrange(m)]

	return senti

'''TEST FUNCTION'''
tagger=PerceptronTagger()
data=['peace','love','little','donuts']
print process_term(data,tagger)