from wordcloud import WordCloud
from wordcloud import get_single_color_func
import os
import matplotlib.pyplot as plt
import numpy as np
import colorsys
import dbscan
from PIL import ImageColor
import matplotlib.colors as colors


'''--- MAIN FUNCTION ---'''
def wordCloud_single(fname,name,color):
	print 'Extract data'
	[data,senti]=extract_data_single(fname)
	k=len(senti)

	print 'Process tf and senti'
	data=[process_data_single(data[i]) for i in xrange(k)]

	print 'Process colors'
	colors=process_color(senti,color)

	for i in xrange(k):
		key=name+'%d'%(i+1)
		print "Draw wordcloud of %s"%key
		draw_wordcloud_single(fname+'_%s'%key,key,colors[i],data[i])

def wordCloud(fname,cls,names,ext_tfIdf='_tfIdf',ext_senti='_senti'):
	print "Extract term"
	fn_tfIdf=fname+ext_tfIdf
	fn_senti=fname+ext_senti
	data=extract_data(fn_tfIdf,fn_senti,cls)
	k=len(cls)

	print 'Process tfIdf and senti'
	data=[process_data(data[cls[i]]) for i in xrange(k)]

	print 'Generate different colors'
	hues=color_generator(k)

	for i in xrange(k):
		key='cluster%d'%cls[i]
		name=names[i]
		print "Draw wordcloud of %s"%key
		print '>> hue %s'%hues[i]
		draw_wordcloud(fname+'_%s'%key,name,hues[i],data[i])
	

'''--- HELPER FUNCTION ---'''
def init_clusters(k,names):
	d=dict()
	for i in xrange(k):
		key=names[i]
		d[key]=[]
	return d

def color_generator(k):
	cache=colors.ColorConverter.cache
	hues=[]
	for key in cache.keys():
		if key[0]=='#' and (cache[key]!=(0.0,0.0,0.0)) and (cache[key]!=(1.0,1.0,1.0)):
			hues+=[key]

	return np.random.permutation(hues)[:k]

def my_color_func(color):
	print color
	old_r, old_g, old_b = ImageColor.getrgb(color)
	rgb_max = 255.
	h, s, v = colorsys.rgb_to_hsv(old_r/rgb_max, old_g/rgb_max, old_b/rgb_max)
	def senti_color(senti):
		r,g,b=colorsys.hsv_to_rgb(h,s,0.2+0.8*senti)
		return 'rgb({:.0f}, {:.0f}, {:.0f})'.format(r * rgb_max, g * rgb_max, b * rgb_max)

	return senti_color

'''------------------------------------------------------------------'''
def extract_data_single(fname):
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)	
	data=[data[i][:-1].split(',') for i in xrange(n)]
	k=len(data[0])/2
	data_ex=init_clusters(k,range(k))

	senti=[float(data[0][2*i+1]) for i in xrange(k)]

	for i in xrange(k):
		data_ex[i]=[[data[j][2*i],float(data[j][2*i+1])] for j in xrange(2,n) if data[j][2*i]!='']

	return [data_ex,senti]

def extract_data(fn_tfIdf,fn_senti,cls):
	print 'Extract tfidf'
	tfIdf=extract_data_part(fn_tfIdf,cls)
	print 'Extract senti'
	senti=extract_data_part(fn_senti,cls)

	k=len(cls)
	data=init_clusters(k,cls)
	for i in xrange(k):
		tfIdf_cl=tfIdf[cls[i]]
		senti_cl=senti[cls[i]]
		n=len(tfIdf_cl)
		data[cls[i]]=[[tfIdf_cl[j][0],tfIdf_cl[j][1],senti_cl[j][1]] for j in xrange(n)]

	return data

def extract_data_part(fname,cls):	
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)	
	data=[data[i][:-1].split(',') for i in xrange(n)]
	k=len(cls)
	data_ex=init_clusters(k,cls)

	for i in xrange(k):
		data_ex[cls[i]]=[[data[j][2*i],float(data[j][2*i+1])] for j in xrange(2,n) if data[j][2*i]!='']

	return data_ex


'''------------------------------------------------------------------'''
def process_color(senti,color):
	senti=(np.array(senti)-min(senti))/(max(senti)-min(senti))
	k=len(senti)

	(r,g,b)=color
	print r,g,b
	rgb_max = 255.
	h, s, v = colorsys.rgb_to_hsv(r/rgb_max, g/rgb_max, b/rgb_max)
	colors=[colorsys.hsv_to_rgb(h,s,0.2+0.8*senti[i]) for i in xrange(k)]
	colors=['#%02x%02x%02x'%(colors[i][0]*rgb_max,colors[i][1]*rgb_max,colors[i][2]*rgb_max) for i in xrange(k)]

	return colors

def process_data_single(data):
	n=len(data)
	tf=[data[i][1] for i in xrange(n)]
	tf=np.array(tf)/min(tf)

	return [[data[i][0].title(),tf[i]] for i in xrange(n)]

def process_data(data):
	n=len(data)
	tfIdf=[data[i][1] for i in xrange(n)]
	tfIdf=np.array(tfIdf)/min(tfIdf)
	senti=[data[i][2] for i in xrange(n)]
	senti=(np.array(senti)-min(senti))/(max(senti)-min(senti))

	return [[data[i][0].title(),tfIdf[i],senti[i]] for i in xrange(n)]

'''------------------------------------------------------------------'''
def draw_wordcloud_single(fname,name,color,mtx):
	wordcloud=WordCloud(
		prefer_horizontal=1.0,color_func=get_single_color_func(color),
		max_font_size=50, relative_scaling=0.3,font_path='font\\verdanab.ttf',
		width=600,background_color=None,mode='RGBA'
		).generate_from_frequencies(mtx)
	plt.figure()
	plt.imshow(wordcloud)
	plt.axis("off")
	plt.savefig('png\\'+fname+'_wcloud.png')
	plt.close()


def draw_wordcloud(fname,name,color,mtx):
	wordcloud=WordCloud(
		prefer_horizontal=1.0,color_func=my_color_func(color),
		max_font_size=50, relative_scaling=0.3,font_path='font\\verdanab.ttf',
		width=600
		).generate_from_tfIdf_senti(mtx)
	plt.figure()
	plt.imshow(wordcloud)
	plt.axis("off")
	plt.title('Tweet WordCloud %s'%name.title())
	plt.savefig('png\\'+fname+'_wcloud.png')
	plt.close()

'''
hues=color_generator(15)
for hue in hues:
	print hue,ImageColor.getrgb(hue)
'''