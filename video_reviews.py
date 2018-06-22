import pandas as pd
import os,re,sys,subprocess
import pysrt
import glob
from nltk.tokenize import word_tokenize
from nltk.util import ngrams
from itertools import *
links_file = sys.argv[1]
def get_n(text ):
    list1 =[]
    for i in range(1,len(text.split(' '))+1):
        list1.append(" ".join(text.split()[0:i]))
    return list1

def get_ngrams(text ):
    list1 =[]
    for i in range(1,len(text.split(' '))):
        n_grams = ngrams(word_tokenize(text), i)
        list1.append([ ' '.join(grams) for grams in n_grams])
    list1=list(chain.from_iterable(list1))
    return list1
links = pd.read_csv(links_file,encoding='utf-8')
links = links.dropna(axis=1, how='all')
category = links.Category.unique()
out = []
for c in category:
    print c
    t = links[links.Category == c]
    for row in t.to_dict('records'):
        file = c+'_'+str(row['product_id'])+'.en.srt'
        file_path = file.split('.srt')[0]+'_cleaned.srt'
        if not os.path.exists(file_path):
            print "not"
            text = ""
            command ="youtube-dl -o "+c+'_'+str(row['product_id'])+".mp4 --sub-format vtt --convert-subs srt --write-auto-sub --sub-lang en -f mp4 "+str(row['URLS'])
            subprocess.call(command, shell=True)
            subs = pysrt.open(file, encoding='iso-8859-1')
            f =open(file.split('.srt')[0]+'_cleaned.srt','a+')
            j=0
            row['start_time'] = subs[0].start
            for i in range(0,len(subs)):
                if i%2 !=0:
                    text = text+subs[i].text+" "
                    print >> f,j
                    print >> f,subs[i-1].start,' --> ',subs[i-1].end
                    print >> f,subs[i].text,'\n'
                    j =j +1
            os.remove(file)
            f.close()
        else:
            print "yes"
            text =""
            subs = pysrt.open(file_path, encoding='iso-8859-1')
            for i in range(0,len(subs)):
                text = text+subs[i].text+" "

            row['start_time'] = subs[0].start
        row['end_time'] = subs[len(subs)-1].end
        row['review_text'] = text 
        out.append(row) 
video_review = pd.DataFrame(out) 
video_snip = video_review#pd.read_csv('video_reviews.csv')
ids= video_snip.product_id.unique()
timing_output=[]
flag=0
t1=""
t2=""
t3=""
t4=""
t5=""
for id in ids:
    temp = video_snip[video_snip.product_id == id]
    subs = pysrt.open(temp.Category.unique()[0]+"_"+str(id)+'.en_cleaned.srt', encoding='iso-8859-1')
    for row in temp.to_dict('records'):
        x=""
       # s_txt =get_n(row['sentiment_text'])
        row['video_start_time'] = subs[0].start
        for i in range(0,len(subs)):
            if row['sentiment_text'] in subs[i].text:
                row['snippet_start_time'] = subs[i-1].start
                row['snippet_end_time'] = subs[i].end
                x = subs[i].text
            else:
#                 s_txt =get_n(row['sentiment_text'])
#                 snip = get_ngrams(x)
#                 ques = set(s_txt)&set(snip) 
#                 ques = sorted(ques, key = lambda k : s_txt.index(k))
#                 if any (ques):
#                     row['snippet_start_time'] = subs[i].start
#                     x = x+" "+subs[i].text
#                     if row['sentiment_text'] in x:
#                         row['snippet_end_time'] = subs[i].end 
                if i-1 >=0:
                    t1 = subs[i-1].text+" "+subs[i].text
                if i-2 >=0:
                    t4 = subs[i-2].text+" "+subs[i-1].text+" "+subs[i].text
                if (i+1) < len(subs):
                    t2 = subs[i].text+" "+subs[i+1].text
                if (i+2) < len(subs):
                    t3 = subs[i].text+" "+subs[i+1].text+" "+subs[i+2].text
                if (i+3) < len(subs):
                    t5 = subs[i].text+" "+subs[i+1].text+" "+subs[i+2].text+" "+subs[i+3].text

                if row['sentiment_text'] in t1:
                    row['snippet_start_time'] = subs[i-1].start
                    row['snippet_end_time'] = subs[i].end 
                    break
                elif row['sentiment_text'] in t2:
                    row['snippet_start_time'] = subs[i].start
                    row['snippet_end_time'] = subs[i+1].end
                    break
                elif row['sentiment_text'] in t3:
                    if (i+2) < len(subs):
                        row['snippet_start_time'] = subs[i].start
                        row['snippet_end_time'] = subs[i+2].end
                        break
                elif row['sentiment_text'] in t4:
                    row['snippet_start_time'] = subs[i-2].start
                    row['snippet_end_time'] = subs[i].end
                    break
                elif row['sentiment_text'] in t5:
                    row['snippet_start_time'] = subs[i].start
                    row['snippet_end_time'] = subs[i+3].end
                    break
        row['video_end_time'] = subs[len(subs)-1].end
        timing_output.append(row)
timing_output = pd.DataFrame(timing_output)   
timing_output.to_csv('videos_sentiment_timings_test.csv',index=False)             
