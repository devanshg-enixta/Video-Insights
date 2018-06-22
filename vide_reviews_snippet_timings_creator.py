from nltk.tokenize import word_tokenize
from nltk.util import ngrams
from itertools import *
import pandas as pd
import os,re,sys,subprocess
import pysrt
import glob
import csv
file =sys.argv[1]
video_snip = pd.read_csv(file,sep='~')
c = sys.argv[2]
ids= video_snip.source_product_id.unique()
timing_output=[]
flag=0
t1=""
t2=""
t3=""
t4=""
t5=""
for id in ids:
    temp = video_snip[video_snip.source_product_id == id]
    for row in temp.to_dict('records'):
        x=""
        subs = pysrt.open(c+'_'+str(id)+'_'+row['source_review_id'].split('=')[-1]+'.en_cleaned.srt', encoding='iso-8859-1')
        row['video_start_time'] = str(subs[0].start).split(',')[0]
        for i in range(0,len(subs)):
            subs[i].text = subs[i].text.lower().replace('-',' ')
            subs[i].text = re.sub(r'([^\s\w]|_)+', '',subs[i].text )
            row['sentiment_text'] = row['sentiment_text'].lower()
            row['sentiment_text'] = re.sub(r'([^\s\w]|_)+', '',row['sentiment_text'] )
            if row['sentiment_text'] in subs[i].text:
                row['start_time'] = str(subs[i-1].start).split(',')[0]
                row['end_time'] = str(subs[i].end).split(',')[0]
                x = subs[i].text
            else:
                row['snippet_end_time'] = subs[i].end 
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
                    row['start_time'] = str(subs[i-1].start).split(',')[0]
                    row['end_time'] = str(subs[i].end).split(',')[0]
                    break
                elif row['sentiment_text'] in t2:
                    row['start_time'] = str(subs[i].start).split(',')[0]
                    row['end_time'] = str(subs[i+1].end).split(',')[0]
                    break
                elif row['sentiment_text'] in t3:
                    if (i+2) < len(subs):
                        row['start_time'] = str(subs[i].start).split(',')[0]
                        row['end_time'] = str(subs[i+2].end).split(',')[0]
                        break
                elif row['sentiment_text'] in t4:
                    row['start_time'] = str(subs[i-2].start).split(',')[0]
                    row['end_time'] = str(subs[i].end).split(',')[0]
                    break
                elif row['sentiment_text'] in t5:
                    row['start_time'] = str(subs[i].start).split(',')[0]
                    row['end_time'] = str(subs[i+3].end).split(',')[0]
                    break
        row['video_end_time'] = str(subs[len(subs)-1].end).split(',')[0]
        timing_output.append(row)
        row["sentiment_text_start_index"]=row['start_index']
        row["sentiment_text_end_index"]=row['end_index']
        row['snippet_length'] = (len(row["sentiment_text"]))
        
timing_output = pd.DataFrame(timing_output)                
timing_output['source_id'] = 7
timing_output=timing_output.rename(index=str, columns={"category": "category_id", "source_product_id": "product_id",'confidence-score':"sentiment_conf_score"})
COLS= ['product_id','category_id','source_id','source_review_id','video_review_id','category_aspect_id','aspect','sentiment_type','sentiment_text','sentiment_text_start_index','sentiment_text_end_index','sentiment_start_snippet','sentiment_end_snippet','treemap_name','sentiment_conf_score','end_time','start_time']
timing_output.to_csv(c+'_product_video_review_sentiment.csv',index=False,columns=COLS)
snipp = timing_output[['source_review_id','start_time','end_time','video_end_time','video_start_time','sentiment_text','snippet_length','source_id','product_id']]
snipp = snipp.rename(index=str, columns={"source_review_id": "video_loc", "sentiment_text": "snippet"})
col = ['id','video_loc','start_time','end_time','snippet','snippet_length']
snipp.to_csv(c+'_product_video_review_snippet.csv',index=False,columns=col)
colum =['id','product_id','video_loc','source_id','overall_rating','outof_rating','review_date','normalized_date','source_review_id','reviewer','review_title','review_url','video_url','review_text','video_review_text','start_time','end_time','priority','status']
vid_rev = snipp[['video_loc','video_end_time','video_start_time','source_id','product_id']]
vid_rev = vid_rev.rename(index=str, columns={"video_end_time": "end_time", "video_start_time": "start_time"})
vid_rev = vid_rev.drop_duplicates()
vid_rev['status']= 'A'
vid_rev.to_csv(c+'_product_video_review.csv',index=False,columns=colum)    