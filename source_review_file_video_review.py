import pandas as pd
import os,re,sys,subprocess
import pysrt
import glob
import csv
link_file = sys.argv[1]
links = pd.read_csv(link_file,encoding='utf-8')
links = links.dropna(axis=1, how='all')
category = links.category.unique()
out = []
for c in category:
    t = links[links.category == c]
    for row in t.to_dict('records'):
        filename = c+'_'+str(row['product_id'])+'_'+row['URLS'].split('=')[-1]+'.en.srt'
        file_clean = filename.split('.srt')[0]+'_cleaned.srt'
        if not os.path.exists(file_clean):
            print "not"
            text = ""
            command ="youtube-dl -o "+c+'_'+str(row['product_id'])+'_'+row['URLS'].split('=')[-1]+".mp4 --sub-format vtt --convert-subs srt --write-auto-sub --sub-lang en -f mp4 "+str(row['URLS']).strip()
            subprocess.call(command, shell=True)
            subs = pysrt.open(filename, encoding='iso-8859-1')
            f =open(file_clean,'a+')
            j=0
            row['start_time'] = subs[0].start
            for i in range(0,len(subs)):
                if i%2 !=0:
                    text = text+subs[i].text+" "
                    print >> f,j
                    print >> f,subs[i-1].start,' --> ',subs[i-1].end
                    print >> f,subs[i].text,'\n'
                    j =j +1
            os.remove(filename)
            f.close()
        else:
            print "yes"
            text =""
            subs = pysrt.open(file_clean, encoding='iso-8859-1')
            for i in range(0,len(subs)):
                text = text+subs[i].text+" "

            row['start_time'] = subs[0].start 
        row['end_time'] = subs[len(subs)-1].end
        row['review_text'] = text 
        row['source'] = 7
        row['source_product_id'] = row['product_id']
        row['source_review_id'] = row['URLS'].split('=')[-1]
        out.append(row)
    video_review = pd.DataFrame(out)
    video_review=video_review.rename(index=str, columns={"source_product_name": "product_name","URLS": "review_url"})
    video_review['review_date'] = ""
    video_review['star_rating'] = 5
    video_review['verified_user']=""
    video_review['reviewer_name']=""
    video_review['review_url']=video_review['review_url']
    video_review['review_tag']=""
    col = ['source','source_review_id','source_product_id','product_name','review_date','star_rating','verified_user','reviewer_name','review_url','review_tag','review_text']
    video_review.to_csv(c+'_source_video_reviews.txt',sep='~',quoting=csv.QUOTE_NONE,encoding='utf-8',index=False,columns=col)