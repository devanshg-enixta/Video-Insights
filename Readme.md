1. First one has to download pysrt using terminal/command prompt ---> pip install pysrt   
2. create a file containing video links. Sample vidoe link file is attached below with the name video_links.csv (you can provide all the links of the video in seperate row irrespective of the category )
2.  run the script source_review_file_video_review.py as follows -- >
         python source_review_file_video_review.py video_links.csv
*This script will make a sourc_review_file for each category for which the video links has been provided named as landmarks_source_video_reviews.txt where landmarks being the category.
3. Now run the Smaartpulse using the review file using command 
python main.py --category_id 16 --input_reviews_file landmarks_source_video_reviews.txt --classification_type sentiment --client_id 9

4. now using the treemap_file generated by the smaartpulse.... run the script named as vide_reviews_snippet_timings_creator.py as follows:-
 
python vide_reviews_snippet_timings_creator.py treemap_outputfile_2018-5-3_landmarks_sentiment_poc.txt  landmarks
* this script generates the file as needed by the db for the ingestion the file name should be strictly like this.
 The first argument is the treemap_file name and second is the category name as given in the video_links.csv file. (Please don't change the name)
