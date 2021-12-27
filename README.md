# **Assignment 2 of Distributed Systems Programming course.**

## Job Flow
Down bellow we present the job flow of our map - reduce program:

![שקופית2](https://user-images.githubusercontent.com/73799544/147504730-89a6c46a-1f36-4c0b-a275-b6f7739876f4.JPG)

Steps 1,2 and 3 get as input the corpus of 1,2 and 3 hebrew gram respectivily. Steps 4 - 6 are also depict bellow

![שקופית1](https://user-images.githubusercontent.com/73799544/147505002-157ace9a-eb9c-4433-a739-eb6f4d1d0a79.JPG)


## Statistics:
We run the framework once using local aggregation, and once with out, and compare the amount of key-pairs (records) sent from the mappers to the reducers in each stage.

### With local aggregation:
We depict the amount of records sent from the mapper to the reducer in each step, and their size:

Step 1: Amount of records: 645290. Size (in bytes): 7222616.

Step 2: Amount of records: 4758948. Size (in bytes): 64506969.

Step 3: Amount of records: 2804000. Size (in bytes): 43595711.

Step 4: Amount of records: 10366794. Size (in bytes): 152420750.

Step 5: Amount of records: 5607919. Size (in bytes): 123103523.

Step 6: Amount of records: 2802697. Size (in bytes): 76581507.

### Without local aggregation:
Step 1: 88800980.

Step 2: --.

Step 3: --.

Step 4: --.

Step 5: --.

Step 6: --.


## Analysis:
We choose 10 interesting words, and show their top-5 next words.

### Word-pair \#1: והיה הדבר
#### Extension \#1:   לפלא ---> Probability: 0.12502247518814544.
#### Extension \#2:   קשה ---> Probability: 0.108344127346545.
#### Extension \#3:   הזה ---> Probability: 0.09264566177064017.
#### Extension \#4:   תמוה ---> Probability: 0.02595942497727992.
#### Extension \#5:   הולך ---> Probability: 0.019630204491129788.
	

### Word-pair \#2: ירושלים עיר
#### Extension \#1:   הקודש ---> Probability: 0.33095077365409875.
#### Extension \#2:   הקדש ---> Probability: 0.08305946185336943.
#### Extension \#3:   ־ ---> Probability: 0.05472737573493457.
#### Extension \#4:   קדשנו ---> Probability:0.036837163308147805.
#### Extension \#5:   האמת ---> Probability: 0.014586697531563584.

### Word-pair \#3: כבר שעת
#### Extension \#1:   לילה ---> Probability: 0.10344236266911053.
#### Extension \#2:   צהריים ---> Probability: 0.0937264771310196.
#### Extension \#3:   ־ ---> Probability: 0.0924932488676909.
#### Extension \#4:   ערב ---> Probability: 0.07936638005474976.
#### Extension \#5:   חצות ---> Probability: 0.06990629164943397.

### Word-pair \#4: לא נשתנה
#### Extension \#1:   . ---> Probability: 0.09511182241316672.
#### Extension \#2:   דבר ---> Probability: 0.056141268235972215.
#### Extension \#3:   הרבה ---> Probability: 0.025003270261256427.
#### Extension \#4:   כלום ---> Probability: 0.022591550293545818.
#### Extension \#5:   כל ---> Probability: 0.019775152865219726.

### Word-pair \#5: מאז בואו
#### Extension \#1:   של ---> Probability: 0.1919310828176682.
#### Extension \#2:   ארצה ---> Probability: 0.07926898500918625.
#### Extension \#3:   לארץ ---> Probability: 0.060303448934570396.
#### Extension \#4:   אל ---> Probability: 0.034139137249397856.
#### Extension \#5:   הנה ---> Probability: 0.10304347235846413.

### Word-pair \#6: --- ---
#### Extension \#1:   בעיני ---> Probability: 0.4546724649326169.
#### Extension \#2:   בעיניו ---> Probability: 0.18521039888552432.
#### Extension \#3:   בעיניה ---> Probability: 0.06418758895343431.
#### Extension \#4:   בעיניהם ---> Probability: 0.04648156964092304.
#### Extension \#5:   בעיניך ---> Probability: 0.020209849201225618.

### Word-pair \#7: עוד אלא
#### Extension \#1:   שהוא ---> Probability: 0.02860553050501406.
#### Extension \#2:   אפילו ---> Probability: 0.02410504080035991.
#### Extension \#3:   שגם ---> Probability: 0.019075124011274792.
#### Extension \#4:   שכל ---> Probability: 0.01881689241802652.
#### Extension \#5:   שיש ---> Probability: 0.017475460800359985.

### Word-pair \#8: קליטתם של
#### Extension \#1:   העולים ---> Probability: 0.1036869614452916.
#### Extension \#2:   עולי ---> Probability: 0.060577112398540536.
#### Extension \#3:   עולים ---> Probability: 0.04523004907600305.
#### Extension \#4:   בני ---> Probability: 0.031430851777473234.
#### Extension \#5:   יהודי ---> Probability: 0.027521426083508178.

### Word-pair \#9: שאני רוצה
#### Extension \#1:   . ---> Probability: 0.07642113824038102.
#### Extension \#2:   להיות ---> Probability: 0.034451902842177896.
#### Extension \#3:   לעשות ---> Probability: 0.027894040619652748.
#### Extension \#4:   לומר ---> Probability: 0.026850123015816112.
#### Extension \#5:   להגיד ---> Probability: 0.016191649615060796.

### Word-pair \#10: תקופה של
#### Extension \#1:   שנה ---> Probability: 0.011185452590191125.
#### Extension \#2:   ״ ---> Probability: 0.010722358784620911.
#### Extension \#3:   פריחה ---> Probability: 0.010519807994707843.
#### Extension \#4:   למעלה ---> Probability: 0.010376770313337115.
#### Extension \#5:   חמש ---> Probability: 0.009176709777924601.

**We can see that the system indeed retreived a reasonable result for these combinations!**



















