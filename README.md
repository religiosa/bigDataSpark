# bigDataSpark
From a course about Big Data and data mining. Using Spark with Scala.

Exercise 2: The Movie Lens dataset

2.2: Prepare the dataset

2.3: Compute how many movies employed adults have seen in total.

2.4: Return 10 best rated movies from each age range.

2.5: Function for Pearson Correlation.

2.6: Test Pearson Correlation with different combinations of movies and groups. 

I selected five different occupations and five different genres. The occupations are: artist, homemaker, K-12 student, programmer, scientists and the genres: children’s, documentary, film-noir, horror, sci-fi.

From these I consider following pairings interesting:

* Homemaker and horror: All the stay-at-home mothers that I know, or at
least nearly all, have told to have developed some sort of sensitivity towards scary and tragic films, so I presume this combination could yield negative correlation.
* K-12 students and children’s: Quite self-explanatory. Should yield positive correlation.
* Scientists and documentary: I’m not sure, what this should yield. I suppose scientists are more interesting in factual movies, but documentaries could be too simplistic for some.
* Artist and film noir: I suppose artists could watch more film noir than average moviegoer.

Preprosessing the data: 
The data set is given as text files, for user, movie and rating separately. These are then read and processed into an RDD, which contains user, movie and rating info in one. From this RDD, two RDDs are separated: one for all user ids and occupations, one for user ids and favourite genres. The favourite genres are selected based on, what the user has watched most.

Limitations: The code could and should be optimized, but ran out of time on the course. 

Exercise 3: Wikipedia

This task was about parsing a Wikipedia page (I chose http://en.wikipedia.org/wiki/Solanaceae ). 

3.2: Function that changes a letter of a word, practice broadcast.

3.3: Divide the page into sections and compute a probability distribution for small words such as "and, an, the, a, ...". 

3.4: Compute Hamming distance for words. For different lengths, compute distance with each substring and return average distance. Cluster all the words from the Wikipedia article with K-Means Clustering, test with different K.  

The clusters, that my algorithm produces, focus on small words, because of the way we calculate Hamming distance for strings of different length. The algorithm favors short words: if we have strings ’an’ and ’banana’, the distances are calculated to pairs ”an - ba”, ”an - an”, ”an - na”, ”an - an”, ”an - na”, so the total distance is

(2 + 0 + 2 + 0 + 2)/5 = 1, 2

which is really small. So calculating Hamming distance that way isn’t sensible in a way. One solution could be to take into account the shortness of the word and have some sort of penalty or have the shorter string match the best substring in the longer and then treat remaining blanks as letters as well. 

The clusters my solution found in data do not match the paragraphs or their headings very well. The clusters have some, that have grouped all similar or same small words in one cluster, for example ”of” or ”that”. Then there are few clusters of words like ”potato”, ”generally” or ”contains”. In all cases, there is a big cluster consisting of all the other words.

My solution loses some of the clusters somehow, so e.g. by giving 2500 as K I ended up with 25 clusters. So my solution gives more reasonable answers with relatively big K.

