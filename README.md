# Mongo-Spark
### First requirement:-
read the json-formated tweets in the attached file and use MongoSpark library to insert them into mongoDB database in a collection called 'tweets'.

### Second requierment:-
The timestamp associated with each tweet is to be stored as a Date object, where the timestamp field is to be indexed.

### Third requirement:-
Also, the geo-coordinates of tweets should be indexed properly to ensure a fast spatial-based retrieval.

### Fourth requirement:-
calculate the number of occurrences of word w published within a circular region of raduis (r), having a central point of (lon, lat), mentioned in tweets published during the time interval (start, end). Perform this operation by two ways:
1- using MongoSpark, by collecting tweets and filtering them spatio-temporally using dataframe apis.
2- using mongodb library by sending a normal mongoDB query to filter by time and space.
3- Text indexing is optional.

### Fifth requirement:- 
Run the application as follows:
WordFreqCalculator.scala w r lon lat start end

w: word to calculte its frequency
r: raduis in meters
lon: longitude
lat: latitude
start: starting epoc time
end: ending epoc time
