# Misleading profile selection
## Input:
A textual file containing the list of movies watched by the users of a video on demand service
- Each line of the file contains the information about one visualization 
***userid,movieid,start-timestamp,end-timestamp***
- The user with id userid watched the movie with id movieid from start-timestamp to end-timestamp

A second textual file containing the list of preferences for each user
- Each line of the file contains the information about one preference
***userid,movie-genre***
- The user with id userid liked the movie of type moviegenre

A third textual file containing the list of movies with the associated information
- Each line of the file contains the information about one movie
***movieid,title,movie-genre***
- There is only one line for each movie
- i.e., each movie has one single genre

## Output:
Select the userids of the list of users with a misleading profile
- A user has a misleading profile if more than threshold% of the movies he/she watched are not associated with a movie genre he/she likes
- Threshold is an argument/parameter of the application and it is specified by the user

Store the result in an HDFS file