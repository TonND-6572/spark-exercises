# Profile update
## Input:
A textual file containing the list of movies watched by the users of a video on demand service
- Each line of the file contains the information about one visualization
***userid,movieid,start-timestamp,end-timestamp***
- The user with id userid watched the movie with id movieid from start-timestamp to end-timestamp

A second textual file containing the list of
preferences for each user
- Each line of the file contains the information about one preference 
***userid,movie-genre***
- The user with id userid liked the movie of type moviegenre

A third textual file containing the list of movies with the associated information
- Each line of the file contains the information about one movie 
***movieid,title,movie-genre***
- There is only one line for each movie
- i.e., each movie has one single genre

## Output:
Select for each user with a misleading profile (according to the same definition of Exercise #44) the list of movie genres that are not in his/her preferred genres and are associated with at least 5 movies watched by the user

Store the result in an HDFS file
- Each line of the output file is associated with one pair (user, selected misleading genre) associated with him/her
- The format is userid, selected (misleading) genre
- Users associated with a list of selected genres are associated with multiple lines of the output file