# Mapping Question-Answer(s)
## Input
- A large textual file containing a set of questions
    - Each line contains one question
    - Each line has the format
        - *QuestionId,Timestamp,TextOfTheQuestion*

- A large textual file containing a set of answers
    - Each line contains one answer
    - Each line has the format
        - *AnswerId,QuestionId,Timestamp,TextOfTheAnswer*

## Output: 
A file containing one line for each question
Each line contains a question and the list of answers to that question
    *QuestionId, TextOfTheQuestion, list of Answers*

---
## Example of data
### Question
Q1,2015-01-01,What is ..?
Q2,2015-01-03,Who invented ..
### Answer
A1,Q1,2015-01-02,It is ..
A2,Q2,2015-01-03,John Smith
A3,Q1,2015-01-05,I think it is ..

### Output data
(Q1,([What is ..?],[It is .., I think it is ..]))
(Q2,([Who invented ..],[John Smith]))