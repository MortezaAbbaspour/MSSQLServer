# Who should read this book
This book is designed to help experienced T-SQL practitioners become more knowledgeable
and efficient in this field. The book’s target audience is T-SQL developers,
DBAs, BI pros, data scientists, and anyone who is serious about T-SQL.
# Organization of this book
The book starts with two chapters that lay the foundation of logical and physical query
processing required to gain the most from the rest of the chapters.
The first chapter covers logical query processing. It describes in detail the logical
phases involved in processing queries, the unique aspects of SQL querying,
and the special mindset you need to adopt to program in a relational, set-oriented
environment.
The second chapter covers query tuning and the physical layer. It describes internal
data structures, tools to measure query performance, access methods, cardinality
estimates, indexing features, prioritizing queries with extended events, columnstore
technology, use of temporary tables and table variables, sets versus cursors, query tuning
with query revisions, and parallel query execution. (The part about parallel query
execution was written by Adam Machanic.)
The next five chapters deal with various data manipulation–related topics. The
coverage of these topics is extensive; beyond explaining the features, they focus a lot
on the performance of the code and the use of the features to solve common tasks.
Chapter 3 covers multi-table queries using subqueries, the APPLY operator, joins, and
the UNION, INTERSECT, and EXCEPT relational operators. Chapter 4 covers data analysis
using grouping, pivoting, and window functions. Chapter 5 covers the TOP and OFFSETFETCH
filters, and solving top N per group tasks. Chapter 6 covers data-modification
topics like minimally logged operations, using the sequence object efficiently, merging
data, and the OUTPUT clause. Chapter 7 covers date and time treatment, including the
handling of date and time intervals.
Chapter 8 covers T-SQL for BI practitioners and was written by Dejan Sarka. It
describes how to prepare data for analysis and how to use T-SQL to handle statistical
data analysis tasks. Those include frequencies, descriptive statistics for continuous
variables, linear dependencies, and moving averages and entropy.
Chapter 9 covers the programmability constructs that T-SQL supports. Those are
dynamic SQL, user-defined functions, stored procedures, triggers, SQLCLR programming
(written by Adam Machanic), transactions and concurrency, and error handling.Previously, these topics were covered in the book Inside Microsoft SQL Server: T-SQL
Programming.
Chapter 10 covers one of the major improvements in SQL Server 2014—the In-Memory
OLTP engine. This chapter was written by Microsoft’s Kevin Farlee, who was
involved in the actual development of this feature.
Chapter 11 covers graphs and recursive queries. It shows how to handle graph
structures such as employee hierarchies, bill of materials, and maps in SQL Server using
T-SQL. It shows how to implement models such as the enumerated path model (using
your own custom solution and using the HIERARCHYID data type) and the nested sets
model. It also shows how to use recursive queries to manipulate data in graphs.