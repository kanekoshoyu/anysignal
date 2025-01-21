# To ORM or not ORM

I can see how questdb is highly flexible and easy to maintain. but then it comes with the potential issue with maintainability in terms of schema changes etc.
we need to keep track of the schema, and the way to do it is to version the database along with its schema and table name.
let's avoid using ORM, it gives unnecessary modelling.

I define the questdb schema into table, just so we can look at the tables directly