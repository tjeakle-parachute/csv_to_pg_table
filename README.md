# How to use

Assumptions:

1. You have ruby 3.x running.
2. You have installed the csv and pg gems
3. You have postgres running locally

Simply copy this file into the same directory as your csv and have postgres running locally.

```
git clone this-repo
cd this-repo
cp csv_to_pg_table.rb /my-directory-with-csv
cd /my-directory-with-csv
ruby csv_to_pg_table.rb
```

Follow the prompts to create a table in your local database directly from that csv. The datatypes it uses (or doesn't use) make it kind of a mess for anything large scale.
It can also do a little more but its poorly written so I'm waiting to fix that to document its other abilities.
Feedback, suggestions, and/or questions are welcome.
Hit me up on slack!
