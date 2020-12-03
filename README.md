# Gutenberg Search

A utility to search ebooks in Project Gutenberg.

## Usage

Most frequently used words in a Project Gutenberg ebook.
```
> ./search_words.py 19900 --limit 5
+---------+-------+
|frequency|word   |
+---------+-------+
|832      |one    |
|434      |would  |
|391      |man    |
|385      |witches|
|373      |time   |
+---------+-------+
```

Project Gutenberg ebooks where a word shows up the most.
```
> ./search_ebooks.py fish --limit 5
+---------+--------+------------------------------------------------------+----------------------------------+
|frequency|ebook_id|title                                                 |author                            |
+---------+--------+------------------------------------------------------+----------------------------------+
|139      |19077   |Salads, Sandwiches and Chafing-Dish Dainties          |Hill, Janet McKenzie              |
|         |        |With Fifty Illustrations of Original Dishes           |                                  |
|63       |19853   |Bob Hunt in Canada                                    |Orton, George W.                  |
|52       |19565   |Bunny Brown and His Sister Sue and Their Shetland Pony|Hope, Laura Lee                   |
|47       |19042   |Animal Figures in the Maya Codices                    |Tozzer, Alfred M. (Alfred Marston)|
|         |        |                                                      |Allen, Glover M. (Glover Morrill) |
|47       |19981   |Doctor Luke of the Labrador                           |Duncan, Norman                    |
+---------+--------+------------------------------------------------------+----------------------------------+
```

## Installation

Gutenberg Search consists of a single-file module and some Python scripts. It uses the following Python packages.
- `requests` for downloading files from Project Gutenberg
- `rdflib` for parsing meta files in XML/RDF format
-  `sqlalchemy` and `sqlalchemy_utils` for database operations
-  `nltk` for word tokenization

Database information should be provided through the following environment variables.
- `DB_DIALET`: database dialet
- `DB_HOST`: database host
- `DB_WRITER`: database username with read/write permissions
- `DB_WRITER_PASSWORD`: password of above username
- `DB_READER`: database username with read permission
- `DB_READER_PASSWORD`: password of above username

To initialize the database, run `python3 gutenberg.py`. This will create two databases, `guten1` with ebooks 19001 to 19600, and `guten2` with ebooks 19601 to 20000. Despite some optimizations, the initialization still takes around 30 minutes. Note that the following ebooks are skipped.
- Not in English
- `.txt` file does not exist
- Fail to detect header or footer

Sample output:
```
> python3 gutenberg.py
Loading ebooks 19001-19301 into guten1....................................................
245 ebooks loaded
Loading ebooks 19301-19601 into guten1.....................................................
251 ebooks loaded
Loading ebooks 19601-20001 into guten2................................................................
307 ebooks loaded
```

## Optimizations

### Word hashing

Since indexing strings may not be supported by the underlying database, to speed up word search, indexing is created on a hash (last 5 digits of CRC32 checksum) of the word.

### Pre-fetching word list

The naive way to insert a ebook-word frequency count consists of the following steps:
- Look up `word_id` of the word from `words` table.
- If the word is not found, add it.
- Add a (`ebook_id`, `word_id`, `frequency`) record to the `ebook_words` table.

This consists of 2 to 3 database calls per word, which is very expensive. To speed up the process, contents of `words` table is pre-fetched. Then `word_id` look-up can be performed locally. As a result, only 2 database calls (one to add new words to `words` table and the other to add records to `ebook_words` table) are needed to process all words of an ebook.

Note that this approach does not work when multiple connections add words at the same time, because the local word list may not be correct. Therefore, the pre-fetching approach is only used when `concurrency=False` is specified at database connection:
```python
db = gutenberg.Database(url, concurrency=False)
```

### Threading

A seperate connection and thread is used to add records to the `ebook_words` table.

## Scaling

Horizontal scaling is accomplished by using multiple hosts with non-overlapping ebooks. The search scripts shows how results from multiple databases can be combined.