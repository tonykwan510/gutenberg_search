#!/usr/bin/python3
import argparse
import os
import gutenberg

parser = argparse.ArgumentParser(description='Project Gutenberg ebooks where a word shows up the most.')
parser.add_argument('word', metavar='word', type=str, help='search word')
parser.add_argument('--limit', type=int, default=10, help='number of ebooks to display')
args = parser.parse_args()

dialet = os.getenv('DB_DIALET')
host = os.getenv('DB_HOST')
user = os.getenv('DB_READER')
password = os.getenv('DB_READER_PASSWORD')
databases = ('guten1', 'guten2')

# Collect results from all databases
result = []
for database in databases:
	url = f'{dialet}://{user}:{password}@{host}/{database}'
	db = gutenberg.Database(url, False)
	result.extend(db.query_word_ebooks(args.word, args.limit))

result.sort(key=lambda row:row.ebook_id)
result.sort(key=lambda row:row.frequency, reverse=True)

# Split multi-line titles
# Combine multiple records from 'ebook_authors' table for ebooks with multiple authors
cols = ('frequency', 'ebook_id', 'title', 'author')
rows = []
cnt = 0
ebook_id = 0
for row in result:
	if row.ebook_id == ebook_id:
		authors += 1
		if authors > len(title_lines):
			row1 = {col:'' for col in cols}
			row1['author'] = row['author']
			rows.append(row1)
		else:
			rows[len(title_lines)-authors-1]['author'] = row['author']
	else:
		ebook_id = row.ebook_id
		authors = 1
		title_lines = row['title'].splitlines()

		row1 = {col:row[col] for col in cols}
		row1['title'] = title_lines[0]
		rows.append(row1)

		row1 = {col:'' for col in cols}
		for title in title_lines[1:]:
			row1['title'] = title
			rows.append(row1)

		cnt += 1
		if cnt == args.limit:
			break

# Pretty print records
sep = '+'
format = '|'
for col in cols:
    n = max(len(col), max(len(str(row[col])) for row in rows))
    sep += '-' * n + '+'
    format += f'%-{n}s|'

print(sep)
print(format % cols)
print(sep)
for row in rows:
    print(format % tuple(row[col] for col in cols))
print(sep)
