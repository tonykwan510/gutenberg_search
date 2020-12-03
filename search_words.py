#!/usr/bin/python3
import argparse
import os
import gutenberg

parser = argparse.ArgumentParser(description='Most frequently used words in a Project Gutenberg ebook.')
parser.add_argument('ebook_id', metavar='ebook_id', type=int, help='ebook number')
parser.add_argument('--limit', type=int, default=10, help='number of words to display')
args = parser.parse_args()

dialet = os.getenv('DB_DIALET')
host = os.getenv('DB_HOST')
user = os.getenv('DB_READER')
password = os.getenv('DB_READER_PASSWORD')
databases = ('guten1', 'guten2')

for database in databases:
	url = f'{dialet}://{user}:{password}@{host}/{database}'
	db = gutenberg.Database(url, False)

	result = db.query_ebook_words(args.ebook_id, args.limit)
	if result: break
else:
	print(f'Ebook {args.ebook_id} not found in databases.')

# Pretty print records
cols = ('frequency', 'value')
sep = '+'
format = '|'
for col in cols:
	n = max(len(col), max(len(str(row[col])) for row in result))
	sep += '-' * n + '+'
	format += f'%-{n}s|'

print(sep)
print(format % cols)
print(sep)
for row in result:
	print(format % tuple(row[col] for col in cols))
print(sep)
