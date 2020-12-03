# Utilities to work with Project Gutenberg

from typing import List, Dict
import requests
import rdflib
import re

# Patterns for end of ebook header
_ebook_headers = [
	'\nProduced by ',
	'\n.*\*\*\*.*START OF .*PROJECT GUTENBERG', # Example: 768
	'\n.*END .*SMALL PRINT', # Example: 774
	'\n<<THIS ELECTRONIC VERSION', # Example: 1100
	'\n\*SMALL PRINT\!', # Example: 2357
	'\n\*.*This file should be named', # Example: 4461
	'\nCharacter set encoding', # Example: 4716
]

# Patterns for start of ebook footer
_ebook_footers = [
	'\n.*Project Gutenberg',
	'\n<<THIS ELECTRONIC VERSION', # Example: 1100
	'\n\[End of original text', # Example: 1224
]

def get_meta(ebook_id: int) -> Dict:
	"""Read ebook metadata data from Project Gutenberg
	Return a dictionary with fields 'ebook_id', 'title', 'language', 'author'
	'author' field is a list of dictionaries, each with fields 'name', 'birthdate', 'deathdate'
	"""

	# Read and parse metadata
	url = f'http://www.gutenberg.org/cache/epub/{ebook_id}/pg{ebook_id}.rdf'
	g = rdflib.Graph()
	try:
		g.parse(url)
	except Exception as err:
		print(f'ebook {ebook_id}: {err}')
		return None

	meta = {'ebook_id': ebook_id}

	# Title
	rows = list(g.query('SELECT ?b WHERE {?a <http://purl.org/dc/terms/title> ?b}'))
	meta['title'] = rows[0][0].toPython()

	# Language
	rows = list(g.query('SELECT ?d WHERE {?a <http://purl.org/dc/terms/language> ?b . ?b ?c ?d}'))
	meta['language'] = str(rows[0][0])

	# Authors
	meta['author'] = []
	rows = list(g.query('SELECT ?b WHERE {?a <http://purl.org/dc/terms/creator> ?b}'))
	for row in rows:
		val = row[0].toPython()
		author = {'author_id': int(val.split('/')[-1])}

		# Name
		res = list(g.query('SELECT ?a WHERE {<%s> <http://www.gutenberg.org/2009/pgterms/name> ?a}' % val))
		author['name'] = res[0][0].toPython()

		# Birth date
		res = list(g.query('SELECT ?a WHERE {<%s> <http://www.gutenberg.org/2009/pgterms/birthdate> ?a}' % val))
		author['birthdate'] = res[0][0].toPython() if res else None

		# Death date
		res = list(g.query('SELECT ?a WHERE {<%s> <http://www.gutenberg.org/2009/pgterms/deathdate> ?a}' % val))
		author['deathdate'] = res[0][0].toPython() if res else None

		meta['author'].append(author)
	return meta

def get_ebook(ebook_id: int) -> str:
	"""Read and return ebook text from Project Gutenberg
	"""

	# Try different name file name formats
	for suffix in ('', '-0', '-8'):
		url = f'http://www.gutenberg.org/files/{ebook_id}/{ebook_id}{suffix}.txt'
		r = requests.get(url)
		if r.status_code == requests.codes.ok: break
	else:
		return None

	# Look for end of ebook header
	for header in _ebook_headers:
		res = re.search(header, r.text[:20000], re.I)
		if res: break
	else:
		print(f'ebook {ebook_id}: Failed to find end of header')
		return None

	start = r.text.find('\n', res.start()+1) + 1

	# Look for start of ebook footer
	ind = max(start, len(r.text)-30000)
	for footer in _ebook_footers:
		res = re.search(footer, r.text, re.I)
		if res: break
	else:
		print(f'ebook {ebook_id}: Failed to find start of footer')
		return None

	end = res.start() + ind
	return r.text[start:end]

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from collections import Counter

_stopwords = stopwords.words('english')

def count_words(text: str) -> Dict:
	"""Count frequency of each word
	"""

	counts = Counter()
	for line in text.splitlines():
		counts.update(
			word for word in word_tokenize(line.lower())
			if word.isalpha() and word not in _stopwords
		)
	return counts

from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData, ForeignKey, select
from sqlalchemy_utils import database_exists, create_database, drop_database
from binascii import crc32
import threading

class Database:
	"""Working with underlying database
	"""

	# Table schemas
	metadata = MetaData()

	authors = Table('authors', metadata,
		Column('author_id', Integer, primary_key=True, autoincrement=False),
		Column('name', Text, nullable=False),
		Column('birthdate', Integer),
		Column('deathdate', Integer)
	)

	ebooks = Table('ebooks', metadata,
		Column('ebook_id', Integer, primary_key=True, autoincrement=False),
		Column('title', Text, nullable=False),
		Column('language', String(2))
	)

	words = Table('words', metadata,
		Column('word_id', Integer, primary_key=True, autoincrement='auto'),
		Column('hash', Integer, index=True),
		Column('value', Text)
	)

	ebook_authors = Table('ebook_authors', metadata,
		Column('ebook_id', Integer, ForeignKey('ebooks.ebook_id'), primary_key=True),
		Column('author_id', Integer, ForeignKey('authors.author_id'), primary_key=True)
	)

	ebook_words = Table('ebook_words', metadata,
		Column('ebook_id', Integer, ForeignKey('ebooks.ebook_id'), primary_key=True),
		Column('word_id', Integer, ForeignKey('words.word_id'), primary_key=True),
		Column('frequency', Integer)
	)

	def __init__(self, url: str, concurrency: bool) -> None:
		"""Initialize database connection
		url: connection string
		concurrency: whether database will be updated by other sources
                     will use an unsafe but fast method to add records if set to false
		"""

		# Create database if not exist
		if not database_exists(url):
			create_database(url)

		# Create tables
		# It does nothing if tables already exist
		self.engine = create_engine(url)
		Database.metadata.create_all(self.engine)

		# Establish 2 connections, one of which is for threading
		self.conn = self.engine.connect()
		self.conn2 = self.engine.connect()

		self.concurrency = concurrency

		# Maxium word_id in local word list
		# -1 means no word list has been built
		self.word_id_max = -1

		# Whether the thread is being used
		self.thread = None

	def _has_author(self, author_id: int) -> bool:
		"""Check whether an author already exists in 'authors' table
		"""

		query = select([Database.authors]).where(Database.authors.c.author_id==author_id)
		result = self.conn.execute(query).first()
		return result is not None

	def _has_ebook(self, ebook_id: int) -> bool:
		"""Check whether an ebook already exists in database
		"""

		query = select([Database.ebooks]).where(Database.ebooks.c.ebook_id==ebook_id)
		result = self.conn.execute(query).first()
		return result is not None

	def _add_author(self, author: Dict) -> None:
		"""Add an author to 'authors' table
		"""

		if not self._has_author(author['author_id']):
			self.conn.execute(Database.authors.insert(), author)

	def _add_ebook(self, meta: Dict) -> None:
		"""Add an ebook to database
		Make sure ebook not already exist before calling this method
		"""

		# Add ebook to 'ebooks' table
		meta2 = {key:val for key, val in meta.items() if key != 'author'}
		self.conn.execute(Database.ebooks.insert(), meta2)

		# Nothing else to do if ebook has no author
		authors = meta.get('author', [])
		if not authors: return

		# Add author(s) to 'authors' table
		for author in authors:
			self._add_author(author)

		# Update 'ebook_authors' table
		ebook_id = meta['ebook_id']
		vals = [{'ebook_id':ebook_id, 'author_id':author['author_id']} for author in authors]
		self.conn.execute(Database.ebook_authors.insert(), vals)

	def _fetch_words(self) -> None:
		"""Fetch world list from database
		"""

		query = select([Database.words])
		result = self.conn.execute(query)
		self.word_dict = {word['value']:word['word_id'] for word in result}

		# Update maximum word_id
		self.word_id_max = max(self.word_dict.values()) if self.word_dict else 0

	def _hash(self, word: str) -> int:
		"""Hashing word to speed up word search
		"""

		return crc32(bytes(word, 'utf-8')) % 10000

	def _word_id(self, word: str) -> int:
		"""Find word_id from 'words' table
		"""

		# Find all words with same hash
		hash = self._hash(word)
		query = select([Database.words]).where(Database.words.c.hash==hash)
		result = self.conn.execute(query)

		for row in result:
			if row['value'] == word:
				return row['word_id']
		return -1

	def _add_words_safe(self, words: List[str]) -> List[int]:
		"""Safe but slow method to add words to 'words' table
		Return word_id list to be used in 'ebook_words' table
		"""

		word_ids = []
		for word in words:
			word_id = self._word_id(word)

			# Add word if not already exist
			if word_id == -1:
				val = {'hash':self._hash(word), 'value':word}
				word_id = self.conn.execute(Database.words.insert(), val).inserted_primary_key

			word_ids.append(word_id)
		return word_ids

	def _add_words_unsafe(self, words: List[str]) -> List[int]:
		"""Unsafe but fast method to add words to 'words' table
		Return word_id list to be used in 'ebook_words' table
		"""

		# Build local word list if not exist
		if self.word_id_max == -1:
			self._fetch_words()

		# Prepare records for new words
		vals = []
		for word in words:
			if word not in self.word_dict:
				self.word_id_max += 1
				self.word_dict[word] = self.word_id_max
				vals.append({'wordid':self.word_id_max, 'hash':self._hash(word), 'value':word})

		# Add new words to 'words' table
		if vals:
			self.conn.execute(Database.words.insert(), vals)

		return [self.word_dict[word] for word in words]

	def _add_ebook_words(self, ebook_id: int, word_counts: Dict) -> None:
		"""Add (ebook_id, word, frequency) records to 'ebook_words' table
		"""

		words, counts = zip(*word_counts.items())

		if self.concurrency:
			word_ids = self._add_words_safe(words)
		else:
			word_ids = self._add_words_unsafe(words)

		vals = [{'ebook_id':ebook_id, 'word_id':word_id, 'frequency':frequency}
				for word_id, frequency in zip(word_ids, counts)]

		# Wait for running thread to finish
		if self.thread: self.thread.join()

		self.thread = threading.Thread(target=self.conn2.execute, args=(Database.ebook_words.insert(), vals))
		self.thread.start()

	def drop(self) -> None:
		"""Delete the connected database
		Useful for small test
		"""

		drop_database(self.engine.url)

	def build(self, start: int, end=None, ping=0) -> int:
		"""Add ebooks with ebook_id in [start, end) to database
		Print a dot for every 'ping' records added
		"""

		if end is None: end = start + 1

		cnt = 0
		for ebook_id in range(start, end):
			# Nothing to do if ebook already exists
			if self._has_ebook(ebook_id): continue

			# Ignore non-English ebooks because it is harder to locate ebook headers and footers
			meta = get_meta(ebook_id)
			if meta is None or meta['language'] != 'en': continue

			# Skip ebook if cannot locate text file
			ebook = get_ebook(ebook_id)
			if ebook is None: continue

			self._add_ebook(meta)
			counts = count_words(ebook)
			self._add_ebook_words(ebook_id, counts)

			cnt += 1
			if ping > 0 and cnt % ping == 0: print('.', end='', flush=True)
		return cnt

	def query_ebook_words(self, ebook_id: int, limit=None) -> List:
		"""Return most frequently used words in an ebook
		"""

		query = select([Database.words.c.value.label('word'), Database.ebook_words.c.frequency]) \
					.select_from(Database.words.join(Database.ebook_words)) \
					.where(Database.ebook_words.c.ebook_id==ebook_id) \
					.order_by(Database.ebook_words.c.frequency.desc())
		if limit: query = query.limit(limit)

		return self.conn.execute(query).fetchall()

	def query_word_ebooks(self, word: str, limit=None) -> List:
		"""Return ebooks where a word shows up the most
		"""

		word_id = self._word_id(word)
		query = select([Database.ebooks.c.ebook_id, Database.ebooks.c.title, \
						Database.authors.c.name.label('author'), Database.ebook_words.c.frequency]) \
					.select_from(
						Database.words.join(Database.ebook_words) \
						.join(Database.ebooks) \
						.join(Database.ebook_authors) \
						.join(Database.authors)) \
					.where(Database.words.c.word_id==word_id) \
					.order_by(Database.ebook_words.c.frequency.desc(), Database.ebooks.c.ebook_id)
		if limit: query = query.limit(limit)

		return self.conn.execute(query).fetchall()

import os

if __name__ == '__main__':

	# Use ebook_id range to build databases
	builds = [
		('guten1', 19001, 19301),
		('guten1', 19301, 19601),
		('guten2', 19601, 20001),
	]

	dialet = os.getenv('DB_DIALET')
	host = os.getenv('DB_HOST')
	user = os.getenv('DB_WRITER')
	password = os.getenv('DB_WRITER_PASSWORD')

	databases = set()
	for database, ebook_id1, ebook_id2 in builds:
		url = f'{dialet}://{user}:{password}@{host}/{database}'

		# Drop database if already exists
		if database not in databases and database_exists(url):
			drop_database(url)

		databases.add(database)
		db = Database(url, False)

		print(f'Loading ebooks {ebook_id1}-{ebook_id2} into {database}...', end='', flush=True)
		n = db.build(ebook_id1, ebook_id2, 5)
		print(f'\n{n} ebooks loaded')
