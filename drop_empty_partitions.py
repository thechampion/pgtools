#! /usr/bin/env python

import argparse
import os.path
import sys

from itertools import imap

import psycopg2


DEFAULT_DATABASE = "postgres"


def parse_args():
	parser = argparse.ArgumentParser(usage="%(prog)s [connection-option...] [option...] [dbname]\n" \
		"       %(prog)s [connection-option...] [option...] --all | -a", add_help=False)
	parser.add_argument("-a", "--all", action="store_true", default=False,
		help="Clear all databases.")
	parser.add_argument("-d", "--dbname", dest="database",
		help="Specifies the name of the database to connect to.")
	parser.add_argument("-h", "--host",
		help="Specifies the host name of the machine on which the server is running." \
			" If the value begins with a slash, it is used as the directory for the Unix domain socket.")
	parser.add_argument("-n", "--dry-run", action="store_true", default=False,
		help="Perform a trial run with no changes made.")
	parser.add_argument("-p", "--port", type=int,
		help="Specifies the TCP port or the local Unix-domain socket file" \
			" extension on which the server is listening for connections. Defaults to the value of the PGPORT environment" \
			" variable or, if not set, to the port specified at compile time, usually 5432.")
	parser.add_argument("-T", "--exclude-table", dest="exclude_tables", action="append", metavar="TABLE",
		help="Do not process table descendants. -T can be given more than once to exclude several tables.")
	# TODO implement -t switch
	#parser.add_argument("-t", "--table", dest="tables", action="append", metavar="TABLE",
	#	help="Process only the TABLE descendants. Multiple tables can be selected by writing multiple -t switches.")
	parser.add_argument("-U", "--username", dest="user",
		help="Connect to the database as the user username instead of" \
			" the default. (You must have permission to do so, of course.)")
	parser.add_argument("-v", "--verbose", action="store_true", default=False,
		help="Print detailed information during processing.")
	parser.add_argument("-?", "--help", action="help",
		help="Show this help message and exit.")
	#parser.add_argument("dbname", dest="database")

	args = parser.parse_args()
	if args.all and args.database:
		parser.error("option --all cannot be used if database name is specified")

	return args

def make_connection_params(args):
	params = dict(imap(lambda p: (p, getattr(args, p)), ("host", "port", "database", "user")))
	params["application_name"] = os.path.basename(sys.argv[0])
	return params

def get_databases(conn):
	cur = conn.cursor()
	cur.execute("select datname from pg_database" \
		" where datname not in ('postgres', 'template0', 'template1') order by datname")
	return [t[0] for t in cur]

def print_message(msg):
	print msg

def void_message(_):
	pass

def drop_empty_partitions(conn, exclude_tables=None, dry_run=False, msg_fn=void_message):
	cond = ""
	if exclude_tables:
		cond = "where inhparent <> all(array[%s])" % ", ".join(imap(lambda t: "'%s'::regclass" % t, exclude_tables))

	cur = conn.cursor()
	cur.execute("select inhrelid::regclass::text from pg_inherits %s order by 1" % cond)
	for tab in [t[0] for t in cur]:
		cur.execute("select exists (select * from %s)" % tab)
		if not cur.fetchone()[0]:
			msg_fn("dropping table %s" % tab)
			if not dry_run:
				cur.execute("drop table %s cascade" % tab)


def main():
	args = parse_args()
	params = make_connection_params(args)
	conn = psycopg2.connect(**params)
	msg_fn = print_message if args.verbose else void_message
	if not args.all:
		conn.autocommit = True
		drop_empty_partitions(conn, args.exclude_tables, args.dry_run, msg_fn)
		sys.exit(0)

	databases = get_databases(conn)
	for dbname in databases:
		msg_fn("Processing database %s" % dbname)
		params["database"] = dbname
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		drop_empty_partitions(conn, args.exclude_tables, args.dry_run, msg_fn)

if __name__ == "__main__":
	main()
