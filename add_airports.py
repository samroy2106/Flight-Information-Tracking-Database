import sys, csv, psycopg2

psql_user = 'samroy2106'
psql_db = 'samroy2106'
psql_password = 'V00878774'
psql_server = 'studdb2.csc.uvic.ca'
psql_port = 5432

if len(sys.argv) < 2:
    print("Usage: %s <input file>"%sys.argv[0],file=sys.stderr)
    sys.exit(1)

input_filename = sys.argv[1]

# Open your DB connection here
conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

cursor = conn.cursor()

with open(input_filename) as f:
	for row in csv.reader(f):
		if len(row) == 0:
			continue #Ignore blank rows
		if len(row) != 4:
			print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
			conn.rollback()
			break
		airport_code,airport_name,country,international = row
		if international.lower() not in ('true','false'):
			print('Error: Fourth value in each line must be either "true" or "false"',file=sys.stderr)
			conn.rollback()
			break
		international = international.lower() == 'true'

		try:
			cursor.execute("insert into airports values (%s, %s, %s, %s);", (airport_code, airport_name, country, international))
			conn.commit()
		except psycopg2.ProgrammingError as err:
			print("Caught a ProgrammingError:",file=sys.stderr)
			print(err,file=sys.stderr)
			conn.rollback()
		except psycopg2.IntegrityError as err:
			print("Caught an IntegrityError:",file=sys.stderr)
			print(err,file=sys.stderr)
			conn.rollback()
		except psycopg2.InternalError as err:
			print("Caught an IntegrityError:",file=sys.stderr)
			print(err,file=sys.stderr)
			conn.rollback()

cursor.close()
conn.close()
