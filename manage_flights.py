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
		action = row[0]
		if action.upper() == 'DELETE':
			if len(row) != 2:
				print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
				conn.rollback()
				break
			flight_id = row[1]
			#DELETE action
			try:
				cursor.execute("delete from flights where id = %s;", (flight_id,))
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
		elif action.upper() in ('CREATE','UPDATE'):
			if len(row) != 8:
				print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
				conn.rollback()
				break
			flight_id = row[1]
			airline = row[2]
			src,dest = row[3],row[4]
			departure, arrival = row[5],row[6]
			aircraft_id = row[7]
			#CREATE and UPDATE actions
			try:
				if(action.upper() == 'CREATE'):
					cursor.execute("insert into flights values (%s, %s, %s, %s, %s, %s, %s);", (flight_id, airline, src, dest, departure, arrival, aircraft_id))
					conn.commit()
				elif(action.upper() == 'UPDATE'):
					cursor.execute("update flights set airline = %s, source = %s, destination = %s, departure = %s, arrival = %s, aircraft = %s where id = %s;", (airline, src, dest, departure, arrival, aircraft_id, flight_id))
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
		else:
			print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
			conn.rollback()
			break

cursor.close()
conn.close()
