import psycopg2, sys

psql_user = 'samroy2106'
psql_db = 'samroy2106'
psql_password = 'V00878774'
psql_server = 'studdb2.csc.uvic.ca'
psql_port = 5432

def print_header(passenger_id, passenger_name):
    print("Itinerary for %s (%s)"%(str(passenger_id), str(passenger_name)) )

def print_entry(flight_id, airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, aircraft_model):
    print("Flight %-4s (%s):"%(flight_id, airline))
    print("    [%s] - [%s] (%s minutes)"%(departure_time, arrival_time,duration_minutes))
    print("    %s -> %s (%s: %s)"%(source_airport_name, dest_airport_name, aircraft_id, aircraft_model))

# Open your DB connection here
conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

cursor = conn.cursor()

if len(sys.argv) < 2:
    print('Usage: %s <passenger id>'%sys.argv[0], file=sys.stderr)
    sys.exit(1)

passenger_id = sys.argv[1]

try:
	passenger_id = int(passenger_id)
except ValueError:
	print("Invalid input \"%s\""%passenger_id, file=sys.stderr)
	sys.exit(0)

cursor.execute("""with step1 as (select * from reservations where passenger_id = %s),
					step2 as (select flight_id, passenger_id, source, destination, departure, arrival, aircraft
						from flights right join step1 on flights.id = step1.flight_id),
					step3 as (select * from step2 left join passengers on step2.passenger_id = passengers.id),
					step4 as (select * from step3 left join aircraft on step3.aircraft = aircraft.id)
					select name, flight_id, airline, source, destination, departure, arrival, extract(epoch from (arrival-departure)/60) as duration, aircraft, model_name
						from step4 order by departure asc;""", (passenger_id,))

rows_found = 0
while True:
	row = cursor.fetchone()
	if row is None:
		break
	rows_found += 1
	passenger_name = row[0]
	flight_id = row[1]
	airline = row[2]
	source = row[3]
	destination = row[4]
	departure = row[5]
	arrival = row[6]
	duration = int(row[7])
	aircraft_id = row[8]
	aircraft_model = row[9]

	print_header(passenger_id, passenger_name)
	print_entry(flight_id, airline, source, destination, departure, arrival, duration, aircraft_id, aircraft_model)

print()
print("The itinerary contains %d flight(s)"%rows_found)

cursor.close()
conn.close()
