import psycopg2, sys

psql_user = 'samroy2106'
psql_db = 'samroy2106'
psql_password = 'V00878774'
psql_server = 'studdb2.csc.uvic.ca'
psql_port = 5432

def print_entry(flight_id, airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, aircraft_model, seating_capacity, seats_full):
    print("Flight %s (%s):"%(flight_id,airline))
    print("    [%s] - [%s] (%s minutes)"%(departure_time,arrival_time,duration_minutes))
    print("    %s -> %s"%(source_airport_name,dest_airport_name))
    print("    %s (%s): %s/%s seats booked"%(aircraft_id, aircraft_model,seats_full,seating_capacity))

# Open your DB connection here
conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

cursor = conn.cursor()

cursor.execute("""with step1 as (select id, departure, arrival, (arrival-departure) as diff_hours from flights),
				step2 as (select id, extract(hours from diff_hours)*60 as mins1, extract(minutes from diff_hours) as mins2 from step1),
				step3 as (select flights.id, flights.airline, departure, arrival, source, destination, aircraft, model_name, capacity
					from flights join aircraft on flights.aircraft = aircraft.id),
				step4 as (select flight_id, count(*) as num_reservations from reservations group by flight_id),
				step5 as (select id, airline, departure, arrival, (mins1+mins2) as total_duration, source, destination, aircraft, model_name, capacity
					from step2 natural join step3)
				select id, airline, departure, arrival, total_duration, source, destination, aircraft, model_name, num_reservations, capacity
					from step4 right join step5 on step4.flight_id = step5.id
					order by departure asc;""")

rows_found = 0
while True:
	row = cursor.fetchone()
	if row is None:
		break
	rows_found += 1
	flight_id = row[0]
	airline = row[1]
	departure = row[2]
	arrival = row[3]
	total_duration = int(row[4])
	source = row[5]
	destination = row[6]
	aircraft_id = row[7]
	aircraft_model = row[8]

	if(row[9] is None):
		num_reservations = 0
	else:
		num_reservations = row[9]

	total_capacity = row[10]

	print_entry(flight_id, airline, source, destination, departure, arrival, total_duration, aircraft_id, aircraft_model, total_capacity, num_reservations)

print()
print("%d flights found"%rows_found)

cursor.close()
conn.close()
