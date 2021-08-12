import psycopg2, sys

psql_user = 'samroy2106'
psql_db = 'samroy2106'
psql_password = 'V00878774'
psql_server = 'studdb2.csc.uvic.ca'
psql_port = 5432

def print_entry(aircraft_id, airline, model_name, num_flights, flight_hours, avg_seats_full, seating_capacity):
    print("%-5s (%s): %s"%(aircraft_id, model_name, airline))
    print("    Number of flights : %d"%num_flights)
    print("    Total flight hours: %d"%flight_hours)
    print("    Average passengers: (%.2f/%d)"%(avg_seats_full,seating_capacity))

# Open your DB connection here
conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

cursor = conn.cursor()

cursor.execute("""with step1 as (select aircraft, count(*) as num_flights from flights group by aircraft),
				step2 as (select aircraft, (arrival-departure) as hours from flights),
				step3 as (select aircraft, sum(hours) as flight_hours from step2 group by aircraft),
				step4 as (select aircraft, round((extract(epoch from flight_hours)/3600)) as flight_hours from step3),
				step5 as (select count(flights) as res_by_aircraft, aircraft from reservations left join flights on reservations.flight_id = flights.id group by aircraft),
				step6 as (select aircraft, count(*) as flights_by_aircraft from flights group by aircraft),
				step7 as (select aircraft, res_by_aircraft, flights_by_aircraft from step5 natural join step6),
				step8 as (select aircraft, (res_by_aircraft::decimal/flights_by_aircraft) as avg_seats_full from step7),
				step9 as (select id, airline, model_name, step1.num_flights as num_flights, capacity from aircraft left join step1 on aircraft.id = step1.aircraft),
				step10 as (select step9.id as id, step9.airline as airline, step9.model_name as model_name, step9.num_flights as num_flights, step4.flight_hours as flight_hours, step9.capacity as capacity from step9 left join step4 on step9.id = step4.aircraft)
				select step10.id as id, step10.airline, step10.model_name, step10.num_flights, step10.flight_hours, step8.avg_seats_full, step10.capacity
				from step10 left join step8 on step10.id = step8.aircraft order by id asc;""")

rows_found = 0
while True:
	row = cursor.fetchone()
	if row is None:
		break
	rows_found += 1
	aircraft_id = row[0]
	airline = row[1]
	model_name = row[2]

	if(row[3] is None):
		num_flights = 0
	else:
		num_flights = row[3]

	if(row[4] is None):
		flight_hours = 0
	else:
		flight_hours = row[4]

	if(row[5] is None):
		avg_seats_full = 0
	else:
		avg_seats_full = row[5]

	seating_capacity = row[6]

	print_entry(aircraft_id, airline, model_name, num_flights, flight_hours, avg_seats_full, seating_capacity)

print()
print("%d Aircraft found"%rows_found)

cursor.close()
conn.close()
