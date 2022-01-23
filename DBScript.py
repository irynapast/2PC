import random

import psycopg2
from random import Random

def run_booking():
    last_id = Random().randint(0, 100)
    print(f'last id={last_id}')
    print('opening connections')
    conn_flights = psycopg2.connect("dbname=fly user=postgres password=")
    conn_hotels = psycopg2.connect("dbname=hotel user=postgres password=")
    conn_accounts = psycopg2.connect("dbname=account user=postgres password=")

    xid_flights = conn_flights.xid(last_id, str(last_id), "conn_flights")
    print(f'xid_flights = {xid_flights}')
    conn_flights.tpc_begin(xid_flights)
    xid_hotels = conn_hotels.xid(last_id, str(last_id), "conn_hotels")
    print(f'xid_hotels = {xid_hotels}')
    conn_hotels.tpc_begin(xid_hotels)
    xid_accounts = conn_accounts.xid(last_id, str(last_id), "conn_accounts")
    print(f'xid_accounts = {xid_accounts}')
    conn_accounts.tpc_begin(xid_accounts)

    curr_flights = conn_flights.cursor()
    curr_flights.execute(flight_insert())
    curr_hotels = conn_hotels.cursor()
    curr_hotels.execute(hotel_insert())
    curr_accounts = conn_accounts.cursor()
    curr_accounts.execute(account_update())

    try:
        conn_flights.tpc_prepare(xid_flights)
        conn_hotels.tpc_prepare(xid_hotels)
        conn_accounts.tpc_prepare(xid_accounts)
        print('prepared')
    except psycopg2.DatabaseError as error:
        print(f'error preparing {error}')
        conn_flights.tpc_rollback(xid_flights)
        conn_hotels.tpc_rollback(xid_hotels)
        conn_accounts.tpc_rollback(xid_accounts)
        return
    try:
        conn_flights.tpc_commit(xid_flights)
        conn_hotels.tpc_commit(xid_hotels)
        conn_accounts.tpc_commit()
        print('committed')
    except psycopg2.DatabaseError as error:
        print(f'error committing {error}')
        conn_flights.tpc_rollback(xid_flights)
        conn_hotels.tpc_rollback(xid_hotels)
        conn_accounts.tpc_rollback(xid_accounts)
        return

    print('closing connections')
    conn_flights.close()
    conn_hotels.close()
    conn_accounts.close()


def flight_insert():
    return "insert into flight_booking (client_name, flight_number, from_, to_, date) values ('client name', 'flight number', 'Lviv', 'Somewhere', now())"


def hotel_insert():
    return "insert into hotel_booking (client_name, hotel_number, arrival, departure) values ('client name', 'hotel number', now(), now())"


def account_update():
    return "update account set amount = amount - 200 where account_id = 1"

def make_xid():
    pass

if __name__ == '__main__':
    run_booking()
