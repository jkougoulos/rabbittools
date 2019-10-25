#!/usr/bin/python3

import random 
import sys
import time
import re
import pprint
import argparse
import pika
import traceback

#pp = pprint.PrettyPrinter(indent=4)


chancons = None
conn = None
sndqueue = None
sndvhost = None
prefetch_msgs = None
autoack = None

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def on_conn_open(connection):
    global conn

    conn = connection
    conn.channel(on_open_callback=on_channelcons_open)


def on_channelcons_open(channel):
    global chancons
    global prefetch_msgs

    chancons = channel
    chancons.basic_qos( prefetch_count=prefetch_msgs, callback=on_basic_qos_ok)


def on_basic_qos_ok( _unused_frame ):
    start_consuming()

def start_consuming():
    global chancons
    global sndquueue
    global autoack

    chancons.basic_consume( sndqueue, on_message, auto_ack=autoack)


def on_conn_close(conn, exc):
    print( str(exc) )
    sys.exit(0)

def on_conn_error( connection, exc ):
    raise exc

def on_message(channel, method_frame, header_frame, body):
    global autoack

    try:
        print( body.decode(errors='ignore').rstrip('\n') )
    except Exception as exc:
        eprint( "\n----------------EXCEPTION START----------------" )
        eprint( traceback.format_exc() )
        eprint( '\n' )
        eprint( repr(exc) )
        eprint( "----------------EXCEPTION END----------------\n" )
    if not autoack:
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)


pp = pprint.PrettyPrinter(indent=4)

parser = argparse.ArgumentParser(description='Send lines received via stdin to RabbitMQ Exchange')

parser.add_argument('--queue', help="Queue sending the messages", required=True )
parser.add_argument('--host', default='localhost', help="RabbitMQ host to send the messages (default is: %(default)s)" )
parser.add_argument('--user', default='guest', help="Username to use for connection to RabbitMQ (default is: %(default)s)" )
parser.add_argument('--password', default='guest', help="Username to use for connection to RabbitMQ (default is: %(default)s)" )
parser.add_argument('--vhost', default='/', help="Virtual host to use for connection to RabbitMQ (default is: %(default)s)" )
parser.add_argument('--prefetch', default=64 , help="Maximum packets to receive for processing (default is: %(default)s)", type=int )
parser.add_argument('--autoack', help="Auto-acknowledge messages", action='store_true' )


args = parser.parse_args()

#pp.pprint(vars(args)) 

host = args.host
user = args.user
password = args.password
prefetch_msgs = args.prefetch
sndqueue = args.queue
sndvhost = args.vhost
autoack = args.autoack


creds = pika.PlainCredentials(user, password )
parameters = pika.ConnectionParameters(host=host, credentials=creds, virtual_host=sndvhost)

conn = pika.SelectConnection(
                                   parameters=parameters,
                                   on_open_callback=on_conn_open,
                                   on_close_callback=on_conn_close,
                                   on_open_error_callback=on_conn_error
)

try:
    conn.ioloop.start()
except KeyboardInterrupt:
    conn.close()
    conn.ioloop.start()

