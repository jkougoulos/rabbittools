#!/usr/bin/python3

import pika
import sys
import select
import time
import argparse
#import pprint

#pp = pprint.PrettyPrinter(indent=4)


chansend = None
conn = None
maxlines_toread = None
maxwrite_buffer = None
rcvexchange = None
routekey = None
rcvvhost = None
ttl = None
nottl = None
qos = None

def handle_tick():
    global chansend
    global conn
    global maxlines_toread
    global maxwrite_buffer
    global qos
    global ttl

    count = 0

    props = None
    if nottl:
        props = pika.BasicProperties(
                                      content_type='text/plain',                                                                                                                                  
                                      delivery_mode=qos,
        )
    else:
        props = pika.BasicProperties(
                                      content_type='text/plain',                                                                                                                                  
                                      delivery_mode=qos,
                                      expiration=ttl
        )
 

    # check if we have anything in stdin, we have read less than 512 line block, and the write output buffer has less than 1MB
    curr_buf = conn._get_write_buffer_size()
    if curr_buf < maxwrite_buffer:   # our buffer is high, probably rabbit applies tcp backpressure
        consec_flow = 0
        while select.select([sys.stdin,],[],[],0.0)[0] and count < maxlines_toread:  # we have lines to read and we read in blocks of maxlines
            line = sys.stdin.buffer.readline()
            if line is b'':
                conn.close()
                return 

            count += 1
            if nottl:
                props
            chansend.basic_publish(exchange=rcvexchange,
                            routing_key=routekey,
                            body=line,
                            properties=props
            )
        delay = (maxlines_toread - count ) / maxlines_toread    # adjust delay for next read to percentage of lines read - we don't want to select all the time 
    else:
        delay = 0.3               # tcp backpressure detected - waiting a bit

    conn.ioloop.call_later(delay, handle_tick)

    
def on_conn_open(connection):
    connection.channel(on_open_callback=on_channel_open)


def on_channel_open(channel):
    global chansend

    chansend = channel
    conn.ioloop.call_later(0.01, handle_tick )


def on_conn_close(connection, exc):
    print( str(exc) )
    sys.exit(0)

def on_conn_error( connection, exc ):
    raise exc 


parser = argparse.ArgumentParser(description='Send lines received via stdin to RabbitMQ Exchange')

parser.add_argument('--exchange', help="Exchange to receive the messages", required=True )
parser.add_argument('--routekey', help="routing key of messages", default='' )
parser.add_argument('--host', default='localhost', help="RabbitMQ host to send the messages (default is: %(default)s)" )
parser.add_argument('--user', default='guest', help="Username to use for connection to RabbitMQ (default is: %(default)s)" )
parser.add_argument('--password', default='guest', help="Username to use for connection to RabbitMQ (default is: %(default)s)" )
parser.add_argument('--vhost', default='/', help="Virtual host to use for connection to RabbitMQ (default is: %(default)s)" )
parser.add_argument('--lineblock', default=512 , help="Maximum number of lines to read on each round (default is: %(default)s)", type=int )
parser.add_argument('--maxnetwritebuff', default=1*1024*1024 , help="Maximum network buffer size to use while RabbitMQ applies backpressure (default is: %(default)s)", type=int )
parser.add_argument('--ttl', default='8000', help="TTL of messages in milliseconds (default is: %(default)s)" )
parser.add_argument('--nottl', help="Ignore TTL", action='store_true' )
parser.add_argument('--qos', default=1, help="QoS of messages - 1: transient - 2: persisten (default is: %(default)s)", choices = [1,2], type=int )


args = parser.parse_args()

host = args.host
user = args.user
password = args.password
maxlines_toread = args.lineblock
maxwrite_buffer = args.maxnetwritebuff
rcvexchange = args.exchange
rcvvhost = args.vhost
routekey = args.routekey
ttl = args.ttl
nottl = args.nottl
qos = args.qos

creds = pika.PlainCredentials(user, password )
parameters = pika.ConnectionParameters(host=host, credentials=creds, virtual_host=rcvvhost)

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
except Exception as exc:
    print( str(exc) )
