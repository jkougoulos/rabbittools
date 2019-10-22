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

def handle_tick():
    global chansend
    global conn
    global maxlines_toread
    global maxwrite_buffer

    count = 0

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
            chansend.basic_publish(exchange=rcvexchange,
                            routing_key=routekey,
                            body=line,
                            properties=pika.BasicProperties(
                                            content_type='text/plain',
                                            delivery_mode=1
                            )
            )
        delay = (maxlines_toread - count ) / maxlines_toread    # adjust delay for next read to percentage of lines read 
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


args = parser.parse_args()

#pp.pprint(vars(args)) 

host = args.host
user = args.user
password = args.password
maxlines_toread = args.lineblock
maxwrite_buffer = args.maxnetwritebuff
rcvexchange = args.exchange
rcvvhost = args.vhost
routekey = args.routekey

#print( "host is: " + host)
#print( "user is: " + user)
#print( "password is: " + password)
#print( "maxlines is: " + str(maxlines_toread))
#print( "maxbuffer is: " + str(maxwrite_buffer))
#print( "exchange is: " + rcvexchange)
#print( "routing_key is: " + routekey)

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
