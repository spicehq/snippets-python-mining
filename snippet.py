'''
Getting average mining time per day of week for the latests blocks
Find more information at https://github.com/spicehq/samples
'''

from pyarrow import flight

api_key = '3031|abcd'  # TODO: replace with API key
client = flight.connect('grpc+tls://flight.spiceai.io')
token_pair = client.authenticate_basic_token('', api_key)
options = flight.FlightCallOptions(headers=[token_pair])

flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(
    'SELECT number, "timestamp" FROM eth.blocks ORDER BY number DESC LIMIT 1000000;'), options)
reader = client.do_get(flight_info.endpoints[0].ticket, options)
data = reader.read_pandas()

data = data.iloc[::-1]  # reverse order
data['time'] = data['timestamp'].astype('datetime64[s]')
data['dayofweek'] = data['time'].dt.dayofweek
data['mining_time'] = data['timestamp'].diff().dropna()  # Dropping firt row as diff will output NaN
data = data.groupby(['dayofweek']).mean()
print(data)
