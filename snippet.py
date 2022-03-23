'''
Getting average mining time per day of week for the latests blocks
Find more information at https://github.com/spicehq/samples
'''
import os
from pathlib import Path
import ssl
import tempfile
import urllib.request

from pyarrow import flight

# Check for gRPC certificates
if not (Path(Path.cwd().absolute().anchor) / 'usr' / 'share' / 'grpc' / 'roots.pem').exists():
    env_name = 'GRPC_DEFAULT_SSL_ROOTS_FILE_PATH'
    if env_name not in os.environ or not Path(os.environ[env_name]).exists():
        tls_root_certs = Path(tempfile.gettempdir()) / 'roots.pem'
        if not Path(tls_root_certs).exists():
            print('Downloading gRPC certificates')
            ssl._create_default_https_context = ssl._create_unverified_context
            urllib.request.urlretrieve('https://pki.google.com/roots.pem', str(tls_root_certs))
        os.environ[env_name] = str(tls_root_certs)

client = flight.connect('grpc+tls://flight.spiceai.io')
token_pair = client.authenticate_basic_token('', 'API_KEY')
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
