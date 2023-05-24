# TODO: remove this script before merge
text = """
arxiv
barcodenumber
blockcypher
coinaddrvalidator
cryptoaddress
cryptocompare
dataprofiler
disposable_email_domains
edtf_validate
ephem
geonamescache
geopandas
geopy
global-land-mask
gtin
holidays
ipwhois
isbnlib
langid
pgeocode
phonenumbers
price_parser
primefac
prophet
pwnedpasswords
py-moneyed
pydnsbl
pygeos
pyogrio
python-geohash
python-stdnum
pyvat
rtree
schwifty
scikit-learn
shapely
simple_icd_10
sympy
tensorflow
timezonefinder
us
user_agents
uszipcode
yahoo_fin
zipcodes
"""

reqs = " ".join(text.splitlines())
group_name = "all-contrib-dev"
print(f"poetry add --group {group_name}{reqs}")
