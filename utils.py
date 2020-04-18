"""
This file contains shorter utilities to write/save raw files, and change data formats
"""

import yaml
from slpp import slpp as lua #pip install git+https://github.com/SirAnthony/slpp
from datetime import datetime as dt

    
def source_merge(a, b, path=None):
    "merges b into a"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                source_merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                pass #raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


def read_lua(datasource: str, merge_account_sources=True):
    """ Attempts to read lua from the given locations
    """

    account_data = {'BLUEM': None, '396255466#1': None}
    for account_name in account_data.keys():
        path_live = f"/Applications/World of Warcraft/_classic_/WTF/Account/{account_name}/SavedVariables/{datasource}.lua" 

        with open(path_live, 'r') as f:
            account_data[account_name] = lua.decode('{'+f.read()+'}')

    if merge_account_sources:
        return source_merge(account_data['BLUEM'], account_data['396255466#1'])
    else:
         return account_data


def load_items():
    """ Loads and returns the user created YAML file of interesting items and their stack sizes
    """
    with open('config/items.yaml', 'r') as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def get_general_settings():
    """Gets general program settings
    """
    with open('config/general_settings.yaml', 'r') as f:
        return yaml.load(f, Loader=yaml.FullLoader) 


def format_auction_data(ac):
    """ Reads the raw scandata dict dump and converts to usable dataframe    
    """
    auction_data = []

    for rope in ac['AucScanData']['scans']['Grobbulus']['ropes']:
        auctions = rope[9:-3].split('},{')
        for auction in auctions:
            auction_data.append(auction.split('|')[-1].split(','))

    # Contains lots of columns, we ignore ones we likely dont care about
    # We apply transformations and relabel
    df = pd.DataFrame(auction_data)
    df['item'] = df[8].str.replace('"','')
    df['count'] = df[10].replace('nil', 0).astype(int)
    df['price'] = df[16].astype(int)
    df['agent'] = df[19].str.replace('"','')
    df['timestamp'] = df[7].apply(lambda x: dt.fromtimestamp(int(x)))

    # There is some timing difference in the timestamp, we dont really care we just need time of pull
    df['timestamp'] = df['timestamp'].max()

    df = df[df['count']>0]
    df['price_per'] = df['price'] / df['count']

    cols = ["timestamp", "item", "count", "price", "agent", "price_per"]
    df = df[cols]

    return df


def get_item_codes():
    """
    Reads the beancounter database and produces codes per item dict
    """
    data = read_lua('BeanCounter')
    item_code = {}
    for keypart, itempart in data['BeanCounterDBNames'].items():
        key = keypart.split(':')[0]
        item = itempart.split(';')[1]
        item_code[item] = key
    return item_code


def write_lua(data, account='396255466#1', name='Auc-Advanced'):
    """
    Write python dict as lua object
    """
    lua_print = "\n"
    for key in data.keys():
        lua_print += f'{key} = '+ dump_lua(data[key]) + '\n'

    location = f'/Applications/World of Warcraft/_classic_/WTF/Account/{account}/SavedVariables/{name}.lua'        
    with open(location, 'w') as f:
        f.write(lua_print)  


def dump_lua(data):
    """
    Borrowed code to write python dict as lua format(ish)
    """
    if type(data) is str:
        return f'"{data}"'
    if type(data) in (int, float):
        return f'{data}'
    if type(data) is bool:
        return data and "true" or "false"
    if type(data) is list:
        l = "{"
        l += ", ".join([dump_lua(item) for item in data])
        l += "}"
        return l
    if type(data) is dict:
        t = "{"
        t += ", ".join([f'["{k}"]={dump_lua(v)}' for k,v in data.items()])
        t += "}"
        return t
    print(f"Unknown type {type(data)}")
     