import yaml
from collections import defaultdict
from slpp import slpp as lua #pip install git+https://github.com/SirAnthony/slpp
import pandas as pd
from math import ceil
from datetime import datetime as dt

pd.options.mode.chained_assignment = None  # default='warn'

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


def generate_inventory(verbose=False):
    """ Reads and reformats the Arkinventory data file into a pandas dataframe
    Loads yaml files to specify item locations and specific items of interest
    Saves down parquet file ready to go
    """
    settings = get_general_settings() 
    data = read_lua('ArkInventory')

    characters = data['ARKINVDB']['global']['player']['data'] 

    # Search through inventory data to create dictionary of all items and counts, also counts total monies
    monies = {}
    character_inventories = defaultdict(str)
    raw_data = []

    for ckey in characters:
        character = characters[ckey]
        character_name = ckey.split(' ')[0]
        character_inventories[character_name] = {}

        monies[ckey] = int(character.get('info').get('money', 0))

        # Get Bank, Inventory, Character, Mailbox etc
        location_slots = character.get('location', [])


        for lkey in location_slots:
            items = defaultdict(int)
            if lkey not in settings['location_info']:
                continue
            else:
                loc_name = settings['location_info'][lkey]

            location_slot = location_slots[lkey]
            if location_slot:
                bag_slots = location_slot['bag']

                # Get the items from each of the bags, add to master list
                for bag in bag_slots:
                    for item in bag.get('slot', []):                       
                        if item.get('h') and item.get('count'):
                            item_name = item.get('h').split('[')[1].split(']')[0]

                            items[item_name] += item.get('count')

            for item_name, item_count in items.items():
                raw_data.append((character_name, loc_name, item_name, item_count))

            character_inventories[character_name][settings['location_info'][lkey]] = items

    # Convert information to dataframe
    cols = ['character', 'location', 'item', 'count', 'timestamp']
    df = pd.DataFrame(raw_data)

    df['timestamp'] = dt.now()
    df.columns = cols
    df.to_parquet('intermediate/inventory.parquet', compression='gzip')

    df_monies = pd.Series(monies)
    df_monies.name = 'monies'
    df_monies = pd.DataFrame(df_monies)
    df_monies['timestamp'] = dt.now()

    df_monies.to_parquet('intermediate/monies.parquet', compression='gzip')

    if verbose:
        print(f"Inventory formatted. {len(df)} records, {int(df_monies['monies'].sum()/10000)} total money across chars")
        

    # Uncomment below for
    # full scandata reset

    # cols = ['monies', 'timestamp']
    # auction_scandata_reset = pd.DataFrame(columns=cols)
    # auction_scandata_reset.to_parquet('full/monies.parquet', compression='gzip')

    # cols = ['character', 'location', 'item', 'count', 'timestamp']
    # auction_scandata_reset = pd.DataFrame(columns=cols)
    # auction_scandata_reset.to_parquet('full/inventory.parquet', compression='gzip')

    # Uncomment above for
    # full scandata reset  

    inventory_repo = pd.read_parquet('full/inventory.parquet')
    inventory_repo.to_parquet('full_backup/inventory.parquet', compression='gzip')

    monies_repo = pd.read_parquet('full/monies.parquet')
    monies_repo.to_parquet('full_backup/monies.parquet', compression='gzip')

    updated = '*not*'
    if df['timestamp'].max() > inventory_repo['timestamp'].max():
        updated = ''
        inventory_repo = inventory_repo.append(df)
        inventory_repo.to_parquet('full/inventory.parquet', compression='gzip')

        monies_repo = monies_repo.append(df_monies)
        monies_repo.to_parquet('full/monies.parquet', compression='gzip')    

    unique_periods = len(inventory_repo['timestamp'].unique())

    if verbose:
        print(f"Inventory full repository. {len(inventory_repo)} records with {unique_periods} snapshots. Repository has {updated} been updated this run")


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


def generate_auction_scandata(verbose=False):
    """ Snapshot of all AH prices from latest scan
        Reads the raw scandata from both accounts, cleans and pulls latest only
        Saves latest scandata to intermediate and adds to a full database with backup
    """
    raw_scan_data = read_lua('Auc-ScanData', merge_account_sources=False)

    cleaned_scan_data = {account: format_auction_data(auction_data) for account, auction_data in raw_scan_data.items()}

    latest_account_access = None
    latest_account_time = pd.to_datetime('1985-05-15')
    for account, auction_data in cleaned_scan_data.items():
        if auction_data['timestamp'].max() > latest_account_time:
            latest_account_time = auction_data['timestamp'].max()
            latest_account_access = account

    auction_data = cleaned_scan_data[latest_account_access]

    # Saves latest scan to intermediate (immediate)
    auction_data.to_parquet('intermediate/auction_scandata.parquet', compression='gzip')
    
    if verbose:
        print(f"Auction scandata loaded and cleaned. {len(auction_data)} records, last updated {latest_account_time} by {latest_account_access}")

    # Uncomment below for
    # full scandata reset

    # # # ensure cols are same as those used in clean
    #cols = ["timestamp", "item", "count", "price", "agent", "price_per"]
    #auction_scandata_reset = pd.DataFrame(columns=cols)
    #auction_scandata_reset.to_parquet('full/auction_scandata.parquet', compression='gzip')
    
    # Uncomment above for
    # full scandata reset

    auction_scan_minprice = auction_data[auction_data['price_per']!=0]
    auction_scan_minprice['price_per'] = auction_scan_minprice['price_per'].astype(int)
    auction_scan_minprice.loc[:, 'auction_type'] = 'market'
    auction_scan_minprice = auction_scan_minprice.groupby(['item', 'timestamp'])['price_per'].min().reset_index()

    auction_scan_minprice.to_parquet('intermediate/auction_scan_minprice.parquet', compression='gzip')

    # Saves full backup and adds latest to full
    auction_data_repo = pd.read_parquet('full/auction_scandata.parquet')
    auction_data_repo.to_parquet('full_backup/auction_scandata.parquet', compression='gzip')
    auction_scan_minprice_repo = pd.read_parquet('full/auction_scan_minprice.parquet')
    auction_scan_minprice_repo.to_parquet('full_backup/auction_scan_minprice.parquet', compression='gzip')
    
    updated = '*not*'
    if auction_data['timestamp'].max() > auction_data_repo['timestamp'].max():
        updated = ''
        auction_data_repo = auction_data_repo.append(auction_data)
        auction_data_repo.to_parquet('full/auction_scandata.parquet', compression='gzip')

        auction_scan_minprice_repo = auction_scan_minprice_repo.append(auction_scan_minprice)
        auction_scan_minprice_repo.to_parquet('full/auction_scan_minprice.parquet', compression='gzip')
    
    unique_periods = len(auction_data_repo['timestamp'].unique())
    
    if verbose:
        print(f"Auction scandata full repository. {len(auction_data_repo)} records with {unique_periods} snapshots. Repository has {updated} been updated this run")


def generate_auction_activity(verbose=False):
    """ Generates auction history parquet file with auctions of interest.
        Reads and parses Beancounter auction history across all characters
        Works the data into a labelled and cleaned pandas before parquet saves
    """
    relevant_auction_types = ['failedAuctions', 'completedAuctions', 'completedBidsBuyouts']       

    settings = get_general_settings()
    data = read_lua('BeanCounter')

    # Generates BeanCounters id:item_name dict
    num_item = {}
    for key, item_raw in data['BeanCounterDBNames'].items():
        item_name = item_raw.split(";")[1]
        num_item[key.split(":")[0]] = item_name

    # Parses all characters relevant listings into flat list
    parsed = []
    for character, auction_data in data['BeanCounterDB']['Grobbulus'].items():
        for auction_type, item_listings in auction_data.items():
            if auction_type in relevant_auction_types: 
                auction_name = settings['auction_type_labels'][auction_type]      
                for item_id, listings in item_listings.items():            
                    for _, listing in listings.items():
                        for auction in listing:
                            parsed.append([auction_name] + [num_item[item_id]] + [character] + auction.split(';'))

    # Setup as pandas dataframe, remove irrelevant columns
    df = pd.DataFrame(parsed)
    df = df.drop([4,5,6,8,11,12], axis=1)

    cols = ["auction_type", "item", "character", "count", "price", "agent", "timestamp"]
    df.rename(columns=dict(zip(df.columns, cols)), inplace=True)

    df = df[df['price']!='']
    df['price'] = df['price'].astype(int)
    df['count'] = df['count'].astype(int)

    df['price_per'] = round(df['price'] / df['count'], 4)
    df['timestamp'] = df['timestamp'].apply(lambda x: dt.fromtimestamp(int(x)))

    if verbose:
        print(f"Auction actions full repository. {df.shape[0]} records")
    df.to_parquet('full/auction_activity.parquet', compression='gzip')

    # user_items = load_items()
    # item_labels = {item: details['snatch_group'] for item, details in user_items.items()}

    # df_interest = df.loc[df[df['item'].isin(user_items)].index]
    # df_interest['snatch_group'] = df_interest['item'].replace(item_labels)

    # if verbose:
    #     print(f"{df_interest.shape[0]} auction events of interest")

    # df_interest.sort_values('timestamp').to_parquet('intermediate/auctions.parquet', compression='gzip')