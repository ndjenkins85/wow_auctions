import yaml
from collections import defaultdict
from slpp import slpp as lua #pip install git+https://github.com/SirAnthony/slpp
import pandas as pd
from math import ceil
from datetime import datetime as dt

def read_lua(datasource: str):
    """ Attempts to read lua from the given locations
    """
    path_live = f"/Applications/World of Warcraft/_classic_/WTF/Account/BLUEM/SavedVariables/{datasource}.lua" 
    path_local = f"data/{datasource}.lua"

    try:
        with open(path_live, 'r') as f:
            data = lua.decode('{'+f.read()+'}')
    except:        
        with open(path_local, 'r') as f:
            data = lua.decode('{'+f.read()+'}')
    return data


def get_interesting_items_and_stacks():
    """ Loads and returns the user created YAML file of interesting items and their stack sizes
    """
    # Pulls our list of items of interest, and filters dataframe accordingly
    path_items_of_interest = 'config/items_of_interest.yaml'
    with open(path_items_of_interest, 'r') as f:
        items_of_interest = yaml.load(f, Loader=yaml.FullLoader)

    # Reformat items of interest data to get stack size, and item categories
    stack_sizes = {}
    categories = {}
    for cat, dic in items_of_interest.items():
        stack_sizes.update(dic)
        for item in dic:
            categories[item] = cat

    return categories, stack_sizes


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

    categories, stack_sizes = get_interesting_items_and_stacks()
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
    cols = ['character', 'location', 'item', 'count']
    df = pd.DataFrame(raw_data, columns=cols)

    # Save out the monies information
    total_monies = sum(list(monies.values()))/10000
    if verbose:
        print(f"Total Monies: {total_monies:.2f}")
    with open('intermediate/monies.txt', 'w') as f:
        f.write(str(total_monies))  

    # Add category and stack size info and save
    df['category'] = df['item'].apply(lambda x: categories.get(x))
    df['stack_size'] = df['item'].apply(lambda x: stack_sizes.get(x))
    df.to_parquet('intermediate/inventory_full.parquet', compression='gzip')    

    # Create more focused version
    df = df.dropna()
    df['stack_size'] = df['stack_size'].astype(int)
    df.to_parquet('intermediate/inventory.parquet', compression='gzip')
    if verbose:
        print(f'Saving inventory data with {df.shape} shape')


def get_character_needs(character):
    """ Looks through character inventory and their wish list to determine if anything is missing 
        Can take single character as string or list of characters   
    """
    # Loads inventory parquet
    df = pd.read_parquet('intermediate/inventory.parquet')
    
    # Load the self-demand / wish list
    path_self_demand = 'config/self_demand.yaml'
    with open(path_self_demand, 'r') as f:
        self_demand = yaml.load(f, Loader=yaml.FullLoader)    
    
    if type(character) is str:
        character = [character]
    elif type(character) is not list:
        raise Exception('Expected character input as string or list')

    character_needs = {}
    for c in character:
        # Find the character of interest only
        char_needs = {}
        for item, chars in self_demand.items():
            for char, count in chars.items():
                if char == c:
                    char_needs[item] = count

        # Group their locations
        char_df = df[df['character']==c]
        char_df = char_df.groupby('item').sum()[['count']]

        char_df['item'] = char_df.index
        char_df['count_needed'] = char_df['item'].apply(lambda x: char_needs.get(x, 0))

        char_df['short'] = char_df['count'] - char_df['count_needed']
        items_short = char_df[char_df['short']<0]['short'].to_dict()

        character_needs[c] = items_short

    return character_needs    


def get_mule_counts():
    """ Gets the accessible item counts across the mule characters
        Returns a dataframe with category and count fields
    """

    df = pd.read_parquet('intermediate/inventory.parquet')

    settings = get_general_settings()

    mules = ['Amazoni', 'Ihodl']
    locations = ['Inventory', 'Bank', 'Mailbox']
        
    ind = df[(df['character'].isin(mules))&(df['location'].isin(locations))].index

    df_mules = df.loc[ind]

    categories = df_mules[['item','category']].drop_duplicates().set_index('item')
    item_count = df_mules.groupby('item').sum()[['count']].join(categories)
    item_count = item_count.sort_values(['category','count'], ascending=[True, False])
    item_count = item_count[['category','count']]
    return item_count


def generate_auction_history(verbose=False):
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
        print(f"{df.shape[0]} auction events")
    df.to_parquet('intermediate/auctions_full.parquet', compression='gzip')

    categories, _ = get_interesting_items_and_stacks()
    df_interest = df[df['item'].isin(categories)]

    if verbose:
        print(f"{df_interest.shape[0]} auction events of interest")    
    df_interest.to_parquet('intermediate/auctions.parquet', compression='gzip')


def analyse_price(df, min_count=10):
    """ Given an auctions dataframe, generate mean and std price dataframes
    """
    price_mean = df.groupby(['auction_type','item']).mean()['price_per'].unstack().T
    price_count = df.groupby(['auction_type','item']).count()['price_per'].unstack().T.fillna(0).astype(int)
    price_std = df.groupby(['auction_type','item']).std()['price_per'].unstack().T

    price_mean = price_mean[price_count>=min_count]
    price_std = price_std[price_count>=min_count]
        
    return price_mean, price_std