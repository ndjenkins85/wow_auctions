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
    user_items = load_items()
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

    # Add label and stack size info and save
    item_labels = {item: details['label'] for item, details in user_items.items()}
    stack_sizes = {item: details.get('max_stack') for item, details in user_items.items()}

    df['label'] = df['item'].apply(lambda x: item_labels.get(x))
    df['stack_size'] = df['item'].apply(lambda x: stack_sizes.get(x))
    df.to_parquet('intermediate/inventory_full.parquet', compression='gzip')    

    # Create more focused version
    df_subset = df.dropna().copy()
    df_subset['stack_size'] = df_subset['stack_size'].astype(int)

    df_subset.to_parquet('intermediate/inventory.parquet', compression='gzip')
    if verbose:
        print(f'Saving inventory data with {df_subset.shape} shape')


def get_character_needs(character=[]):
    """ Looks through character inventory and their wish list to determine if anything is missing 
        Can take single character as string or list of characters   
    """
    if type(character) is str:
        character = [character]
    elif type(character) is not list:
        raise Exception('Expected character input as string or list')

    df = pd.read_parquet('intermediate/inventory.parquet')

    user_items = load_items()
    self_demand = {item: details.get('self_demand', {}) for item, details in user_items.items()}

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

    user_items = load_items()
    item_labels = {item: details['label'] for item, details in user_items.items()}

    df_interest = df.loc[df[df['item'].isin(user_items)].index]
    df_interest['labels'] = df_interest['item'].replace(item_labels)

    if verbose:
        print(f"{df_interest.shape[0]} auction events of interest")

    df_interest.sort_values('timestamp').to_parquet('intermediate/auctions.parquet', compression='gzip')


def add_item_prices(MAX_RECENT=25, MIN_RECENT=5, MAX_SUCCESS=250, MIN_SUCCESS=10, MIN_PROFIT_MARGIN=1000):
    """ Enriches the user items listing with pricing data from analysis
        Analysed within recent X transactions; yields average buy_price, sell_price
        Calculates material costs for any crafted item, and other costs
        Calculates profit per item and minimum item price
    """

    user_items = load_items()
    df_raw = pd.read_parquet('intermediate/auctions.parquet')
    df = df_raw.copy()

    df['rank'] = df.groupby(['auction_type', 'item'])['timestamp'].rank(ascending=False)
    recent_df = df[df['rank']<=MAX_RECENT]

    item_prices = recent_df.groupby(['auction_type', 'item'])['price_per'].mean().unstack().T
    item_counts = recent_df.groupby(['auction_type', 'item'])['price_per'].count().unstack().T.fillna(0)
    item_prices = item_prices[item_counts>=MIN_RECENT]
    item_std = df.groupby(['item'])['price_per'].std().to_dict()

    # Ensures the user_items data is populated if not present in auction history, with dummy backup data
    for item in user_items:
        if item not in item_prices.index:
            item_prices.loc[item] = user_items[item]['backup_price']

    # Given the average recent buy price, calculate material costs per item
    item_costs = {}
    for item, details in user_items.items():
        material_cost = 0
        for ingredient, count in details.get('made_from', {}).items():
            material_cost += item_prices.loc[ingredient, 'buy_price'] * count
        if material_cost is not 0:
            item_costs[item] = int(material_cost)

    # Create auction success metrics
    df_success = df_raw.copy()

    # Look at the most recent X sold or failed auctions
    df_success = df_success[df_success['auction_type'].isin(['sell_price', 'failed'])]
    df_success['rank'] = df_success.groupby(['item'])['timestamp'].rank(ascending=False)

    # Limit to recent successful auctions
    df_success = df_success[df_success['rank']<=MAX_SUCCESS]
    df_success['auction_success'] = df_success['auction_type'].replace({'sell_price': 1, 'failed': 0})
    # Ensure theres at least some auctions for a resonable ratio
    df_success = df_success[df_success['rank']>=MIN_SUCCESS]

    # Calcualte success%
    df_success = df_success.groupby('item')['auction_success'].mean()
    item_success = df_success.to_dict()            
        
    # Save enrichments to user_items dict
    for item, details in user_items.items():
        if item in item_costs:
            user_items[item]['material_cost'] = int(item_costs[item])
        else:
            user_items[item]['material_cost'] = None
            
        # TODO: May need to fix the formatting of outputs of .nan if its a problem downstream
        if item in item_prices.index:
            user_items[item]['buy_price'] = float(item_prices.loc[item, 'buy_price'])
        else:
            user_items[item]['buy_price'] = None

        if item in item_prices.index:            
            user_items[item]['sell_price'] = float(item_prices.loc[item, 'sell_price'])            
        else:
            user_items[item]['sell_price'] = None
            
        if item in item_std:
            user_items[item]['std_price'] = item_std[item]
        else:
            user_items[item]['std_price'] = None
            
        if item in item_success:
            user_items[item]['auction_success'] = item_success[item]
        else:
            user_items[item]['auction_success'] = None            
            
            
            
    profit = pd.DataFrame.from_dict(user_items).T
    profit = profit[['sell_price', 'material_cost', 'full_deposit', 'std_price', 'auction_success']].dropna()

    profit['gross_profit'] = (profit['sell_price'] - 
                        profit['material_cost'] - 
                        (profit['sell_price'] * 0.05) - 
                        (profit['full_deposit'] * (1 - profit['auction_success'])))

    profit['min_list_price'] = ((profit['material_cost'] + 
                                 (profit['full_deposit'] * (1 - profit['auction_success']))) + 
                                MIN_PROFIT_MARGIN) * 1.05

    # Round to nearest silver, bit more readable
    profit['min_list_price'] = (profit['min_list_price'].astype(float) / 100).astype(int)*100

    for item, details in user_items.items():
        if item in profit.index:
            user_items[item]['gross_profit'] = int(profit.loc[item, 'gross_profit'])
            user_items[item]['min_list_price'] = int(profit.loc[item, 'min_list_price'])
            
    with open('intermediate/items_enriched.yaml', 'w') as f:
        yaml.dump(user_items, f)    