import yaml
from collections import defaultdict
from slpp import slpp as lua #pip install git+https://github.com/SirAnthony/slpp
import pandas as pd
from math import ceil
from datetime import datetime as dt
import seaborn as sns
import matplotlib.pyplot as plt

from sources import load_items

def analyse_item_prices(verbose=False):
    """
    Generate item prices based on all past auction activity and scans
    """
    auction_activity = pd.read_parquet('full/auction_activity.parquet')
    auction_activity = auction_activity[['item', 'timestamp', 'price_per', 'auction_type']]

    auction_scan_minprice = pd.read_parquet('full/auction_scan_minprice.parquet')

    df_auction_prices = auction_scan_minprice.append(auction_activity)

    items = df_auction_prices['item'].unique()
    df_auction_prices = df_auction_prices.set_index(['item', 'timestamp']).sort_index()
    item_prices = {item: df_auction_prices.loc[item, 'price_per'].ewm(alpha=0.2).mean().iloc[-1] for item in items}
    
    item_prices = pd.DataFrame.from_dict(item_prices, orient='index')
    item_prices.index.name = 'item'
    item_prices.columns = ['market_price']
    item_prices.to_parquet('intermediate/item_prices.parquet', compression='gzip')
    
    if verbose:
        print(f"Item prices calculated. {len(item_prices)} records")


def analyse_sales_performance():
    """
    Produces charts and tables to help measure performace
    """

    item_prices = pd.read_parquet('intermediate/item_prices.parquet')
    user_items = load_items()

    inventory_full = pd.read_parquet('full/inventory.parquet')
    inventory_trade = inventory_full[inventory_full['item'].isin(user_items)]

    inventory_trade = pd.merge(inventory_trade, item_prices, how='left', left_on='item', right_index=True)
    inventory_trade['total_value'] = inventory_trade['count'] * inventory_trade['market_price']
    inventory_value = inventory_trade.groupby(['timestamp', 'character']).sum()['total_value'].unstack()

    monies_full = pd.read_parquet('full/monies.parquet')
    monies_full = monies_full.reset_index().set_index(['timestamp', 'index'])['monies'].unstack()

    inv_mule = inventory_value['Amazona'] + inventory_value['Amazoni']
    inv_rest = inventory_value.sum(axis=1) - inv_mule

    monies_mule = monies_full['Amazona - Grobbulus'] + monies_full['Amazoni - Grobbulus']
    monies_mule.name = 'Mule monies'
    monies_rest = monies_full.sum(axis=1) - monies_mule

    holdings = pd.DataFrame(monies_mule)
    holdings['Rest monies'] = monies_rest
    holdings['Mule inventory'] = inv_mule.values
    holdings['Rest inventory'] = inv_rest.values

    holdings['Total holdings'] = holdings.sum(axis=1)
    holdings = (holdings/10000).astype(int)

    sns.set()
    sns.set_style('whitegrid')
    sns.despine()

    plt = sns.lineplot(data=holdings[['Mule monies', 'Mule inventory']], color="b").set_title('Mule money and inventory')
    plt.figure.savefig('outputs/mules.png')

    plt = sns.lineplot(data=holdings['Total holdings'], color="black").set_title('Total holdings')
    plt.figure.savefig('outputs/holdings.png')

    latest_inventory = inventory_trade[inventory_trade['timestamp']==inventory_trade['timestamp'].max()]
    latest_inventory['total_value'] = (latest_inventory['total_value']/10000).round(2)
    latest_inventory = latest_inventory.groupby('item').sum()[['count', 'total_value']]
    latest_inventory = latest_inventory.sort_values('total_value', ascending=False)
    latest_inventory.to_parquet('outputs/latest_inventory_value.parquet', compression='gzip')

    earnings = pd.DataFrame([holdings.iloc[-10], holdings.iloc[-1]])
    earnings.loc[str(earnings.index[1] - earnings.index[0])] = earnings.iloc[1] - earnings.iloc[0]
    earnings.index = earnings.index.astype(str)
    earnings.to_parquet('outputs/earnings_days.parquet', compression='gzip')


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


def generate_historic_average_prices(verbose=False, MAX_RECENT=25, MIN_RECENT=5, MAX_SUCCESS=250, MIN_SUCCESS=10, MIN_PROFIT_MARGIN=1000):
    """ Enriches the user items listing with pricing data from analysis
        Analysed within recent X transactions; yields average buy_price, sell_price
        Calculates material costs for any crafted item, and other costs
        Calculates profit per item and minimum item price
    """

    user_items = load_items()
    df_raw = pd.read_parquet('full/auction_actions.parquet')
    df = df_raw.copy()

    df['rank'] = df.groupby(['auction_type', 'item'])['timestamp'].rank(ascending=False)
    recent_df = df[df['rank']<=MAX_RECENT]

    item_prices = recent_df.groupby(['auction_type', 'item'])['price_per'].mean().unstack().T
    item_counts = recent_df.groupby(['auction_type', 'item'])['price_per'].count().unstack().T.fillna(0)
    item_prices = item_prices[item_counts>=MIN_RECENT]
    item_std = df.groupby(['item'])['price_per'].std().to_dict()

    item_prices[['buy_price']].dropna().to_parquet('intermediate/buy_prices.parquet', compression='gzip')

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

    item_details = item_prices.join(df_success)
    full_deposit = pd.Series({item: details.get('full_deposit') for item, details in user_items.items()})
    item_details['full_deposit'] = full_deposit
    item_details['material_cost'] = pd.Series(item_costs)
    profit = item_details[['sell_price', 'material_cost', 'full_deposit', 'auction_success']].dropna()

    profit['gross_profit'] = (profit['sell_price'] - 
                        profit['material_cost'] - 
                        (profit['sell_price'] * 0.05) - 
                        (profit['full_deposit'] * (1 - profit['auction_success'])))

    profit['min_list_price'] = ((profit['material_cost'] + 
                                 (profit['full_deposit'] * (1 - profit['auction_success']))) + 
                                MIN_PROFIT_MARGIN) * 1.05

    # Round to nearest silver, bit more readable
    profit['min_list_price'] = (profit['min_list_price'].astype(float) / 100).astype(int)*100
    if verbose:
        print(profit['min_list_price'])

    profit.to_parquet('intermediate/sell_prices.parquet', compression='gzip')


def get_profits(date=None, item=None):
    df_raw = pd.read_parquet('intermediate/auctions.parquet')
    df_raw = df_raw.drop_duplicates(subset=['auction_type','item', 'timestamp'])

    df_failed = df_raw[df_raw['auction_type'].isin(['failed'])]
    df_success = df_raw[df_raw['auction_type'].isin(['buy_price', 'sell_price'])]
    df_auction_type = df_success.set_index(['auction_type','item', 'timestamp'])['price_per'].unstack().T

    item_history = df_success.set_index(['item','auction_type', 'timestamp'])['price_per'].unstack().T
    general_profit = ((df_auction_type['sell_price'].sum(axis=1).cumsum() - df_auction_type['buy_price'].sum(axis=1).cumsum()) / 10000).astype(int)

    if item:
        item_history[item].dropna(how='all').plot()
    elif date:
        general_profit.loc[date:].plot()


def generate_market_average_prices(MAX_RECENT=25, MIN_RECENT=5):
    
    user_items = load_items()
    df = pd.read_parquet('intermediate/auctions.parquet')    

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

    item_prices.to_parquet('intermediate/market_average_prices.parquet', compression='gzip')


def generate_item_sell_prices(MAX_SUCCESS=250, MIN_SUCCESS=10, MIN_PROFIT_MARGIN=1000):

    user_items = load_items()

    item_prices = pd.read_parquet('intermediate/market_average_prices.parquet')

    # Given the average recent buy price, calculate material costs per item
    item_costs = {}
    for item, details in user_items.items():
        material_cost = 0
        for ingredient, count in details.get('made_from', {}).items():
            material_cost += item_prices.loc[ingredient, 'buy_price'] * count
        if material_cost is not 0:
            item_costs[item] = int(material_cost)

    df_success = pd.read_parquet('intermediate/auctions.parquet')

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

    item_details = item_prices.join(df_success)
    full_deposit = pd.Series({item: details.get('full_deposit') for item, details in user_items.items()})
    item_details['full_deposit'] = full_deposit
    item_details['material_cost'] = pd.Series(item_costs)
    profit = item_details[['sell_price', 'material_cost', 'full_deposit', 'auction_success']].dropna()

    profit['gross_profit'] = (profit['sell_price'] - 
                        profit['material_cost'] - 
                        (profit['sell_price'] * 0.05) - 
                        (profit['full_deposit'] * (1 - profit['auction_success'])))

    profit['min_list_price'] = ((profit['material_cost'] + 
                                 (profit['full_deposit'] * (1 - profit['auction_success']))) + 
                                MIN_PROFIT_MARGIN) * 1.05

    # Round to nearest silver, bit more readable
    profit['min_list_price'] = (profit['min_list_price'].astype(float) / 100).astype(int)*100

    profit.to_parquet('intermediate/item_sell_profit.parquet', compression='gzip')        