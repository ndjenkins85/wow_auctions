"""
This script contains analysis of the cleaned panda parquet sources
It creates outputs for dashboard and lua policy updates
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from utils import *

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

    plt = sns.lineplot(data=holdings[['Mule monies', 'Mule inventory']], color="b")
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


def analyse_character_needs(character=[]):
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


def analyse_item_min_sell_price(MAX_SUCCESS=250, MIN_SUCCESS=10, MIN_PROFIT_MARGIN=1000):
    """
    Calculate minimum sell price for potions given raw item cost, deposit loss, AH cut, and min profit
    """

    user_items = load_items()

    item_prices = pd.read_parquet('intermediate/item_prices.parquet')
    item_prices.loc['Crystal Vial'] = 400
    item_prices.loc['Leaded Vial'] = 32
    item_prices.loc['Empty Vial'] = 3

    # Given the average recent buy price, calculate material costs per item
    item_costs = {}
    for item, details in user_items.items():
        material_cost = 0
        for ingredient, count in details.get('made_from', {}).items():
            material_cost += item_prices.loc[ingredient, 'market_price'] * count
        if material_cost is not 0:
            item_costs[item] = int(material_cost)


    df_success = pd.read_parquet('full/auction_actions.parquet')        

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

    item_min_sale = pd.DataFrame.from_dict(item_costs, orient='index')
    item_min_sale.index.name = 'item'
    item_min_sale.columns = ['mat_cost']

    item_min_sale = item_min_sale.join(df_success)

    full_deposit = pd.Series({item: details.get('full_deposit') for item, details in user_items.items()})
    full_deposit.name = 'deposit'

    item_min_sale = item_min_sale.join(full_deposit).dropna()

    item_min_sale['min_list_price'] = ((item_min_sale['mat_cost'] + 
                                 (item_min_sale['deposit'] * (1 - item_min_sale['auction_success']))) + 
                                MIN_PROFIT_MARGIN) * 1.05

    item_min_sale[['min_list_price']].to_parquet('intermediate/min_list_price.parquet', compression='gzip')


def create_sell_policy(sale_number=3, stack_size=5, duration='short'):
    """
    Creates simple sell policy whereby it's 0.6% lower than market price
    If proposed price is lower than the reserve price, don't auction    
    """
    duration = {'short': 720, 'medium': 1440, 'long': 2880}.get(duration)

    # Get our calculated reserve price
    item_min_sale = pd.read_parquet('intermediate/min_list_price.parquet')

    df_item_code = pd.Series(get_item_codes())
    df_item_code.name = 'code'
    item_min_sale = item_min_sale.join(df_item_code)

    # Get latest minprice per item
    # Note this is subject to spiking when someone puts a very low price on a single auction
    auction_scan_minprice = pd.read_parquet('intermediate/auction_scan_minprice.parquet')
    auction_scan_minprice = auction_scan_minprice.set_index('item')['price_per']
    auction_scan_minprice.name = 'market_price'
    
    df_sell_policy = item_min_sale.join(auction_scan_minprice)
    
    # Create sell price from market price, create check if lower than reserve
    df_sell_policy['sell_price'] = (df_sell_policy['market_price'] * 0.9933).astype(int)
    df_sell_policy['below_min_flag'] = (df_sell_policy['min_list_price'] >= df_sell_policy['sell_price']).astype(int)
    df_sell_policy['min_list_price'] = df_sell_policy['min_list_price'].astype(int)
    
    # Prepare policy info for dashboard
    df_sell_policy.drop(['code', 'market_price'], axis=1)
    df_sell_policy['profit_per_item'] = df_sell_policy['sell_price'] - df_sell_policy['min_list_price']
    df_sell_policy.to_parquet('outputs/sell_policy.parquet', compression='gzip')    
    
    # Seed new appraiser
    new_appraiser = {
     'bid.markdown': 0,
     'columnsortcurDir': 1,
     'columnsortcurSort': 6,
     'duration': 720,
     'bid.deposit': True
    }    
    
    # Iterate through items setting policy
    for item, d in df_sell_policy.iterrows():
        new_appraiser[f'item.{d["code"]}.fixed.bid'] = d['sell_price'] + d['below_min_flag'] # wont autosell if ++
        new_appraiser[f'item.{d["code"]}.fixed.buy'] = d['sell_price']
        new_appraiser[f'item.{d["code"]}.match'] = False
        new_appraiser[f'item.{d["code"]}.model'] = 'fixed'
        new_appraiser[f'item.{d["code"]}.number'] = sale_number
        new_appraiser[f'item.{d["code"]}.stack'] = stack_size
        new_appraiser[f'item.{d["code"]}.bulk'] = True 
        new_appraiser[f'item.{d["code"]}.duration'] = duration
        
    # Read client lua, replace with 
    data = read_lua('Auc-Advanced', merge_account_sources=False)
    data = data.get('396255466#1')
    data['AucAdvancedConfig']['profile.Default']['util']['appraiser'] = new_appraiser
    write_lua(data)

