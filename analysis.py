"""
This script contains analysis of the cleaned panda parquet sources
It creates outputs for dashboard and lua policy updates
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from utils import *

sns.set(rc={'figure.figsize':(11.7,8.27)})

def analyse_item_prices(verbose=False, full_pricing=False):
    """
    Generate item prices based on all past auction activity and scans
    """
    auction_activity = pd.read_parquet('full/auction_activity.parquet')
    auction_activity = auction_activity[['item', 'timestamp', 'price_per', 'auction_type']]

    auction_scan_minprice = pd.read_parquet('full/auction_scan_minprice.parquet')

    df_auction_prices = auction_scan_minprice.append(auction_activity)

    if full_pricing:
        items = df_auction_prices['iten'].unique()
    else:
        items = load_items()

    price_history = df_auction_prices.set_index(['item', 'timestamp']).sort_index()['price_per']

    if full_pricing:
        item_prices = {item: price_history.loc[item].ewm(alpha=0.2).mean().iloc[-1] for item in items}    
    else:
        # Only calculate for our item list; get backup price if present
        item_prices = {}
        for item, details in items.items():
            price = details.get('backup_price')
            if not price:
                price = price_history.loc[item].ewm(alpha=0.2).mean().iloc[-1]
            item_prices[item] = price
        
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


def analyse_auction_success(MAX_SUCCESS=250, MIN_SUCCESS=10):
    """
    Produces dataframe of recent successful auctions
    """
    df_success = pd.read_parquet('full/auction_activity.parquet')        

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
    return df_success


def analyse_item_min_sell_price(MIN_PROFIT_MARGIN=1000):
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

    df_success = analyse_auction_success()

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


def analyse_sell_data():
    """
    Creates dataframe of intellegence around the selling market conditions

    """

    # Get our calculated reserve price
    item_min_sale = pd.read_parquet('intermediate/min_list_price.parquet')

    # Get latest minprice per item
    # Note this is subject to spiking when someone puts a very low price on a single auction
    auction_scan_minprice = pd.read_parquet('intermediate/auction_scan_minprice.parquet')
    auction_scan_minprice = auction_scan_minprice.set_index('item')['price_per']
    auction_scan_minprice.name = 'market_price'

    df = item_min_sale.join(auction_scan_minprice)

    # If item isnt appearing in market atm (NaN), fill with doubled min list price
    df['market_price'] = df['market_price'].fillna(df['min_list_price'] * 2)

    # Create sell price from market price, create check if lower than reserve
    df['sell_price'] = (df['market_price'] * 0.9933).astype(int)
    df['infeasible'] = (df['min_list_price'] >= df['sell_price']).astype(int)
    df['min_list_price'] = df['min_list_price'].astype(int)
    df['profit_per_item'] = df['sell_price'] - df['min_list_price']

    # Get latest auction data to get the entire sell listing
    auction_data = pd.read_parquet('intermediate/auction_scandata.parquet')
    auction_data = auction_data[auction_data['item'].isin(item_min_sale.index)]
    auction_data = auction_data[auction_data['price_per']>0]

    # Find the minimum price per item, join back
    auction_data = pd.merge(auction_data, df['market_price'], how='left', left_on='item', right_index=True)

    # Find my minimum price per item, join back (if exists)
    my_auction_mins = auction_data[auction_data['agent']=='Amazona'].groupby('item').min()
    my_auction_mins = my_auction_mins['price_per']
    my_auction_mins.name = 'my_min'
    auction_data = pd.merge(auction_data, my_auction_mins, how='left', left_on='item', right_index=True)
    auction_data = auction_data.dropna() # Ignores items I'm not selling

    # Find items below my min price (i.e. competition); get count of items undercutting
    undercut_count = auction_data[auction_data['price_per']<auction_data['my_min']]
    undercut_count = undercut_count.groupby('item').sum()['count']
    undercut_count.name = 'undercut_count'

    df = df.join(undercut_count)
    df['undercut_count'] = df['undercut_count'].fillna(0).astype(int)

    # If my min price is the same as the current min price and the same as the listing price, i'm winning
    my_min_is_market = auction_data['my_min']==auction_data['market_price']
    my_min_is_list = auction_data['my_min']==auction_data['price_per']
    auction_leads = auction_data[my_min_is_market & my_min_is_list].groupby('item').sum()['count']
    auction_leads.name = 'auction_leads'

    df = df.join(auction_leads)
    df['auction_leads'] = df['auction_leads'].fillna(0).astype(int)

    # Get table showing how much inventory is where; auctions, bank/inv/mail, alt.
    # Can help determine how much more to sell depending what is in auction house now
    inventory_full = pd.read_parquet('full/inventory.parquet')
    inventory_full = inventory_full[inventory_full['character'].isin(['Amazoni', 'Amazona'])]
    inventory_full = inventory_full[inventory_full['item'].isin(item_min_sale.index)]
    inventory_full = inventory_full[inventory_full['timestamp'].max()==inventory_full['timestamp']]

    df['auctions'] = inventory_full[inventory_full['location']=='Auctions'].groupby('item').sum()
    df['auctions'] = df['auctions'].fillna(0).astype(int)

    df['inventory'] = inventory_full[(inventory_full['character']=='Amazona')&(inventory_full['location']!='Auctions')].groupby('item').sum()
    df['inventory'] = df['inventory'].fillna(0).astype(int)

    df['immediate_inv'] = inventory_full[(inventory_full['character']=='Amazona')&(inventory_full['location']=='Inventory')].groupby('item').sum()
    df['immediate_inv'] = df['immediate_inv'].fillna(0).astype(int)

    df['storage'] = inventory_full[inventory_full['character']=='Amazoni'].groupby('item').sum()
    df['storage'] = df['storage'].fillna(0).astype(int)

    df.to_parquet('outputs/sell_policy.parquet', compression='gzip')


def apply_sell_policy(sale_number=3, stack_size=5, duration='short', factor=1):
    """
    Given a datatable of the sell environment, create sell policy and save to WoW
    """

    df_sell_policy = pd.read_parquet('outputs/sell_policy.parquet')
    duration = {'short': 720, 'medium': 1440, 'long': 2880}.get(duration)
    item_codes = get_item_codes()
    
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
        code = item_codes[item]

        new_appraiser[f'item.{code}.fixed.bid'] = int((d['sell_price'] + d['infeasible']) * factor)
        new_appraiser[f'item.{code}.fixed.buy'] = int(d['sell_price'] * factor)
        new_appraiser[f'item.{code}.match'] = False
        new_appraiser[f'item.{code}.model'] = 'fixed'
        new_appraiser[f'item.{code}.number'] = sale_number
        new_appraiser[f'item.{code}.stack'] = stack_size
        new_appraiser[f'item.{code}.bulk'] = True
        new_appraiser[f'item.{code}.duration'] = duration
        
    # Read client lua, replace with 
    data = read_lua('Auc-Advanced', merge_account_sources=False)
    data = data.get('396255466#1')
    data['AucAdvancedConfig']['profile.Default']['util']['appraiser'] = new_appraiser
    write_lua(data)


def apply_buy_policy():
    """
    Determines herbs to buy based on potions in inventory. 
    Always buys at or below current market price.
    """
    
    # TODO; remove self_demand from this list, not a big deal
    # TODO need to subtract out oils (stoneshield) etc

    items = load_items()
    sell_policy = pd.read_parquet('outputs/sell_policy.parquet')

    # Determine how many potions I have, and how many need to be replaced
    replenish = sell_policy['auctions'] + sell_policy['inventory'] + sell_policy['storage']
    replenish.name = 'inventory'
    replenish = pd.DataFrame(replenish)

    for potion in replenish.index:
        replenish.loc[potion, 'max'] = items.get(potion).get('max_inventory', 60)

    replenish['inventory_target'] = (replenish['max'] - replenish['inventory']).apply(lambda x: max(0,x))
    replenish = replenish.join(analyse_auction_success())

    # Downweight requirements according to recent auction success
    replenish['target'] = (replenish['inventory_target'] * replenish['auction_success']).astype(int)

    # From potions required, get herbs required
    herbs_required = pd.Series()
    for potion, quantity in replenish['target'].iteritems():
        for herb, count in items.get(potion).get('made_from').items():
            if herb in herbs_required:
                herbs_required.loc[herb] += count * quantity
            else:
                herbs_required.loc[herb] = count * quantity

                herbs_required.name = 'herbs_needed'
    herbs = pd.DataFrame(herbs_required)

    # Add item codes from beancounter, used for entering into snatch
    item_codes = get_item_codes()
    herbs = herbs.join(pd.Series(item_codes, name='code'))            

    # Remove herbs already in inventory
    inventory = pd.read_parquet('intermediate/inventory.parquet')
    herbs = herbs.join(inventory.groupby('item').sum()['count']).fillna(0).astype(int)
    herbs['herbs_purchasing'] = (herbs['herbs_needed'] - herbs['count']).apply(lambda x: max(0,x))

    # Cleanup
    herbs = herbs.drop(['Crystal Vial', 'Empty Vial', 'Leaded Vial'])
    herbs = herbs.sort_index()

    # Get market values
    item_prices = pd.read_parquet('intermediate/item_prices.parquet')

    # Clean up auction data
    auction_data = pd.read_parquet('intermediate/auction_scandata.parquet')
    auction_data = auction_data[auction_data['item'].isin(items)]
    auction_data = auction_data[auction_data['price']>0]
    auction_data = auction_data.sort_values('price_per')
    auction_data['price_per'] = auction_data['price_per'].astype(int)

    for herb, count in herbs['herbs_purchasing'].iteritems():
        # Always buy at way below market
        buy_price = item_prices.loc[herb, 'market_price'] * 0.5    

        # Filter to herbs below market price
        listings = auction_data[auction_data['item']==herb]
        listings = listings[listings['price_per']<item_prices.loc[herb, 'market_price']]
        listings['cumsum'] = listings['count'].cumsum()

        # Filter to lowest priced herbs for the quantity needed
        herbs_needed = herbs.loc[herb, 'herbs_purchasing']
        listings = listings[listings['cumsum'] < herbs_needed]

        # If there are herbs available after filtering...
        if listings.shape[0] > 0:
            # Reject the highest priced item, in case there are 100s of listings at that price (conservative)
            not_last_priced = listings[listings['price_per']!=listings['price_per'].iloc[-1]]
            if not_last_priced.shape[0] > 0:
                buy_price = not_last_priced['price_per'].iloc[-1]    

        herbs.loc[herb, 'buy_price'] = buy_price    

    herbs['buy_price'] = herbs['buy_price'].astype(int) 

    # Get snatch data, populate and save back
    data = read_lua('Auc-Advanced', merge_account_sources=False)
    data = data.get('396255466#1')

    snatch = data['AucAdvancedData']['UtilSearchUiData']['Current']['snatch.itemsList']

    for herb, row in herbs.iterrows():
        snatch[f"{row['code']}:0:0"]['price'] = int(row['buy_price'])

    data['AucAdvancedData']['UtilSearchUiData']['Current']['snatch.itemsList'] = snatch    
    write_lua(data)

    herbs = herbs[['herbs_purchasing', 'buy_price']]
    herbs.to_parquet('outputs/buy_policy.parquet', compression='gzip')