#!/usr/bin/env python
# coding: utf-8

# In[3]:


import yaml
from collections import defaultdict
from slpp import slpp as lua #pip install git+https://github.com/SirAnthony/slpp
import pandas as pd
from math import ceil

def get_inventory():
    """ Reads and reformats the Arkinventory data file into a pandas dataframe
    Loads yaml files to specify item locations and specific items of interest
    Saves down parquet file ready to go
    """
    # Get basic config variables
    path_settings = 'config/settings.yaml'
    with open(path_settings, 'r') as f:
        settings = yaml.load(f, Loader=yaml.FullLoader)

    # Pulls our list of items of interest, and filters dataframe accordingly
    path_items_of_interest = 'config/items_of_interest.yaml'
    with open(path_items_of_interest, 'r') as f:
        items_of_interest = yaml.load(f, Loader=yaml.FullLoader)   

    # Attempts to load live version, otherwise takes the last copy
    try:
        path_live = "/Applications/World of Warcraft/_classic_/WTF/Account/BLUEM/SavedVariables/ArkInventory.lua" 
        with open(path_live, 'r') as f:
            data = lua.decode('{'+f.read()+'}')
    except:        
        path_local = "data/ArkInventory.lua"
        with open(path_local, 'r') as f:
            data = lua.decode('{'+f.read()+'}')

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
    print(f"Total Monies: {total_monies:.2f}")
    with open('intermediate/monies.txt', 'w') as f:
        f.write(str(total_monies))

    # Reformat items of interest data to get stack size, and item categories
    stack_sizes = {}
    categories = {}
    for cat, dic in items_of_interest.items():
        stack_sizes.update(dic)
        for item in dic:
            categories[item] = cat    

    # Add category and stack size info and save
    df['category'] = df['item'].apply(lambda x: categories.get(x))
    df['stack_size'] = df['item'].apply(lambda x: stack_sizes.get(x))
    df.to_parquet('intermediate/inventory_full.parquet', compression='gzip')    

    # Create more focused version
    df = df.dropna()
    df['stack_size'] = df['stack_size'].astype(int)
    df.to_parquet('intermediate/inventory.parquet', compression='gzip')


# In[25]:





# In[ ]:




