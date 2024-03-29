"""
This file contains shorter utilities to write/save raw files, and change data formats
"""

import yaml
import pandas as pd
from slpp import slpp as lua  # pip install git+https://github.com/SirAnthony/slpp
from datetime import datetime as dt
import os

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s:%(name)s:%(message)s")

file_handler = logging.FileHandler(f"logs/{__name__}.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def generate_new_pricer_file():
    """ Generates a blank pricer file of items of interest. This is used to fill in the latest pricing
    information from booty bay gazette. This is done in game using a self build addon with the /pricer command    
    """
    items = load_items()

    pricer_file = ["local addonName, addonTable = ...", "", "addonTable.items = {"]

    for key, value in items.items():
        if value.get("group") in ["Buy", "Sell"]:
            pricer_file.append(f"['{key}'] = " + "{},")

    # Replace last ',' with '}'
    pricer_file[-1] = pricer_file[-1][:-1] + "}"

    pricer_path = "/Applications/World of Warcraft/_classic_/Interface/AddOns/Pricer/items_of_interest.lua"

    with open(pricer_path, "w") as f:
        f.write("\n".join(pricer_file))


def source_merge(a, b, path=None):
    "merges b into a"
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                source_merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                pass  # raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


def read_lua(
    datasource: str, merge_account_sources=True, accounts=["BLUEM", "396255466#1"]
):
    """ Attempts to read lua from the given locations
    """
    account_data = {key: None for key in accounts}
    for account_name in account_data.keys():
        path_live = f"/Applications/World of Warcraft/_classic_/WTF/Account/{account_name}/SavedVariables/{datasource}.lua"

        with open(path_live, "r") as f:
            account_data[account_name] = lua.decode("{" + f.read() + "}")

    if merge_account_sources and len(accounts) > 1:
        return source_merge(account_data["BLUEM"], account_data["396255466#1"])
    else:
        return account_data


def load_items():
    """ Loads and returns the user created YAML file of interesting items and their stack sizes
    """
    with open("config/items.yaml", "r") as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def get_general_settings():
    """Gets general program settings
    """
    with open("config/general_settings.yaml", "r") as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def get_and_format_auction_data():
    """ Reads the raw scandata dict dump and converts to usable dataframe    
    """
    path_live = f"/Applications/World of Warcraft/_classic_/WTF/Account/396255466#1/SavedVariables/Auc-ScanData.lua"

    ropes = []
    with open(path_live, "r") as f:
        on = False
        rope_count = 0
        for line in f.readlines():
            if on and rope_count < 5:
                ropes.append(line)
                rope_count += 1
            elif '["ropes"]' in line:
                on = True

    listings = []
    for rope in ropes:
        if len(rope) < 10:
            continue
        listings_part = rope.split("},{")
        listings_part[0] = listings_part[0].split("{{")[1]
        listings_part[-1] = listings_part[-1].split("},}")[0]

        listings.extend(listings_part)

    # Contains lots of columns, we ignore ones we likely dont care about
    # We apply transformations and relabel
    df = pd.DataFrame([x.split("|")[-1].split(",") for x in listings])
    df["time_remaining"] = df[6].replace({1: 30, 2: 60 * 2, 3: 60 * 12, 4: 60 * 24})
    df["item"] = df[8].str.replace('"', "").str[1:-1]
    df["count"] = df[10].replace("nil", 0).astype(int)
    df["price"] = df[16].astype(int)
    df["agent"] = df[19].str.replace('"', "").str[1:-1]
    df["timestamp"] = df[7].apply(lambda x: dt.fromtimestamp(int(x)))

    # There is some timing difference in the timestamp, we dont really care we just need time of pull
    df["timestamp"] = df["timestamp"].max()

    df = df[df["count"] > 0]
    df["price_per"] = df["price"] / df["count"]

    cols = [
        "timestamp",
        "item",
        "count",
        "price",
        "agent",
        "price_per",
        "time_remaining",
    ]
    df = df[cols]

    return df


def get_item_codes():
    """
    Reads the beancounter database and produces codes per item dict
    """
    data = read_lua("BeanCounter")
    item_code = {}
    for keypart, itempart in data["BeanCounterDBNames"].items():
        key = keypart.split(":")[0]
        item = itempart.split(";")[1]
        item_code[item] = key
    return item_code


def write_lua(data, account="396255466#1", name="Auc-Advanced"):
    """
    Write python dict as lua object
    """
    lua_print = "\n"
    for key in data.keys():
        lua_print += f"{key} = " + dump_lua(data[key]) + "\n"

    location = f"/Applications/World of Warcraft/_classic_/WTF/Account/{account}/SavedVariables/{name}.lua"
    with open(location, "w") as f:
        f.write(lua_print)


def dump_lua(data):
    """
    Borrowed code to write python dict as lua format(ish)
    """
    if type(data) is str:
        return f'"{data}"'
    if type(data) in (int, float):
        return f"{data}"
    if type(data) is bool:
        return data and "true" or "false"
    if type(data) is list:
        l = "{"
        l += ", ".join([dump_lua(item) for item in data])
        l += "}"
        return l
    if type(data) is dict:
        t = "{"
        t += ", ".join([f'["{k}"]={dump_lua(v)}' for k, v in data.items()])
        t += "}"
        return t
    logger.warning(f"Unknown type {type(data)}")


def read_multiple_parquet(loc):
    files = os.listdir(loc)
    df_total = pd.read_parquet(f"{loc}{files[0]}")
    for file in files[1:]:
        df = pd.read_parquet(f"{loc}{file}")
        df_total.append(df)
    return df
