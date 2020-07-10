""" Runs the main program with command line options
"""

import argparse
import warnings
warnings.simplefilter(action='ignore')

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

file_handler = logging.FileHandler(f'logs/{__name__}.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Logging
# Setuptools
# venv->Poetry
# argparse -> Click
# docker

from pricer import sources, analysis
from datetime import datetime as dt

#@click.command()
def analyse():
    """ Load sources, calculate prices, create policies
    """
    # TODO update this
    test = False

    sources.generate_booty_data()
    sources.generate_auction_scandata(test=test)
    sources.generate_auction_activity(test=test)
    sources.generate_inventory(test=test)
    analysis.analyse_item_prices()
    analysis.analyse_sales_performance()
    analysis.analyse_item_min_sell_price(MAT_DEV=0)
    analysis.analyse_sell_data()
    analysis.apply_buy_policy(MAT_DEV=0)


if __name__ == "__main__":

    start_time = dt.now()
    logger.info('Program start')

    parser = argparse.ArgumentParser(description='WoW Auctions')
    parser.add_argument('-np', action='store_true')
    parser.add_argument('-a', action='store_true')
    parser.add_argument('-s1', action='store_true')
    parser.add_argument('-s2', action='store_true')
    parser.add_argument('-m1', action='store_true')   
    parser.add_argument('-m2', action='store_true')   
    parser.add_argument('-l1', action='store_true')       
    args = parser.parse_args()  

    if args.np: utils.generate_new_pricer_file()
    if args.a: analyse()

    if args.s1: analysis.apply_sell_policy(stack_size=5, leads_wanted=5, duration='short', update=True)
    if args.s2: analysis.apply_sell_policy(stack_size=1, leads_wanted=10, duration='short', update=True, leave_one=False)

    if args.m1: analysis.apply_sell_policy(stack_size=5, leads_wanted=20, duration='medium', update=True)
    if args.m2: analysis.apply_sell_policy(stack_size=1, leads_wanted=25, duration='medium', update=True, leave_one=False)

    if args.l1: analysis.apply_sell_policy(stack_size=5, leads_wanted=50, duration='long', update=True, factor=2)

    logger.info(f"Program end, total time taken {(dt.now() - start_time).total_seconds()}")