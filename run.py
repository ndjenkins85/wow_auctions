""" Runs the main program with command line options
"""
from pricer import config, sources, analysis

from datetime import datetime as dt
import argparse
import warnings
import logging

warnings.simplefilter(action="ignore")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def analyse(test=False):
    """ Load sources, calculate prices, create policies
    """
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

    start_dt = dt.now()
    config.set_logging(logger)
    logger.debug("Program start")

    parser = argparse.ArgumentParser(description="WoW Auctions")
    parser.add_argument("-np", help="Create pricer file", action="store_true")
    parser.add_argument("-a", help="Run primary analysis", action="store_true")
    parser.add_argument("-t", help="Test mode (no saving)", action="store_true")
    parser.add_argument("-s1", help="Short policy 5stack", action="store_true")
    parser.add_argument("-s2", help="Short policy 1stack", action="store_true")
    parser.add_argument("-m1", help="Mid policy 5stack", action="store_true")
    parser.add_argument("-m2", help="Mid policy 1stack", action="store_true")
    parser.add_argument("-l1", help="Long policy 5stack", action="store_true")
    args = parser.parse_args()

    if args.np:
        utils.generate_new_pricer_file()
    if args.a:
        analyse(test=args.t)

    # Sell policies
    if args.s1:
        analysis.apply_sell_policy(stack=5, leads=5, duration="s")
    if args.s2:
        analysis.apply_sell_policy(stack=1, leads=10, duration="s")
    if args.m1:
        analysis.apply_sell_policy(stack=5, leads=20, duration="m")
    if args.m2:
        analysis.apply_sell_policy(stack=1, leads=25, duration="m")
    if args.l1:
        analysis.apply_sell_policy(stack=5, leads=50, duration="l")

    logger.info(f"Program end, time taken {(dt.now() - start_dt).total_seconds()}")
