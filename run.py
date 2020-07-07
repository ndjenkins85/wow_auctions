""" Runs the main program with command line options
"""

import warnings
warnings.simplefilter(action='ignore')


import click
# Try out CLICK
# Import logging with dt
# Try packaging the files
# Remove parquet from git
# Poetry

from pricer import sources, analysis
from datetime import datetime as dt

@click.command()
def analyse():
	""" Load sources, calculate prices, create policies
	"""
	# TODO update this
	verbose = True
	test = True

	sources.generate_booty_data(verbose=verbose)
	sources.generate_auction_scandata(verbose=verbose, test=test)
	sources.generate_auction_activity(verbose=verbose, test=test)
	sources.generate_inventory(verbose=verbose, test=test)
	analysis.analyse_item_prices(verbose=verbose)
	analysis.analyse_sales_performance()
	analysis.analyse_item_min_sell_price(MAT_DEV=0)
	analysis.analyse_sell_data()
	analysis.apply_buy_policy(MAT_DEV=0)

#apply_sell_policy(stack_size=5, leads_wanted=50, duration='long', update=True, factor=2)
#generate_new_pricer_file

if __name__ == "__main__":
	start = dt.now()
	print(start)

	analyse()
	analysis.apply_sell_policy(stack_size=5, leads_wanted=20, duration='medium', update=True)
	#analysis.apply_sell_policy(stack_size=1, leads_wanted=25, duration='medium', update=True, leave_one=False)


	end = dt.now()
	print(end)
	print("Time taken", (end - start).total_seconds())