# WoW Auction engine

This project is an attempt to increase gold generated per hour in the game World of Warcraft (WoW).

### Background
By way of background, 'gold' in WoW is similar to real life money. Players earn gold from killing monsters, and this gold is used to buy items from other players. To facilitate this exchange, there is a centralised Auction House (AH). From there players can list their items for bid/buy, and use the AH to purchase items they need. In many ways, the AH is comparable with a 'stock market' with item value regularly changing. 

### Herbs and Potions
Some items in game such as herbs can be brewed into powerful potions. Herbs needs to be found and picked in the wild, and brewing usually requires being in the city with some time to spare. Because of these constraints on supply, these two markets (herbs, potions) can fall out of sync. And here is where we seek to collect value - buy herbs when there is high supply and sell potions when there is high demand.

### Keeping it simple
There are some simple strategies which can help players make money in such a chaotic situation. The basic concept of 'buy low sell high' tends to work well, but requires players to identify when prices are non-average. Cost plus pricing is not bad either, take the raw costs of materials add some profit and sell.

### A better system?
What would be more ideal is to have a system which could make pricing decisions for us. Such as when to buy herbs (if available), and for how much. The system may have to work with constraints such as overhead costs and inventory space. Then, it would tell how much to sell potions for, and how many. Because demand for potions drives our own demand for herbs, this process would identify limits to how many herbs we would want, and adjust accordingly.

### Constraints
In addition to usual supply and demand, there are some further constraints for this process to consider:
* Capital
* Inventory space
* Time
* Overhead costs (AH cut, vials, mail)

The capital constraint is an interesting one, and exists alongside another feature of the game. There is strong inflationary pressure in the game because every time monsters die, more gold is added to the game. What this means is over time gold becomes worth less, and items become worth more. Because of this, if we are saving for a very expensive item, we may be better off putting that capital into items and selling later. Thus free cash flow becomes a key consideration of the system.

We also don't have unlimited inventory space, so we should be careful to avoid a situation where low value items are crowding all the space. 

### Data tools at our disposal
Getting the right data at the right time is the name of our game. Short of having a full robot which can do everything for us (against the TOS), we will have to play within the rules of the game. We can move data in and out of the game via a restricted number of 'addons'. After some research, there are some addons which are perfect for us to start to gather data.
- Auctioneer allows us to set a value for items and buy any item at that price or lower
- ArkInventory tells us how many of each item we have in our inventory
- Beancounter tells us our historic gold in / gold out, how much of each item has been bought, for how much, and how many auctions failed

### The plan
With this in mind we propose the following plan to iterate towards a good model. For each item create a price (y) that is affected by: current inventory, upward demand, upward sale price. The best measure for how we are performing is given as 'gold per hour'.

#### Herb buying process

1) Log in Amazoni
2) Collect AH mail to set inventory counts
3) Scan AH prices
4) Log out
5) Run program (sets herb prices)
6) Log in
7) Scan AH prices
8) Use Auctioneer 'Snatch', purchase all
9) Log out, repeat from 6 as required till price reset

Simple approach;
* Each item has an inventory cap, related to demand
* As inventory fills, so too should the price threshold, so only bargins get in

## Primary TODO
- [X] Create lists of items of interest
- [X] Find the total inventory of each item
- [X] Remove inventory reserved for character use
- [X] Define stack sizes for items of interest
- [X] Inventory needs to be split by location (auction, mailbox infinite), to give proper inventory constraint
- [] Define constraints for max inventory size for stock

## Extra TODO
- [] Aside, given some rough prices provide stocktake total
- [] Given dynamic market prices provide stocktake total
- [] Given self-use list, create list of items to send to players



