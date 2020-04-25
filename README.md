# WoW Auction engine

This project helps automate some aspects of trading on the World of Warcraft (WoW) auction house.

### Background
Gold in WoW is similar to real life money. Players earn gold from killing monsters, and this gold is used to buy items from other players. To facilitate this exchange, there is a centralised Auction House (AH). From there players can list their items for bid/buy, and use the AH to purchase items they need. In many ways, the AH is comparable with a 'stock market' with item value regularly changing. 

##### Herbs and Potions
Some items in game such as herbs can be brewed into powerful potions. Herbs needs to be found and picked in the wild, and brewing usually requires being in the city with some time to spare. Because of these constraints on supply, these two markets (herbs, potions) can fall out of sync. And here is where we seek to collect value - buy herbs when there is high supply and sell potions when there is high demand.

##### Keeping it simple
There are some simple strategies which can help players make money. The basic concept of 'buy low sell high' tends to work well, but requires players to identify when prices are non-average. Cost plus pricing is not bad either, take the raw costs of materials add some profit and sell.

##### A better system?
What would be more ideal is to have a system which could make pricing decisions for us. Such as when to buy herbs (if available), and for how much. The system may have to work with constraints such as overhead costs and inventory space. Then, it would tell how much to sell potions for, and how many. Because demand for potions drives our own demand for herbs, this process would identify limits to how many herbs we would want, and adjust accordingly.

### Data tools at our disposal
Getting the right data at the right time is the name of our game. Short of having a full robot which can do everything for us (against the TOS), we will have to play within the rules of the game. We can move data in and out of the game via a restricted number of 'addons'. After some research, there are some addons which are perfect for us to interact with data. The plan is to update information, run program and seed new information, return to game.

* Auctioneer allows us to set a value for items and buy any item at that price or lower
* ArkInventory tells us how many of each item we have in our inventory
* Beancounter tells us our historic gold in / gold out, how much of each item has been bought, for how much, and how many auctions failed

### The plan
The overall aim is to increase profits given constraints of potion actual sale price, herb actual buy price, sale volume, herb market, time taken, capital, inventory space, current inventory, personal demand, fail rate, (out of stock), actions per day.

For simplicity, we assume unlimited capital and inventory space.

We allocate a fixed cost for time variable, such that we're earning 100g/h. We may earn higher or lower than this, this just becomes a constant in calculations help factor time. We introduce this for time spent crafting, and a general per unit penalty for time spent doing actions such as mail, movement, auctioning, relogging. The generic cost is expressed as 3s per item (about 100g/h), and crafting depends on recipe.

Personal demand is the number of goods reserved for use by my characters. We create a list of minimum reserved goods and subtract that from available inventory. This is mainly a convenience, and this approach can quickly inform how to top up all the characters.

The number of times we log in to execute the process helps determine inventory size. This is because we add more items to AH each time. When inventory size cap goes up, so too does our buy price as we look to fill the order. Thus we add a forward projection on how many logins we would do in the next week. 

To further simplify the problem, we will set soft and hard caps on max/min inventory of certain items, to help guide how pricing may scale based on how easy it is to attain items. 

This leaves us with buy prices, current inventory, sale prices, auction fail rate, and out of stock

Buy prices are determined by two variables, market rate on all goods for offer, and the current inventory size. When the inventory is high, we should lower our minimum buy price, to only accept bargins. When our inventory is low, we need to raise our buy price to repopulate.

To help analyse estimated profits, we record a geometric weighted average buy price figure.

Minimum sale price is determined by modelled value of the product given herb costs, vial costs, auction clears, auction fee, time spent to create (fixed g/h variable), global unit cost (another fixed price varible).

We use the minimum sale price to determine if we should sell. Any additional sale price is factored into an geometric weighted average sell price.

When the item does not meet the minimum sale price, this should signal that we carry less of the product and wait for input prices to cool down.

### Program usage

1. Log in Amazona
2. Scan AH prices
3. Collect mail
4. For each intended policy setting:
	1. Log out
	2. Run program (First time run all, next times set policies only)
	3. Log in
	4. Buy and Sell

### Primary TODO
- [X] Create lists of items of interest
- [X] Generate and save auction scandata
- [X] Generate auction activity record
- [X] Generate records of all character inventories and monies
- [X] Analyse item prices; uses historic scandata min prices, and auction activity
- [X] Analyse sales performance over time (money, inventory value)
- [X] Analyse minimum sale price per item, given item input costs, AH cut, margin etc.
- [X] Create sell policies, small discount on market price if above reserve. Allows adjustment of style

### Next TODO

- [ ] Create web dashboard to display information and run aspects of program
- [ ] Adjust min sell price to use item time price and global time price
- [ ] Create buy policies for raw ingredients (snatch)
- [ ] Create additional sell policies and checks (never empty AH, no min price spiking)
- [ ] Create 'only' tracker to gradually increase the pool over a day's trading
- [ ] Sell additional items smartly if i'm the lowest price

### Future potions

* Lesser Invisibility Potion
* Gift of Arthas
* Ghost Dye
* Greater Fire Protection Potion

![Potions](outputs/potions.png)
