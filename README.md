# WoW Auction engine

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This project helps automate some aspects of trading on the World of Warcraft (WoW) auction house.

Related article here: https://www.nickjenkins.com.au/articles/personal/2020/07/07/programming-and-analytics-in-games

The program is currently under development and is not currently designed for third party use.

### Environment setup

```bash
virtualenv venv
. venv/bin/activate
pip install -r requirements.txt
```

#### TODO

* Create additional selling profile for min-bid max-buy high-volume. May require splitting the function more carefully
* More visibility on current inventory
