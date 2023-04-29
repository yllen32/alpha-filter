[![Python 3.6](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-360/) _and greater_

# alpha-filter

When parsing, sometimes it is necessary to reduce the number of requests to the server, for example, our script collects links from pagination to the product every day, and then parses each product separately. But what to do if some time ago we already parsed these goods, why do it twice. alpha-filter will help filter out those ads that have already been read, and will return only new ones.
 
## Getting starting
```sh
pip install alpha-filter
```

### Usage

```python
from alphafilter import filter_ads

first_parsing_urls = ["https://www.example.com/1", "https://www.example.com/2"]
new, old = filter_ads(first_parsing_urls)
new = ["https://www.example.com/1", "https://www.example.com/2"]
old = []

second_parsing_urls = first_parsing_urls # second parsing same with first

new, old = filter_ads(second_parsing_urls)
new = []
old = []

third_parsing_urls = ["https://www.example.com/2", "https://www.example.com/3"]

new, old = filter_ads(third_parsing_urls)
new = ["https://www.example.com/3"]
old = ["https://www.example.com/1"]
```
It uses a fast sqlite database to store urls. The database file ('ads.db') will be created in the root directory

__Warning!!! this package has no protection against sql injection, do not use it for the external interface__
