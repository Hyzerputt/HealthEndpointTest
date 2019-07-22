# Health Endpoint Test

**Dev notes:**
```bash
brew install rabbitmq
```
The server can then be started with rabbitmq-server.

**Python modules**
Threading
cProfile.profile + Stats .remove_dirs, sort(cum_total)
profile.run_call(func)

python -m SimpleHTTPServer

pretty print to file 
```python
from pprint import pprint

myTree = {}
with open('output.txt', 'wt') as out:
    pprint(myTree, stream=out)
```

Use double ended queue to track last 5 response times
```python
from collections import deque
from loguru import logger

queue = deque([0,0,0,0,0], maxlen=5)
logger.success(f"Queue initialized: q={queue}")
for i in range(10):
    queue.appendleft(i)
    logger.info(queue)
```

