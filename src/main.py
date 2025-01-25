#python 3.12.7

from concurrent.futures import ThreadPoolExecutor
import time
from random import randint

futures = []
items = [1, 2, 3, 4, 5]

def pretend_to_do_something(item):
    time.sleep(randint(5, 15))
    if item == 3:
        raise Exception("Boom")
    
    return f"Done with {item}"

# This is the same as the code below but will block until all workers are done
# Normally this is fine but if you need to inspect work as it's completing you cant use the with statement
"""with ThreadPoolExecutor(max_workers=2) as executor:
    for item in items:
        print(f"Submitting {item}")
        futures.append(executor.submit(pretend_to_do_something, item))"""

tpe = None

try:
    # This will load up the tpe's internal queue and move on to the futures loop
    # This provides more granular control over the futures and whats going on inside them
    tpe = ThreadPoolExecutor(max_workers=2)
    for item in items:
        print(f"Submitting {item}")
        futures.append(tpe.submit(pretend_to_do_something, item))

    print("outside of loop")
    
    try:
        for future in futures:
            print("Future loop")
            print(future.result())
            print(future.exception())
            futures.remove(future)
            print(f"{len(futures)} futures left")
            time.sleep(1)
    except Exception as e:
        print(f"Caught the following exception in futures loop {e}")
        tpe.shutdown(wait=False, cancel_futures=True) # Throw the brakes, rely on restart behavior

except Exception as e:
    print(f"Caught in outer try except but you should never reach here {e}")

finally:
    if tpe:
        tpe.shutdown(wait=True)
        print("Shutdown complete")

